"""
sentiment_analyzer.py — TradePulse Real-Time News Sentiment Analysis

Role in pipeline:
    Consumes news headlines from the market.news Kafka topic.
    Scores each headline using VADER sentiment analysis.
    Correlates sentiment with existing volume z-score and anomaly signals
    to detect when unusual market activity coincides with news events.
    Writes results to DynamoDB table market_sentiment and S3.

Why VADER over a transformer model (BERT, FinBERT):
    VADER (Valence Aware Dictionary and sEntiment Reasoner) is rule-based
    and runs inference in under 1ms per headline. FinBERT would be more
    accurate for financial text but adds 200-400ms per inference and requires
    a GPU for production throughput. For real-time stream processing, VADER's
    speed tradeoff is correct. FinBERT could be added as an async enrichment
    step for high-signal events without blocking the main stream.

Why correlate with volume z-score:
    A news article alone is weak signal — most headlines don't move markets.
    But a news article published within 60 seconds of a volume z-score > 2.0
    is strong signal — it suggests the market is reacting to the news in real
    time. This correlation is the core value-add of the two-stream join.
"""

import logging
import time
from datetime import datetime, timezone
from dataclasses import dataclass, asdict
from typing import Optional

from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
import boto3
from botocore.exceptions import ClientError

from src.config import get_settings

logger = logging.getLogger(__name__)
settings = get_settings()


@dataclass
class SentimentResult:
    """
    Structured output of sentiment analysis for a single news headline.

    Fields:
        article_id:       Unique Finnhub article identifier
        ticker:           Stock ticker this article relates to
        headline:         Original headline text
        source:           News source (Reuters, Bloomberg, etc.)
        published_at:     When the article was published (ISO 8601)
        sentiment_score:  VADER compound score, range [-1.0, 1.0]
                         > 0.05  = positive
                         < -0.05 = negative
                         else    = neutral
        sentiment_label:  'positive' | 'negative' | 'neutral'
        vader_scores:     Full VADER output {neg, neu, pos, compound}
        correlated_anomaly: True if a volume anomaly was detected for this
                            ticker within 60 seconds of publication
        volume_zscore_at_publish: Volume z-score at time of publication
                                  (None if no recent trade data available)
        correlation_strength: 'strong' | 'moderate' | 'weak' | 'none'
                              Based on sentiment magnitude + z-score combo
    """
    article_id:               str
    ticker:                   str
    headline:                 str
    source:                   str
    published_at:             str
    sentiment_score:          float
    sentiment_label:          str
    vader_scores:             dict
    correlated_anomaly:       bool
    volume_zscore_at_publish: Optional[float]
    correlation_strength:     str
    analyzed_at:              str


class SentimentAnalyzer:
    """
    Analyzes news headline sentiment and correlates with market signals.

    Design decisions:
    - Per-ticker recent z-score cache: stores the last known volume z-score
      per ticker with a timestamp. Used for correlation without requiring
      a DynamoDB read on every article (too slow for real-time correlation).
    - 60-second correlation window: news that arrives within 60 seconds of
      a z-score spike is considered correlated. This window was chosen based
      on how quickly algorithmic traders typically react to headlines.
    - Correlation strength tiers: provides nuanced signal quality rather
      than binary correlated/not-correlated, which is more useful for
      downstream consumers.
    """

    def __init__(self):
        """
        Initializes VADER analyzer, DynamoDB client, and z-score cache.
        VADER loads its lexicon on init (~50ms) — done once at startup.
        """
        # VADER: rule-based sentiment analyzer optimized for social media
        # and short text. Works well for financial headlines.
        # Compound score interpretation:
        #   >= 0.05:  positive sentiment
        #   <= -0.05: negative sentiment
        #   else:     neutral
        self.vader = SentimentIntensityAnalyzer()

        self.dynamodb = boto3.resource(
            'dynamodb',
            region_name=settings.aws.region
        )
        self.sentiment_table = self.dynamodb.Table(
            settings.dynamo.table_sentiment
        )

        # In-memory cache of recent volume z-scores per ticker
        # Structure: {ticker: {'zscore': float, 'timestamp': datetime}}
        # Populated by update_zscore_cache() called from faust_app.py
        # when new aggregations are computed
        self._zscore_cache: dict = {}

        # Correlation window: news within this many seconds of a
        # z-score spike is considered correlated with market activity
        self.CORRELATION_WINDOW_SECONDS = 60

        # Z-score threshold for "unusual" volume — matches anomaly detector
        self.ZSCORE_UNUSUAL_THRESHOLD = 2.0
        self.ZSCORE_EXTREME_THRESHOLD = 3.0

    def update_zscore_cache(self, ticker: str, zscore: float):
        """
        Updates the in-memory z-score cache for a ticker.
        Called by the Faust aggregation agent whenever a new z-score
        is computed, so the cache always reflects the latest market state.

        Args:
            ticker: Stock ticker symbol
            zscore: Current volume z-score from aggregations.py
        """
        self._zscore_cache[ticker] = {
            'zscore':    zscore,
            'timestamp': datetime.now(timezone.utc),
        }

    def _get_recent_zscore(self, ticker: str) -> Optional[float]:
        """
        Returns the volume z-score for a ticker if it was updated within
        the correlation window, otherwise returns None.

        Args:
            ticker: Stock ticker symbol

        Returns:
            Volume z-score float if recent, None if stale or unavailable
        """
        cached = self._zscore_cache.get(ticker)
        if not cached:
            return None

        age_seconds = (
            datetime.now(timezone.utc) - cached['timestamp']
        ).total_seconds()

        # Only use z-score if it was computed within the correlation window
        # A stale z-score would create false correlations
        if age_seconds > self.CORRELATION_WINDOW_SECONDS:
            return None

        return cached['zscore']

    def _determine_correlation_strength(
        self,
        sentiment_score: float,
        zscore: Optional[float],
        correlated: bool
    ) -> str:
        """
        Determines the strength of the news-market correlation signal.

        Strength tiers:
            strong:   High-magnitude sentiment + extreme volume (z > 3.0)
                      Most likely a market-moving news event
            moderate: Moderate sentiment + unusual volume (z > 2.0)
                      Possible market reaction to news
            weak:     Low sentiment magnitude or borderline volume
                      Coincidental timing likely
            none:     No correlation detected within time window

        Args:
            sentiment_score: VADER compound score [-1.0, 1.0]
            zscore:          Volume z-score at time of publication
            correlated:      Whether temporal correlation exists

        Returns:
            Correlation strength label string
        """
        if not correlated or zscore is None:
            return 'none'

        sentiment_magnitude = abs(sentiment_score)

        if sentiment_magnitude >= 0.5 and zscore >= self.ZSCORE_EXTREME_THRESHOLD:
            return 'strong'
        elif sentiment_magnitude >= 0.2 and zscore >= self.ZSCORE_UNUSUAL_THRESHOLD:
            return 'moderate'
        else:
            return 'weak'

    def analyze(self, news_message: dict) -> SentimentResult:
        """
        Analyzes a news message and returns a SentimentResult.

        Flow:
        1. Run VADER on headline + summary combined text
        2. Check z-score cache for temporal correlation
        3. Determine correlation strength
        4. Build and return SentimentResult

        Args:
            news_message: Parsed Kafka message from market.news topic
                         Must have: ticker, headline, summary, published_at

        Returns:
            SentimentResult with full sentiment and correlation data
        """
        ticker   = news_message['ticker']
        headline = news_message.get('headline', '')
        summary  = news_message.get('summary', '')

        # Combine headline and first 200 chars of summary for richer signal
        # Headlines alone can be ambiguous — summary adds context
        # e.g. "Apple reports results" is neutral, but summary saying
        # "beats estimates by 15%" makes it clearly positive
        analysis_text = headline
        if summary:
            analysis_text += ' ' + summary[:200]

        # VADER compound score: normalized weighted sum of all word scores
        # Range: -1.0 (most negative) to +1.0 (most positive)
        vader_scores = self.vader.polarity_scores(analysis_text)
        compound     = vader_scores['compound']

        # Standard VADER thresholds for financial text
        if compound >= 0.05:
            label = 'positive'
        elif compound <= -0.05:
            label = 'negative'
        else:
            label = 'neutral'

        # Check for temporal correlation with volume activity
        zscore     = self._get_recent_zscore(ticker)
        correlated = (
            zscore is not None and
            zscore >= self.ZSCORE_UNUSUAL_THRESHOLD
        )

        correlation_strength = self._determine_correlation_strength(
            compound, zscore, correlated
        )

        # Log strong correlations at INFO level for operational visibility
        if correlation_strength == 'strong':
            logger.info(
                f"STRONG CORRELATION: {ticker} | "
                f"sentiment={compound:.3f} ({label}) | "
                f"z-score={zscore:.2f} | "
                f"headline='{headline[:80]}...'"
            )

        return SentimentResult(
            article_id=               news_message.get('article_id', ''),
            ticker=                   ticker,
            headline=                 headline,
            source=                   news_message.get('source', ''),
            published_at=             news_message.get('published_at', ''),
            sentiment_score=          round(compound, 4),
            sentiment_label=          label,
            vader_scores=             vader_scores,
            correlated_anomaly=       correlated,
            volume_zscore_at_publish= round(zscore, 4) if zscore else None,
            correlation_strength=     correlation_strength,
            analyzed_at=              datetime.now(timezone.utc).isoformat(),
        )

    def write_to_dynamo(self, result: SentimentResult) -> bool:
        """
        Writes a SentimentResult to DynamoDB market_sentiment table.

        Table design:
            Partition key: ticker (low cardinality acceptable here —
                           news volume is ~100x lower than trades,
                           so hot partition risk is minimal)
            Sort key:      published_at (ISO 8601 for chronological queries)
            TTL:           30 days (sentiment has longer analytical value
                           than raw trades — useful for backtesting)

        Conditional write: skip if article_id already exists to handle
        Kafka replay without creating duplicate sentiment records.

        Args:
            result: Analyzed sentiment result to persist

        Returns:
            True if written, False if duplicate or error
        """
        try:
            item = asdict(result)

            # TTL: 30 days from now
            item['ttl'] = int(time.time()) + (30 * 24 * 60 * 60)

            # Conditional write — skip duplicates silently
            self.sentiment_table.put_item(
                Item=item,
                ConditionExpression='attribute_not_exists(article_id)'
            )
            return True

        except ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code == 'ConditionalCheckFailedException':
                # Duplicate article — expected on Kafka replay, not an error
                logger.debug(
                    f"Duplicate sentiment skipped: {result.article_id}"
                )
                return False
            else:
                logger.error(
                    f"DynamoDB write failed for sentiment "
                    f"{result.article_id}: {e}"
                )
                return False
