# Deployment Guide

TradePulse is deployed on Railway serving the FastAPI dashboard in demo mode. The full Kafka + Faust pipeline runs locally for development and recording.

## Railway Deployment (Public Dashboard)

### First-time setup

1. Install Railway CLI:

```bash
npm install -g @railway/cli
railway login
```

2. Initialize and deploy:

```bash
railway init
railway up
```

3. Set environment variables in Railway dashboard:

   - `DEMO_MODE=true`
   - `AWS_REGION=us-east-1`
   - `AWS_ACCESS_KEY_ID=your_key` (read-only IAM user recommended)
   - `AWS_SECRET_ACCESS_KEY=your_key`
   - `POLYGON_API_KEY=your_key`
   - `FINNHUB_API_KEY=your_key`

4. Generate domain: Railway dashboard → your service → Settings → Domains

5. Add custom domain: point tradepulse.dev CNAME to Railway domain in Namecheap DNS

### Redeployment (after pushing changes)

Railway auto-deploys on every push to main if GitHub integration is enabled. Manual redeploy:

```bash
railway up
```

## Local Full Pipeline

Runs all services including Kafka, Faust, and both producers:

```bash
make up           # Start everything
make logs         # Tail all service logs
make down         # Stop everything
```

## Environment Variables

| Variable | Railway | Local | Description |
|----------|---------|-------|-------------|
| DEMO_MODE | true | false | Simulated vs real data |
| AWS_REGION | us-east-1 | us-east-1 | AWS region |
| POLYGON_API_KEY | required | required | Live equity data |
| FINNHUB_API_KEY | required | required | News sentiment |
| KAFKA_BOOTSTRAP_SERVERS | not needed | localhost:9092 | Kafka broker |

## Cost Estimate

Railway free tier: $5 credit/month — sufficient for dashboard only  
Full pipeline locally: no ongoing cost
