import pandas as pd

def standardize_date(date):
    # Convert "31/12/86" to "1986-12-31"
    day, month, year = date.split('/')
    return f"19{year}-{month}-{day}"

def clean_stock_data(input_file, output_file):
    df = pd.read_csv(input_file)
    df['Date'] = df['Date'].apply(standardize_date)
    df['Volume'] = df['Volume'].replace(0, 'N/A')
    df.to_csv(output_file, index=False)
    print(f"Cleaned data saved to {output_file}")

# Usage
clean_stock_data("stockData.csv", "cleaned_stockData.csv")
