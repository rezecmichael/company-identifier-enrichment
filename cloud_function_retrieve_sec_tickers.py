import pandas
from datetime import datetime
from google.cloud import storage

def get_tickers(url, filename):
    df = pandas.read_csv(url, sep='\t', header=None, names=['ticker','cik'])
    df.ticker = df.ticker.str.upper()
    df = df[df.ticker.notnull()].reset_index(drop=True)
    df = df.assign(TICKER=df['ticker'].apply(lambda x: x.replace("-",".")))
    df = df.assign(DATE=datetime.utcnow().date())
    print(f"Unique tickers: {df.TICKER.nunique()}")

    project = 'your-google-cloud-project'
    bucket = 'your-google-cloud-bucket'

    client = storage.Client(project=project)
    bucket = client.get_bucket(bucket)
    blob = bucket.blob(filename)
    blob.upload_from_string(df.to_csv(index = False), content_type = 'csv')

def main(data, context):
  today = datetime.utcnow().date()
  get_tickers("https://www.sec.gov/include/ticker.txt", f"{today}_sec_tickers.csv")
