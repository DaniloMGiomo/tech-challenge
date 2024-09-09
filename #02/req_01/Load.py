from io import BytesIO
import pandas as pd
import boto3

import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(levelname)s - %(message)s")

class Load:
    def __init__(self, my_bucket = "tech-challenge-02") -> None:
        self.aws_access_key_id = '------------'
        self.aws_secret_access_key = '------------'
        self.region = 'us-east-2'
        self.my_bucket = my_bucket
        self.logger: logging.Logger = logging.getLogger(__name__)
        self.init()

    def init(self) -> None:        
        self.s3 = boto3.resource(
                's3',
                aws_access_key_id = self.aws_access_key_id,
                aws_secret_access_key = self.aws_secret_access_key,
                region_name = self.region
            )
        self.logger.info("Load.init: boto3 resource initialized")
    
    def upload_parquet(self, df: pd.DataFrame, name: str) -> None:
        parquet_buffer = BytesIO()
        df.to_parquet(parquet_buffer, index=False, engine='pyarrow', compression='gzip')
        self.s3.Object(self.my_bucket, name).put(Body=parquet_buffer.getvalue())
        self.logger.info(f"Load.upload_parquet: {name} uploaded to {self.my_bucket}")
    
    def download_parquet(self, name: str) -> pd.DataFrame:
        try:
            obj = self.s3.Object(self.my_bucket, name).get()
            parquet_buffer = BytesIO(obj['Body'].read())
            df = pd.read_parquet(parquet_buffer, engine='pyarrow')
            self.logger.info(f"Load.download_parquet: {name} downloaded from {self.my_bucket}")
            return df
        except Exception as e:
            self.logger.error(e)
            return None