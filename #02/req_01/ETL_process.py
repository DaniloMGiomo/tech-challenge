from Scraper import Scraper
from Transform import Transform
from Load import Load

import pandas as pd
from datetime import datetime

import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(levelname)s - %(message)s")

class ETL:
    def __init__(self):
        self.logger: logging.Logger = logging.getLogger(__name__)
        self.load_process = Load()
    
    def extract(self) -> pd.DataFrame:
        scraper = Scraper()
        df = scraper.get()
        self.logger.info("ETL.extract: COMPLETED")
        name = f"bronze/{datetime.now().strftime('%Y%m%d%H%M%S')}_raw.parquet"
        self.load_process.upload_parquet(df, name)
        return df
    
    def transform (self, df: pd.DataFrame) -> pd.DataFrame:
        transform = Transform(df=df)
        df = transform.process()
        self.logger.info("ETL.transform: COMPLETED")
        return df

    def load(self, df: pd.DataFrame) -> None:
        name = f"silver/{datetime.now().strftime('%Y%m%d%H%M%S')}_transformed.parquet"
        self.load_process.upload_parquet(df, name)
        self.logger.info("ETL.load: COMPLETED")

if __name__ == '__main__':
    etl = ETL()
    df = etl.extract()
    df = etl.transform(df)
    etl.load(df)