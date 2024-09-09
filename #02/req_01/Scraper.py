import logging
import requests
import pandas as pd

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(levelname)s - %(message)s")

class Scraper():
    def __init__(self):
        """
        Initializes the Scraper class with default attributes.
        #
        Attributes:
            headers (dict): Headers to be used in the request.
            url (str): URL to be used in the request.
            logger (logging.Logger): An instance of the logging module to record and view events.
        """
        self.headers = {
            'Accept': 'application/json, text/plain, */*',
            'Accept-Language': 'en-US,en;q=0.9',
            'Connection': 'keep-alive',
            'Referer': 'https://sistemaswebb3-listados.b3.com.br/',
            'Sec-Fetch-Dest': 'empty',
            'Sec-Fetch-Mode': 'cors',
            'Sec-Fetch-Site': 'same-origin',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36',
            'sec-ch-ua': '"Chromium";v="128", "Not;A=Brand";v="24", "Google Chrome";v="128"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"Windows"',
            }
        self.url = 'https://sistemaswebb3-listados.b3.com.br/indexProxy/indexCall/GetPortfolioDay/eyJsYW5ndWFnZSI6InB0LWJyIiwicGFnZU51bWJlciI6MSwicGFnZVNpemUiOjEyMCwiaW5kZXgiOiJJQk9WIiwic2VnbWVudCI6IjEifQ=='
        self.logger: logging.Logger = logging.getLogger(__name__)
    # 
    def get(self) -> pd.DataFrame|Exception:
        """
        Make a GET request to the provided URL and return a pandas DataFrame containing the response data.
        #
        Returns:
            pd.DataFrame: A pandas DataFrame containing the response data.
        #
        Raises:
            Exception: If the request is not successful (status code different from 200) or if the response data is not a valid JSON.
        """
        self.logger.info("Scraper.get: requesting data from url: %s", self.url)
        response = requests.get(
            url=self.url,
            headers=self.headers,
            )
        # 
        self.logger.info("Scraper.get: response status code: %s", response.status_code)
        if response.status_code == 200:
            try:
                results = response.json()['results']
                df = pd.DataFrame(results)
                self.logger.info("Scraper.get: resquesting data is successful")
                return df
            except Exception as e:
                raise Exception(f"Scraper.get: resquesting data is not successful. Error: {e}")
        else:
            raise Exception(f"Scraper.get: response status code is not 200. Error: {response.text}")

if __name__ == "__main__":
    scraper = Scraper()
    df = scraper.get()
    print(df)