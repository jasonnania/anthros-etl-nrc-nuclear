from anthros_etl.extractors.http_extractor import HTTPExtractor
import logging
import datetime as dt
import pandas as pd
import io
import requests
from bs4 import BeautifulSoup
import os

class NRCPowerReactorExtractor(HTTPExtractor):
    logger = logging.getLogger(__name__)

    def __init__(self) -> None:
        super().__init__(
            data_source_url='https://www.nrc.gov/reading-rm/doc-collections/event-status/reactor-status/{0}/{1}ps.html',
            # https://www.nrc.gov/reading-rm/doc-collections/event-status/reactor-status/2023/20230818ps.html
            data_source_name='NRC',
            data_provider_name='NRC',
            user_id=None,
            data_date_format='%Y%m%d'
        )

    @property
    def start_date(self) -> str:
        return

    @property
    def end_date(self) -> str:
        return

    @property
    def request_params(self) -> str:
        return {}

    @property
    def content_type(self) -> str:
        return 'text/html'

    @classmethod
    def _test_get_instance(cls, *args, **kwargs):
        extractor = cls(*args, **kwargs)
        return extractor

    @classmethod
    def _test_get_params(cls) -> dict:
        params: dict = {'request_args': {'verify': False}, 'data_date': dt.date.today() - dt.timedelta(days=1)}
        return params
    
    def process_impl(self, params: dict, payload: bytes = None) -> tuple[object, dict]:
        url = self.data_source_url
        today = dt.date.today() - dt.timedelta(days=1)
        formatted_year = str(today.year)
        formatted_date = today.strftime('%Y%m%d')
        
        # Replace year and date placeholders in the URL
        url = url.format(formatted_year, formatted_date)
        params = {}
        
        response = requests.get(url)
        
        # check for successful request
        if response.status_code == 200:
            
            payload = response.text
            
            # Parse the HTML content using BeautifulSoup
            soup = BeautifulSoup(payload, 'html.parser')

            # Find all tables with class 'power'
            tables = soup.find_all('table', class_='power')

            # Initialize lists to store the extracted data
            report_dates = []
            regions = []
            units = []
            powers = []

            # Iterate through each table and extract data
            for region, table in enumerate(tables, start=1):
                table_rows = table.find_all('tr')[1:]  # Skip the header row
                report_date = formatted_date  # Assuming this is constant for the entire table

                for row in table_rows:
                    columns = row.find_all('td')
                    unit = columns[0].text.strip()
                    power = columns[1].text.strip()

                    report_dates.append(report_date)
                    regions.append(region)
                    units.append(unit)
                    powers.append(power)

            # Create a DataFrame with the extracted data
            data = {
                'Report Date': report_dates,
                'Region': regions,
                'Unit': units,
                'Power': powers
            }
            df = pd.DataFrame(data)
            
            csv_bytes = df.to_csv(index=False).encode('utf-8')
            
            payload = csv_bytes
            
            return payload, params
            
        else:
            return "Could not download bytes content"


if __name__ == '__main__':
    extractor = NRCPowerReactorExtractor()
    params = {
        'data_date': dt.date.today()
    }
    payload, params = extractor.process(params=params)