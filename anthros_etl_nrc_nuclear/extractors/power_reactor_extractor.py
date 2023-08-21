from anthros_etl.extractors.http_extractor import HTTPExtractor
import logging
import datetime as dt
import pandas as pd
import io
import requests

class SourceDataExtractor(HTTPExtractor):
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
            # load the data into payload
            payload = response.content
            
            return payload, params
        else:
            return "Could not download bytes content"


if __name__ == '__main__':
    extractor = SourceDataExtractor()
    run_params = {
        'data_date': dt.date.today()
    }
    payload, params = extractor.process(params=run_params)

    test_result = SourceDataExtractor._test_run()

    csv_data = pd.read_csv(io.BytesIO(payload))
    print(csv_data)
    pass