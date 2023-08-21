from typing import Any
import pandas as pd
import datetime
import pytz

from anthros_etl.data_transformer import DataTransformer


class NYISODayAheadMarketLBMPTransformer(DataTransformer):
    
    def process_impl(self, payload: Any, params: dict) -> tuple[Any, dict]: 
        df = payload
        
        # set meta IDs (these are just examples, change based on data type)
        df['CommodityId'] = 'P'
        df['CategoryId'] = 'PRICE'
        df['SubCategoryId'] = 'RT' 
        df['FrequencyId'] = 'D'
        df['SeriesTypeId'] = 'S'
        df['ForecasterId'] = 'NRC'
        df['UoMId'] = 'MWh'
        df['TimezoneId'] = 'US/Central'
        df['LocalIdOrigin'] = 'US'

        df = df.rename(columns={'Component Type':'ValueKey',
                           'Name':'LocationIdOrigin'})
        
        # set LocationIdDestination to be the same as LocationIdOrigin
        df['LocationIdDestination'] = df['LocationIdOrigin']
        
                
        # Putting date in correct format
        df['Report Date'] = pd.to_datetime(df['Report Date'], format='%Y%m%d')
        df['Report Date'] = pd.to_datetime(df['Report Date']).dt.tz_localize('UTC')
        
        # add UTC time and DateLocal to satisfy metaID requirements
        df['DateUTC'] = df['Report Date'].dt.tz_convert('UTC')
        df['DateLocal'] = df['Report Date']
        
        payload = df
        
        return payload, params
        
        
        
        


# CommodityId = 'P'
# CategoryId = 'PRICE'
# SubCategory = 'RT' or 'DA' depending on whether it's Day-Ahead or Real-Time market
# FrequencyId = 'H'
# SeriesTypeid = 'S'
# LocationId = 'NYISO-' + <node> (we can use the AddLocationTimezones transformer to look up the location records in the next step)
# UoMId = '%/MWh'
# ForecasterId = 'ISO'
# ValueKey =  loss/congestion/lmp/energy depending the value (they should have separate entries for these)

