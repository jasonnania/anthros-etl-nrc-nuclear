import logging 
import anthros_core
import tempfile
import datetime as dt

from anthros_etl.loaders.timeseries_spot_loader import TimeSeriesSpotTableDataLoader
from anthros_etl_nrc_power.etls.power_reactor_et_etl import NRC_POWER_REACTOR_STATUS_TO_ENERGYTOOLS_DF_ETL as base_pl
from anthros_etl.loaders.timeseries_spot_loader import TimeSeriesSpotTableDataLoader
from anthros_etl.inserters.bcp_sql_server_table_inserter import TableInserterBCPInsertSQLServer

logger = logging.getLogger(__name__)

def get_etl_pipeline():
    loader = TimeSeriesSpotTableDataLoader(process_name='NRC_POWER_REACTOR_ET_ETL',
                                           inserter=TableInserterBCPInsertSQLServer(data_dir=tempfile.gettempdir())
                                           )
    pl = base_pl + [loader]
    return pl

def etl(dt_from:dt.date=dt.date.today(),
        dt_to:dt.date=dt.date.today()+dt.timedelta(days=1)-dt.timedelta(seconds=1)):
    
    pl = get_etl_pipeline()
    params = {'dt_from':dt_from, 'dt_to':dt_to}
    payload, params = pl.process(payload=None, params=params)
    return payload, params
    

if __name__ == '__main__':
    
    payload, params = etl()


# EVENTUALLY ADD BACKFILLING AFTER CODE WORKS FOR CURRENT DAY FILE
# def backfill(dt_from:dt.date=dt.date.today()-dt.timedelta(days=30),
#               dt_to:dt.date=dt.date.today(),
#               day_increment:int=6):
    
#     start = dt_from
#     end = min(start + dt.timedelta(days=day_increment), dt_to)
    
#     while start <= dt_to:
#         etl(dt_from=start, dt_to=end)
#         start = end + dt.timedelta(days=1)
#         end = min(start + dt.timedelta(days=day_increment), dt_to)                                          

