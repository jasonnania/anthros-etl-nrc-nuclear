from anthros_etl_nrc_power.extractors.power_reactor_extractor import NRCPowerReactorExtractor
from anthros_etl_nrc_power.transformers.power_reactor_transformer import NRCPowerReactorTransformer

from anthros_etl.pipelines.pipeline import Pipeline
from anthros_etl.data_transformer import DataTransformer
import logging
import tempfile
from typing import Any
import datetime as dt
import pandas as pd

from anthros_etl.transformers.bytes_to_df_transformer import BytesCSVToDataFrameDataTransformer
from anthros_etl.transformers.df_to_energytools_df_transformer import DataFrameToEnergyToolsDataFrameDataTransformer
from anthros_etl.transformers.tsdb_set_meta_ids_transformer import TSDBSetMetaIdsTransformer

NRC_POWER_REACTOR_STATUS_TO_ENERGYTOOLS_DF_ETL = Pipeline([
                                                NRCPowerReactorExtractor(),
                                                BytesCSVToDataFrameDataTransformer(),
                                                DataFrameToEnergyToolsDataFrameDataTransformer()
                                                 ])


NRC_POWER_REACTOR_STATUS_ENERGYTOOLS_DF_TO_TSDB_DF = Pipeline([NRCPowerReactorTransformer(),
                                                           TSDBSetMetaIdsTransformer()])


NRC_POWER_REACTOR_STATUS_TO_TSDB_DF = NRC_POWER_REACTOR_STATUS_TO_ENERGYTOOLS_DF_ETL + NRC_POWER_REACTOR_STATUS_ENERGYTOOLS_DF_TO_TSDB_DF #+ [TimeSeriesSpotTableLoader(process_name = 'NYISO_MARKET_LBMP')]

if __name__ == '__main__': 
    import sys
    logging.basicConfig(stream=sys.stdout, level=logging.INFO)
    etl_pl = NRC_POWER_REACTOR_STATUS_TO_TSDB_DF
    
    payload, params = etl_pl.process(payload=None, params={})