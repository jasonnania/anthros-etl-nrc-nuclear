from anthros_etl_nrc_nuclear.extractors.power_reactor_extractor import NRCNuclearReactorExtractor
from anthros_etl_nrc_nuclear.transformers.power_reactor_transformer import NRCNuclearReactorTransformer

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

NYISO_REAL_TIME_LBMP_TO_ENERGYTOOLS_DF_ETL = Pipeline([
                                                NRCNuclearReactorExtractor(),
                                                BytesCSVToDataFrameDataTransformer(),
                                                DataFrameToEnergyToolsDataFrameDataTransformer()
                                                 ])


NYISO_REAL_TIME_LBMP_ENERGYTOOLS_DF_TO_TSDB_DF = Pipeline([NRCNuclearReactorTransformer(),
                                                           TSDBSetMetaIdsTransformer()])


NYISO_REAL_TIME_LBMP_TO_TSDB_DF = NYISO_REAL_TIME_LBMP_TO_ENERGYTOOLS_DF_ETL + NYISO_REAL_TIME_LBMP_ENERGYTOOLS_DF_TO_TSDB_DF #+ [TimeSeriesSpotTableLoader(process_name = 'NYISO_MARKET_LBMP')]

if __name__ == '__main__': 
    import sys
    logging.basicConfig(stream=sys.stdout, level=logging.INFO)
    etl_pl = NYISO_REAL_TIME_LBMP_TO_TSDB_DF
    
    payload, params = etl_pl.process(payload=None, params={}) 