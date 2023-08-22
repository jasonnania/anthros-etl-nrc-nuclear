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

NRC_NUCLEAR_REACTOR_STATUS_TO_ENERGYTOOLS_DF_ETL = Pipeline([
                                                NRCNuclearReactorExtractor(),
                                                BytesCSVToDataFrameDataTransformer(),
                                                DataFrameToEnergyToolsDataFrameDataTransformer()
                                                 ])

if __name__ == '__main__': 
    import sys
    logging.basicConfig(stream=sys.stdout, level=logging.INFO)
    etl_pl = NRC_NUCLEAR_REACTOR_STATUS_TO_ENERGYTOOLS_DF_ETL
    
    payload, params = etl_pl.process(payload=None, params={})