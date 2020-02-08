#!/usr/bin/env python3
# stage 3: sorting and merging data

import numpy as np
import pandas as pd
#from canalespy import setup_environment, logger
import logging

from glob import glob
from parsons import Table, Redshift, utilities
import os

def main():
    
    #setup_environment()

    #Logging

    logging.basicConfig()
    logger = logging.getLogger(__name__)
    _handler = logging.StreamHandler()
    _formatter = logging.Formatter('%(levelname)s %(message)s')
    _handler.setFormatter(_formatter)
    logger.addHandler(_handler)
    logger.setLevel('INFO')


    #create an instance of redshift 
    rs = Redshift()
    

    STAGE2_GLOB = 'stage2.*.csv'

    SCHEMA = {
        'congressional_district': np.float64,
        'state_house_district': np.float64,
        'state_senate_district': np.float64,
        'n_guns_involved': np.float64,
    }

    def load_csv(csv_fname):
        return pd.read_csv(csv_fname,
                           dtype=SCHEMA,
                           parse_dates=['date'],
                           encoding='utf-8')

    def inner_sort(dfs):
        for df in dfs:
            assert all(~df['date'].isna())
            df.sort_values('date', inplace=True)

    def outer_sort(dfs):
        # If the first incident in one file took place earlier than the first incident in another,
        # we assume all incidents in the former took place earlier than all incidents in the latter.
        dfs.sort(key=lambda df: df.loc[0].date)

    def main():
        # Sort the dataframes by ascending date, then sort by ascending date *within* each dataframe,
        # then merge into 1 giant CSV.
        dfs = [load_csv(fname) for fname in glob(STAGE2_GLOB)]
        inner_sort(dfs)
        outer_sort(dfs)

    giant_df = pd.concat(dfs, ignore_index=True)
    #giant_df.to_csv('stage3.csv',
                    #index=False,
                    #float_format='%g',
                    #encoding='utf-8')

    # Convert dataframe to a parsons table 

    final_table = Table.from_dataframe(giant_df)

    #Push table to redshift 

    final_table.to_redshift('cjaf_gvp.gva_2019_data', if_exists='drop')

    logger.info(f"Successfully created GVA 2019 Data Table")


if __name__ == '__main__':
    main()
