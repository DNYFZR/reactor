# Licence: Copyright Scottish Water - Private & Confidential 
# Flow Predict : AR3 Input Data Profile
# Created By: D.Fraser 

import re, numpy as np, pandas as pd, matplotlib.pyplot as plt, seaborn as sns
from typing import List, Union

class DataProfiler:
    def __init__(self) -> None:
        pass

    @staticmethod
    def generate_sample_arrays(ids: Union[list, tuple, set], data_sources: List[Union[str, pd.DataFrame]], ds_id_cols: List[str], ds_data_cols: List[Union[str, list]]) -> dict:
        '''Generate a dictionary of unique ids (keys) with an associated array of data (values) sampled from items in data_sources[ ds_data_cols] on matching ids in ds_id_cols.
        
        The method operates sequentially through data_sources, such that once an id has been locted in an item in data_sources, it will cease seaching for that id.
        As such data inputs should be ordered by preference for sourcing in cases where the ID is present in multiple datasets.

        
        Parameters
        --- 
        ids: array-like
          list of unique IDs to map data to 
        data_sources: list of string or pandas DataFrame objects
          sources to generate sample arrays from, should be csv filepath if string type
        ds_id_cols : list of strings
          an entry for each entry in data_sources, containing the unique id column name
        ds_data_cols: list of sting or list type objects 
          a regex pattern - or - list of columns, from which data is to be sampled per entry in data_sources 
    
        Returns
        ---
        dictionary object

        Raises
        ---
        None  
        '''
        # Read source data items into memory if required 
        for n, source in enumerate(data_sources):
            if isinstance(source, str):
                data_sources[n] = pd.read_csv(source)

        ref_set = {}
        for n, dataset in enumerate(data_sources):
            # set index to id column and select only cols in data columns list for this dataset by regex match or from list provided
            if isinstance(ds_data_cols[n], str): 
                select_cols = [col for col in dataset.columns if re.match(ds_data_cols[n], col)]
                dataset = dataset.set_index(ds_id_cols[n])[select_cols].copy()
            
            if isinstance(ds_data_cols[n], list):
                dataset = dataset.set_index(ds_id_cols[n])[ds_data_cols[n]].copy()
            
            # Set up new key / val pairings to be added to main ref set
            new_mapping = {i : dataset.loc[i, :].to_numpy().reshape(-1) for i in ids if i not in ref_set.keys() and i in dataset.index}
            
            # Merge with main dict
            ref_set = {**ref_set, **new_mapping}
        
        return ref_set

    def load_ids(self, **kwargs):
        ### Inventory (90k)
        inventory = pd.read_csv(r"/dbfs/mnt/bi_base/Corporate/FlowPulse/AR21v1/Long Term Projections/Interim/2021/inventory_baseline.csv", low_memory=False)

        # Single inventory item issue: id_bug is not in current age infill datasets - 05/09/22 - DF / SH agreed to filter out
        id_bug = 'STW00093734SEPX001NR01-----'
        inventory = inventory[inventory['asset_id'].str.contains(id_bug) == False].copy().reset_index(drop=True)


        ## Base data (>100k raw -> 90k)
        base = pd.read_csv(r"/dbfs/mnt/bi_raw/Corporate/FlowPulse/AR21v1/Long Term Projections/2021/operational_inventory_with_age_data_exclusions_removed.csv", usecols=['plant.no', 'install_date', 'current_age'])
        base = base[base['plant.no'].isin(inventory['asset_id'])].copy()

    def preprocessing(self, **kwargs):

        ## Raw infill datasets
        ages_valid = pd.read_csv(r"/dbfs/mnt/bi_base/Corporate/FlowPulse/AR21v1/Long Term Projections/Interim/2021/op_inventory_valid_install.csv", 
                                index_col=0, 
                                low_memory=False, 
                                usecols=['plant.no', 'rounded_age']).reset_index()

        ages_missing = pd.read_csv(r"/dbfs/mnt/bi_base/Corporate/FlowPulse/AR21v1/Long Term Projections/Interim/2021/op_inventory_imputed_with_age_data.csv", 
                                index_col=0, 
                                low_memory=False)
        ages_missing = ages_missing.copy().set_index('plant.no').iloc[:, -500:].reset_index()

        ages_invalid = pd.read_csv(r"/dbfs/mnt/bi_base/Corporate/FlowPulse/AR21v1/Long Term Projections/Interim/2021/op_inventory_imputed_no_age_data_INFILLED.csv", 
                                index_col=0, 
                                low_memory=False)
        ages_invalid = ages_invalid.copy().set_index('plant.no').iloc[:, -500:].reset_index()

        ## ages_missing has mixed missing & invalid type data - DF / SH call 21/09/22
        # Use plant.no & install_date cols to determine if asset is of 'missing' or 'invalid' (present in infill_map dataset)
        infill_map = base.set_index('plant.no')['install_date'].to_dict()
        infill_map = {k: 'missing' if pd.isna(v) else 'invalid' for k, v in infill_map.items()}

        # Map filter
        ages_missing['infill_type'] = ages_missing['plant.no'].map(infill_map)

        # Split data
        ages_transfer = ages_missing[ages_missing['infill_type'] == 'invalid'].copy()
        ages_missing = ages_missing[ages_missing['infill_type'] == 'missing'].copy()

        # Drop filter col
        ages_transfer = ages_transfer.drop(columns=['infill_type'])
        ages_missing = ages_missing.drop(columns=['infill_type']).reset_index(drop=True)

        # Align cols
        ages_transfer.columns = ages_invalid.columns

        # Re-assign to correct dataset
        ages_invalid = pd.concat([ages_invalid.copy(), ages_transfer], axis=0).reset_index(drop=True)
        del ages_transfer

    def process(self, **kwargs):
        # Collect all possible ages for each ID using GenerateInput staticmethod
        valid = self.generate_sample_arrays(
            ids=inventory['asset_id'].values, 
            source_data=[ages_valid, ], 
            source_id_cols=['plant.no'], 
            source_data_cols=[['rounded_age']], 
        )

        missing = self.generate_sample_arrays(
            ids=inventory['asset_id'].values, 
            source_data=[ages_missing], 
            source_id_cols=['plant.no'], 
            source_data_cols=[[i for i in ages_missing.iloc[:, -500:].columns] ], 
        )

        invalid = self.generate_sample_arrays(
            ids=inventory['asset_id'].values, 
            source_data=[ages_invalid], 
            source_id_cols=['plant.no'], 
            source_data_cols=[[i for i in ages_invalid.iloc[:, -500:].columns]], 
        )


        split = {
            'valid': round(len(valid) / len(inventory), 3),
            'invalid': round(len(invalid) / len(inventory), 3),
            'missing': round(len(missing) / len(inventory), 3), 
            'all': round((len(valid) + len(missing) + len(invalid)) / len(inventory), 3)
        }

        print(split)

        ### Stacked bar chart format

        # Add status & round age
        base['status'] = ['valid' if k in valid.keys() else 'missing' if k in missing.keys() else 'invalid' if k in invalid.keys() else np.nan for k in base['plant.no']]
        base['current_age'] = [np.nan if pd.isna(i) else int(round(i)) for i in base['current_age']]

        # Base dist by status
        base_v = base[base['status'] == 'valid']['current_age'].value_counts().sort_index().rename('valid') / len(inventory)
        base_m = base[base['status'] == 'missing']['current_age'].value_counts().sort_index().rename('missing') / len(inventory)
        base_i = base[base['status'] == 'invalid']['current_age'].value_counts().sort_index().rename('invalid') / len(inventory)

        # Create df
        base_c = pd.concat([base_v, base_m, base_i], axis = 1).fillna(0)
        base_c.index = [int(i) for i in base_c.index]


        ## Infilled data
        infill_v = pd.Series({k: np.random.choice(v) for k, v in valid.items()}).value_counts().sort_index().rename('valid') / len(inventory)
        infill_m = pd.Series({k: np.random.choice(v) for k, v in missing.items()}).value_counts().sort_index().rename('missing') / len(inventory)
        infill_i = pd.Series({k: np.random.choice(v) for k, v in invalid.items()}).value_counts().sort_index().rename('invalid') / len(inventory)

        infill_c = pd.concat([infill_v, infill_m, infill_i], axis = 1)


        ## Base data + missing from infill data
        middle = pd.concat([base_c[['valid', 'invalid']], infill_c['missing']], axis=1).fillna(0)
        middle = middle[['valid', 'missing', 'invalid']].copy()

    def plot(self, **kwargs):
        sns.set_theme()

        fig, ax = plt.subplots(nrows=3, figsize = (22, 16), sharex=True, sharey=True)

        # Base
        ax[0].bar(x = base_c.index,  height = base_c['valid'])
        ax[0].bar(x = base_c.index,  height = base_c['missing'], bottom=base_c['valid'])
        ax[0].bar(x = base_c.index,  height = base_c['invalid'], bottom=(base_c['missing'] + base_c['valid']))
        ax[0].set_title('Base', size=14)

        # Middle
        ax[1].bar(x = base_c.index,  height = middle['valid'])
        ax[1].bar(x = base_c.index,  height = middle['missing'], bottom=middle['valid'])
        ax[1].bar(x = base_c.index,  height = middle['invalid'], bottom=(middle['missing'] + middle['valid']))
        ax[1].set_title('Base + Missing Infilled', size=14)

        # Infilled
        ax[2].bar(x = infill_c.index,  height = infill_c['valid'])
        ax[2].bar(x = infill_c.index,  height = infill_c['missing'], bottom=infill_c['valid'])
        ax[2].bar(x = infill_c.index,  height = infill_c['invalid'], bottom=(infill_c['missing'] + infill_c['valid']))
        ax[2].set_title('Infilled', size=14)
        ax[2].set_xlabel('Age (years)', size=14)

        plt.legend(['valid', 'missing', 'invalid'])
        plt.show()
