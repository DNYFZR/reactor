# Sample from Multiple Sources in Pyspark - AR input data related - better name ? 
import re, numpy as np, pandas as pd
from typing import List, Union

from pyspark.sql import functions as F
from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession

spark = SparkSession(SparkContext.getOrCreate())

def multisource_input( n_iters: int, id_source: Union[str, pd.DataFrame], id_source_col: str, data_sources: List[Union[str, pd.DataFrame]], 
                        ds_id_cols: list, ds_data_regex:  List[Union[str, list]], added_column_name: str = 'state', 
                        models: Union[str, dict, None] = None, converters: Union[str, dict, None] = None ):
    '''
    Configure the base inventory attribute using the load_method and input_map parameters.

    Parameters
    ---
    n_iters : int 
        number of times to repeat sampling for each unique id
    id_source : str | DataFrame
        csv filepath or in memory dataframe source from which uniques ids will be extracted
    id_source_col : str
        name of column in id_source containing unique ids
    data sources : List[str | DataFrame]
        list of filepaths / dataframes containing data to sample for each unique id
    ds_id_cols : List[str]
        list of id column names for each item in data_sources
    ds_data_regex : List[str | list]
        list of lists or regex string patterns to match with columns in each entry in data_sources, 
        which contain relevant data for each unique id
    models : None | str | dict (optional)
        if string - look for column in id_source to match with unique ids. 
        if dict - map to unique ids. 
        if None - default to "general" 
    converters : None | str | dict (optional)
        if string - look for column in id_source to match with unique ids. 
        if dict - map to unique ids. 
        if None - default to 1 

    Returns
    ---
        self : updated inventory attribute - pyspark.sql.dataframe.DataFrame

    Raises
    ---
        ValueError : if load_method or input_map kwargs improperly set
        TypeError : if unsupported object type passed via input_map kwargs
    '''
    # Create base Spark DF
    if isinstance(id_source, str):
        base = spark.read.csv(id_source, header=True, inferSchema=True).select(id_source_col).withColumnRenamed(id_source_col, 'id')

    elif isinstance(id_source, pd.DataFrame):
        base = spark.createDataFrame(id_source).select(id_source_col).withColumnRenamed(id_source_col, 'id')

    else:
        raise TypeError(f'''Input object "id_source" ({id_source}) is not of type string or pandas.core.dataframe.''')

    # Create source Spark DF's & join with base
    for n, ds in enumerate(data_sources):
        
        # Read or convert source to Spark DF
        if isinstance(ds, str):
            temp = spark.read.csv(ds, header=True, inferSchema=True).withColumnRenamed(ds_id_cols[n], 'id')
            temp = temp.select('id', *[i for i in temp.columns if re.match(ds_data_regex[n], i)])

        elif isinstance(ds, pd.DataFrame):
            ds = ds[[ ds_id_cols[n], *[i for i in ds.columns if re.match(ds_data_regex[n], i)] ]].copy()

            ds_schema = {col : str if col == ds_id_cols[n] else float for col in ds.columns}
            ds = ds.astype(ds_schema)

            temp = spark.createDataFrame(ds).withColumnRenamed(ds_id_cols[n], 'id')

        else:
            raise TypeError(f'''Input object at index {n} in "data_sources" is not of type string or pandas.core.dataframe.''')
    
        # Create value array in source
        temp = temp.withColumn(f'arr_{n}', F.array(*[i for i in temp.columns if i != 'id']) )
    
        # Join source to base
        base = base.join(temp.select('id', f'arr_{n}'), on='id', how='left')

    # Coalesce sources & drop unrequired cols & null values
    base = base.withColumn('arr', F.coalesce(*[base[f'arr_{n}'] for n, d in enumerate(data_sources)]))
    base = base.select('id', 'arr').dropna() # remove IDs with no source data with dropna

    # Random sample & expand
    for n in range(n_iters):
        # Random sample from arr & add value & model_iter cols to base
        if n == 0:
            base = base.rdd.map(
                lambda row: (row['id'], row['arr'], int(np.random.choice(row["arr"])))
                ).toDF(['id', 'arr', added_column_name]
                ).withColumn('model_iteration', F.lit(n+1))
        
        # Repeat above on initial base (model_iter == 1), update value & model_iter cols, and union with base
        else:
            base = base.union(base[base['model_iteration'] == 1].rdd.map(
                lambda row: (row['id'], row['arr'], int(np.random.choice(row["arr"])))
                ).toDF(['id', 'arr', added_column_name]
                ).withColumn('model_iteration', F.lit(n+1)) )
    
    # Repartition after expansion
    base = base.repartition(int(spark.conf.get('spark.sql.shuffle.partitions')))

    # Add models & converter columns   
    for var_name, var_val in {'model': models, 'converter': converters}.items():
        # Set to "general" or int(1) if None
        if var_val is None:
            if var_name == 'model':
                base = base.withColumn(var_name, F.lit("general"))
            else:
                base = base.withColumn(var_name, F.lit(1))
    
        # Read from id source if str
        elif isinstance(var_val, str):
            # If id source is filepath then read, else create spark DF from pd data frame
            if isinstance(id_source, str):
                temp = spark.read.csv(id_source, header=True, inferSchema=True).select(id_source_col, var_val)
            else:
                temp = spark.createDataFrame(id_source).select(id_source_col, var_val)

            temp = temp.withColumnRenamed(id_source_col, 'id').withColumnRenamed(var_val, var_name)
            base = base.join(F.broadcast(temp), on='id', how='left')
        
        # Map to RDD if dict
        elif isinstance(var_val, dict): 
            base = base.rdd.map( lambda row: (*row, var_val[row['id']]) ).toDF([*list(base.columns), var_name])
        
        # Else raise type error
        else:
            raise TypeError(f'''Input variable "{var_name}" is not of accepted type (None | str | dict) : {type(var_val)}''')
        
        # Select only required columns
        required = ['id', 'state', 'model', 'converter', 'model_iteration']
            
    return base.select(required)