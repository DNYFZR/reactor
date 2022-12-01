# Configuration Engine - Main Class

import sys, re, numpy as np, pyspark
from typing import Union

from pyspark.sql import functions as F
from pyspark.sql.window import Window

from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession

class ConfigEngine:
    '''
    The Configuration Engine...
    '''
    def __init__(self, **kwargs):
        '''Use key word args to set optional initial class attrs'''
        global spark
        spark = SparkSession(SparkContext.getOrCreate())
        
        for key, val in kwargs.items():
            setattr(self, key, val)
            
    def load(self, attribute: str, **kwargs):
        '''
        Load data to a new spark DF attribute via kwargs:
            - Attribute (required): name of new class attribute
            - Loading a csv:
                - filepath (required) : If present, all other kwargs are applied to spark.read.csv
            - Loading arrays :
                - kwargs : Keys are loaded as column names, and values are loaded as column value arrays
        Returns : 
            - spark data frame assigned to attribute name provided
        Raises : 
            - IndexError if arrays are not of equal length
        '''
        if len(kwargs.keys()) == 0:
            raise ValueError('Cannot initialise empty Spark DataFrame')
        
        elif 'filepath' in kwargs.keys():
            path = kwargs['filepath']
            del kwargs['filepath']
            
            setattr(self, attribute , spark.read.csv(path, **kwargs))

        else:
            cols = list(kwargs.keys())
            data = list(i.tolist() if isinstance(i, np.ndarray) else i for i in kwargs.values())

            if not np.all([len(i) == len(data[0]) for i in data]):
                raise IndexError('All columns must be the same length')

            arr_len = len(data[0])
            data = [tuple(arr[n] for arr in data) for n in range(arr_len)]
            setattr( self, attribute,  spark.sparkContext.parallelize(data).toDF(cols) )

    def save(self, attribute: str, filepath: str, filetype: str = 'csv', **kwargs):
        '''
        Save attribute as filetype, located at filepath :
            - attribute: name of class attribute to save
            - filepath : path to save the file on
            - filetype : output type - csv (defualt) / parquet

        Returns : 
            - Updated self.data attribute
            - Spark session

        Raises : 
            - ValueError if invalid filetype is selected
            - AttributeError if an invalid attribute is selected
        '''
        # If sub-attr is called, ensure access 
        if '.' in attribute:
            attribute = attribute.split('.')
            
            for n, sub in enumerate(attribute):
                if n == 0: # top level attr
                    item = getattr(self, sub)
                else: # successive sub-attr level
                    item = getattr(item, sub)
        else:
            item = getattr(self, attribute)
        
        # Save Spark DF only
        if isinstance(item, pyspark.sql.DataFrame):
        
            if filetype.lower() == 'csv':
                item.toPandas().to_csv(filepath, **kwargs)
                return True

            elif filetype.lower() == 'parquet':
                item.write.parquet(filepath, **kwargs)
                return True

            else:
                raise ValueError('Please select a vaild filetype : csv / parquet')
        
        else:
            raise AttributeError('Please select an attribute that can be saved...')

    def fit(self, **kwargs):
        '''
        Fit data to model :
            - data attribute must be set, and is the initial input to model
            - all kwargs are passed to model
        
        Returns : 
            - model output assinged to attribute, default is data attribute.
            
        Raises : AttributeError if either data or model are not assigned when fit is called. 
        '''
        if 'data' not in dir(self) and 'model' not in dir(self):
            raise AttributeError('Cannot fit without data & model attributes set')
        else:
            setattr(self, self.model.__name__.lower(), self.model(self.data, **kwargs))
        
        return self
    
    def constrain(self,
        attribute: str = 'data', 
        constraint: Union[int, dict] = 1, 
        sum_by: str = 'converter', 
        partition_by: str = 'model_iteration', 
        iter_regex: str = 'timestep', 
        select_random: bool = False ):

        attribute = getattr(self, attribute)
        cols = attribute.columns

        if isinstance(constraint, dict):
            if not np.all([True if col in constraint.keys() else False for col in cols]):
                raise KeyError('Constraint dict must contain an entry for each column')

        elif not isinstance(constraint, (int, float)):
            raise ValueError('Constraint must be of type int, float or dict')

        for n, col in enumerate(cols): 
            prev_col = cols[n-1]

            if isinstance(constraint, dict):
                limit = constraint[col]
            else: 
                limit = constraint

            if re.match(iter_regex, col) and int(col.split('_')[1]) != 0:
                # Add cumulative sum column
                attribute = attribute.withColumn( 'applied',  F.when(attribute[col] == 0, F.col(sum_by)).otherwise(0) )

                # Apply ordering rule
                if select_random: 
                    attribute = attribute.withColumn('order_col', F.rand(seed=np.random.randint(1, 1000)) )
                    attribute = attribute.withColumn( 'total', F.sum('applied').over( Window.partitionBy(partition_by).orderBy(F.col('order_col').desc()).rowsBetween(-sys.maxsize, 0) ))
                    attribute = attribute.drop('order_col')

                else:
                    attribute = attribute.withColumn( 'total', F.sum('applied').over( Window.partitionBy(partition_by).orderBy(F.desc(prev_col)).rowsBetween(-sys.maxsize, 0)) )

                # Apply constraint in reverse order
                remaining_cols = cols[n:][::-1] # cols from last_col : col
                adjust_cols = remaining_cols[:-1] # cols from last_col : next_col

                # Update next vals from end of cols to curr step
                for x, a_col in enumerate(adjust_cols):
                    attribute = attribute.withColumn(a_col, F.when( (F.col('total') > limit) & (F.col(col) == 0), F.col(remaining_cols[x+1]) ).otherwise( F.col(a_col) ) )

                # Update current val
                attribute = attribute.withColumn(col, F.when( (F.col('total') > limit) & (F.col(col) == 0), F.col(prev_col) + 1 ).otherwise( F.col(col) ) )

                attribute = attribute.drop('applied', 'total')
            
        return self
    
    def convert(self, target_value: Union[int, float] = 0, iter_regex: str = 'step', converter: str = 'converter', convert_events: bool = False):
        '''
        Convert a target value to a converter or binary value
            - need to allow for list of target vals 
                - separate attrs to be set for 
                - or - create col for each 1_step_0 / 2_step_0 / 3_step_0... 
        '''
        for col in list(self.data.columns):   
            if re.match(iter_regex, col):

                # Modify to count / converter value
                if convert_events:
                    self.data = self.data.withColumn( col,  F.when(F.col(col) == target_value, F.col(converter)).otherwise(0) )
                else:
                    self.data = self.data.withColumn( col,  F.when(F.col(col) == target_value, F.lit(1)).otherwise(0) )
        
        return self
                    
    def aggregate(self, attribute: str = 'output', source_attr: str = 'data', group_by:Union[str, list] = 'model_iteration'):
        '''Aggregate...'''
        setattr(self, attribute, getattr(self, source_attr))

        # Group data
        setattr( self, attribute, getattr(self, attribute).groupBy(group_by).sum() )

        # Remove group results for key cols
        if isinstance(group_by, str):
            setattr( self, attribute, getattr(self, attribute).drop(f'sum({group_by})') )
        else:
            for col in group_by:
                setattr( self, attribute, getattr(self, attribute).drop(f'sum({col})') )

        # Reset col names to original format
        for col in getattr(self, attribute).columns:
            if col[:3] == 'sum':
                setattr( self, attribute, getattr(self, attribute).withColumnRenamed(col, col.split('(')[1][:-1]) )

        return self

    
if __name__ == '__main__':
    from time import time

    # Import pre-built model
    from weibull import Weibull
    
    # Import pre-build model(holding function in place for now)
    def Group_Data(data, groupCol='model_iteration', keyVal=0, iter_regex='step', convert=True):
        cols = [i for i in data.columns if re.match(iter_regex, i)]

        # Convert
        for col in cols:
            if convert:
                data = data.withColumn(col, F.when(F.col(col) == keyVal, F.col('converter')).otherwise(0))
            else:
                data = data.withColumn(col, F.when(F.col(col) == keyVal, 1).otherwise(0))
        
        # Group
        if isinstance(groupCol, str):
            data = data.select(groupCol, *cols).groupBy(groupCol).sum().orderBy(groupCol)
        elif isinstance(groupCol, list):
            data = data.select(groupCol, *cols).groupBy(groupCol).sum().orderBy(groupCol)
        else:
            raise AttributeError('groupCol attribute must be a string or list of strings')

        # Rename
        for col in data.columns:
            if col[:3] == 'sum':
                data = data.withColumnRenamed(col, col.split('(')[1][:-1])
                
        return data 

    # Initialise Engine
    build = ConfigEngine()

    # Load input data
    build.load('data', 
        filepath='../data/weibull_data_1m.csv',
        header=True,
        inferSchema=True)
    
    # Fit model to engine - run a function or initialise a class
    interval_start = time()

    build.model = Weibull
    build.fit(n_iters=1000)

    # Apply methods specific to model class (if required)
    build.weibull.fit(states=[3,10], values=[0.95, 0.05], model_name='default')
    build.weibull.fit(states=[15,30], values=[0.95, 0.05], model_name='general')

    build.weibull.run(n_steps=50)
    interval_run = time()
    
    # Apply Group_Data model to output of Weibull
    build.model = Group_Data
    build.data = build.weibull.data
    build.fit(convert=False)
    interval_agg = time()

    # Final output
    build.group_data.show()
    interval_show = time()

    # Save sub attribute of model class
    build.save('weibull.data', filepath='../data/weibull_run_raw.csv', index=False)
    build.save('group_data', filepath='../data/weibull_run.csv', index=False)

    print(f'''
    run : {interval_run - interval_start}
    agg : {interval_agg - interval_run}
    show : {interval_show - interval_agg}
    ''')