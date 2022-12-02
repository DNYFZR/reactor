# Configuration Engine - Main Class

import sys, re, numpy as np, pyspark
from typing import Union
import collections

from pyspark.sql import functions as F
from pyspark.sql.window import Window

from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession

class Engine:
    '''
    The Configuration Engine...
    '''
    def __init__(self, **kwargs):
        '''Use key word args to set optional initial class attrs'''
        global spark
        spark = SparkSession(SparkContext.getOrCreate())
        
        for key, val in kwargs.items():
            setattr(self, key, val)

    @staticmethod
    def extract_docstrings(ClassObject: object, filepath: str):
        '''Generate a markdown file containing ClassObject doc strings, saved to filepath, returns None'''
        methods = [i for i in dir(ClassObject) if i[:2] != '__']
        doc_lib = {ClassObject.__name__ : ClassObject.__doc__}

        for method in methods:
            doc_lib.update({method: getattr(ClassObject, method).__doc__})
        
        with open(f'''{filepath}''', 'w') as file:
            for title, docstring in doc_lib.items():
                if title == ClassObject.__name__:
                    file.write(f'''<h1 align="center"><b> {title} </b></h1>''')

                else:
                    file.write(f'''<h2 align="center"><b> {title} </b></h2>''')
        
                file.write('\r\n')
                if docstring != None:
                    file.write(docstring.replace('\n        ', '\n    '))
                else:
                    file.write('No docstring available...')
                file.write('---')
                file.write('\r\n\n')

        return None

    @staticmethod    
    def rdd_count(arr: pyspark.rdd.RDD) -> dict:
        '''Generate a value count dictionary from a pyspark RDD, or data frame row / column'''
        return collections.Counter(arr.flatMap(lambda x: x).collect())

    def _getsubattr(self, attribute: str):
        '''Internal method for accessing sub attributes of objects'''
        attribute = attribute.split('.')
        
        for n, sub in enumerate(attribute):
            if n == 0: # top level attr
                item = getattr(self, sub)
            else: # successive sub-attr level
                item = getattr(item, sub)
        
        return item
    
    def _setsubattr(self, subclass: str, attribute: str, value):
        ''' Set an attribute of a subclass of Engine '''
        setattr(getattr(self, subclass), attribute, value)

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
        
        if len(kwargs) == 0:
            raise ValueError('Cannot initialise an empty Spark DataFrame')
        
        # Read CSV
        elif 'filepath' in kwargs.keys():
            path = kwargs['filepath']
            del kwargs['filepath']
            
            setattr(self, attribute , spark.read.csv(path, **kwargs))

        # Load Arrays
        else:
            cols = list(kwargs.keys())
            data = list(i.tolist() if isinstance(i, np.ndarray) else i for i in kwargs.values())

            if not np.all([len(i) == len(data[0]) for i in data]):
                raise IndexError('All columns must be the same length')

            arr_len = len(data[0])
            data = [tuple(arr[n] for arr in data) for n in range(arr_len)]

            setattr( self, attribute, spark.sparkContext.parallelize(data).toDF(cols) )

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
        # Access attribute 
        if '.' in attribute:
            item = self._getsubattr(attribute)
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
    
    def constrain(self, constraint: Union[int, dict] = 1, attribute: str = 'data_constrained', source_attr: str = 'data', sum_by: str = 'converter', 
                    partition_by: str = 'model_iteration', iter_regex: str = 'step', select_random: bool = False ):

         # Access attribute
        if '.' in source_attr:
            setattr(self, attribute, self._getsubattr(source_attr))
        else:
            setattr(self, attribute, getattr(self, source_attr))
        
        # Input testing
        if isinstance(constraint, dict):
            if not np.all([True if col in constraint.keys() else False for col in cols]):
                raise KeyError('Constraint dict must contain an entry for each column')

        elif not isinstance(constraint, (int, float)):
            raise ValueError('Constraint must be of type int, float or dict')

        # Constrain
        cols = getattr(self, attribute).columns

        for n, col in enumerate(cols): 
            prev_col = cols[n-1]

            if isinstance(constraint, dict):
                limit = constraint[col]
            else: 
                limit = constraint

            if re.match(iter_regex, col) and int(col.split('_')[1]) != 0:
                # Add cumulative sum column
                setattr(self, attribute, getattr(self, attribute).withColumn( 'applied',  F.when(F.col(col) == 0, F.col(sum_by)).otherwise(0) ))

                # Apply ordering rule
                if select_random: 
                    setattr(self, attribute, getattr(self, attribute).withColumn('order_col', F.rand(seed=np.random.randint(1, 1000)) ))
                    setattr(self, attribute, getattr(self, attribute).withColumn( 'total', F.sum('applied').over( Window.partitionBy(partition_by).orderBy(F.col('order_col').desc()).rowsBetween(-sys.maxsize, 0) )))
                    setattr(self, attribute, getattr(self, attribute).drop('order_col'))

                else:
                    setattr(self, attribute, getattr(self, attribute).withColumn( 'total', F.sum('applied').over( Window.partitionBy(partition_by).orderBy(F.desc(prev_col)).rowsBetween(-sys.maxsize, 0)) ))

                # Apply constraint in reverse order
                remaining_cols = cols[n:][::-1] # cols from last_col : col
                adjust_cols = remaining_cols[:-1] # cols from last_col : next_col

                # Update next vals from end of cols to curr step
                for x, a_col in enumerate(adjust_cols):
                    setattr(self, attribute, getattr(self, attribute).withColumn(a_col, F.when( (F.col('total') > limit) & (F.col(col) == 0), F.col(remaining_cols[x+1]) ).otherwise( F.col(a_col) ) ))

                # Update current val
                setattr(self, attribute, getattr(self, attribute).withColumn(col, F.when( (F.col('total') > limit) & (F.col(col) == 0), F.col(prev_col) + 1 ).otherwise( F.col(col) ) ))

                setattr(self, attribute, getattr(self, attribute).drop('applied', 'total'))
            
        return self
    
    def convert(self, attribute: str = 'output', source_attr: str = 'data', target_value: Union[int, float] = 0, iter_regex: str = 'step', converter: str = 'converter', convert_events: bool = False):
        '''
        Convert a target value to a converter or binary value
            - need to allow for list of target vals 
                - separate attrs to be set for 
                - or - create col for each 1_step_0 / 2_step_0 / 3_step_0... 
        '''
        # Access attribute 
        if '.' in source_attr:
            setattr(self, attribute, self._getsubattr(source_attr))        
        else:
            setattr(self, attribute, getattr(self, source_attr))  

        # Convert
        for col in list(getattr(self, attribute).columns):   
            if re.match(iter_regex, col):

                # Modify to count / converter value
                if convert_events:
                    setattr(self, attribute, getattr(self, attribute).withColumn( col,  F.when(F.col(col) == target_value, F.col(converter)).otherwise(0) ))
                else:
                    setattr(self, attribute, getattr(self, attribute).withColumn( col,  F.when(F.col(col) == target_value, F.lit(1)).otherwise(0) ))
        
        return self
                    
    def aggregate(self, attribute: str = 'output', source_attr: str = 'data', agg_method: str = 'sum', group_by:Union[str, list] = 'model_iteration'):
        '''Aggregate...'''
        # Access attribute 
        if '.' in source_attr:
            setattr(self, attribute, self._getsubattr(source_attr))        
        else:
            setattr(self, attribute, getattr(self, source_attr))  

        # Group data
        setattr( self, attribute, getattr(getattr(self, attribute).groupBy(group_by), agg_method)() )

        # Remove group results for key cols
        if isinstance(group_by, str):
            setattr( self, attribute, getattr(self, attribute).drop(f'{agg_method}({group_by})') )
        else:
            for col in group_by:
                setattr( self, attribute, getattr(self, attribute).drop(f'{agg_method}({col})') )

        # Reset col names to original format
        for col in getattr(self, attribute).columns:
            if re.match(agg_method, col):
                setattr( self, attribute, getattr(self, attribute).withColumnRenamed(col, col.split('(')[1][:-1]) )

        return self
    
    def count_values(self, attribute: str, source_attr:str = 'data', col_regex: str = 'step', start: int = 0, 
                    partition_by: Union[str, None] = None, interval: Union[int, None] = None ):
        '''
        Count values within spark dataframes, returns a tall dataframe with individual & cumulative distributions for columns matching iter_regex.
        
        Optional 
            - Filtering using start & interval : select a starting index in columns matching iter_regex, and to select columns at an interval from there.
            - Counting over partitions with partition_by : use a column to split the table into separate partitions for value counting
        
        Parameters
        ---
        df : pyspark DataFrame 
            object to be value counted
        partition_by : str or None (optional)
            name of column to partition the dataset, if None a single partition is used
        iter_regex : str
            regex patter to identify the column(s) to be value counted
        start : int
            starting list index poistion for list of columns matching iter_regex, default = 0
        interval : int or None (optional)
            reduces cols matching iter_regex to equal intervals from start
            
        Returns
        ---
        pyspark DataFrame

        '''
         # Access attribute 
        if '.' in source_attr:
            setattr(self, attribute, self._getsubattr(source_attr))        
        else:
            setattr(self, attribute, getattr(self, source_attr))  
        
        df = getattr(self, attribute)

        # Select iter cols
        iter_cols = [i for i in df.columns if re.match(col_regex, i)]
        iter_cols = [(n, i) for n, i in enumerate(iter_cols)]
        
        if interval is None: 
            iter_cols = [i[1] for i in iter_cols if i[0] == start]
        else:
            iter_cols = [i[1] for i in iter_cols if i[0] == start or (i[0] % interval == 0 and i[0] >= start)]
        
        # Add partition col if requried
        if partition_by is None:
            df = df.withColumn('partition_by', F.lit(1))
            partition_by = 'partition_by'
                
        for col in iter_cols:
            # Create window
            w =  Window.partitionBy(partition_by).orderBy(col)
            
            # Add cumulative dist & select distinct
            temp = df.withColumn(f'{col}_%', F.cume_dist().over(w)).select(partition_by, col, f'{col}_%').distinct()

            # Extract contributing % and step number
            temp = temp.withColumn('value_%', F.when(F.col(col) > 0, F.col(f'{col}_%') - F.lag(f'{col}_%', 1).over(w) ).otherwise( F.col(f'{col}_%') ))
            temp = temp.withColumn(col_regex, F.lit( int(col.split('_')[1]) ))
            
            # Align col naming & ordering for stacking
            temp = temp.withColumnRenamed(col, 'value').withColumnRenamed(f'{col}_%', 'total_%').select(partition_by, col_regex, 'value', 'total_%', 'value_%', )

            # Stack frames
            if col == iter_cols[0]:
                df_out = temp
            else:
                df_out = df_out.union(temp)
                
        if 'partition_by' in df.columns:
            df_out = df_out.drop('partition_by')
        
        setattr(self, attribute, df_out.rdd.toDF(df_out.columns))
        return self
