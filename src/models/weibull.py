# Weibull Simulation Module

import math, numpy as np, random as rd
from sklearn.linear_model import LinearRegression
from pyspark.sql import Row, DataFrame, functions as F

from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession

spark = SparkSession(SparkContext.getOrCreate())

class Weibull:
    '''Cofigure, fit & execute a Weibull Simulation '''
    def __init__(self, data: DataFrame, n_iters: int = 1):
        self.n_iters = n_iters
        self.data = data
        self.params = None
        
    @staticmethod
    def survival_function(x, shape, scale):
        """
        Weibull survival function.  The survival function is the probability
        that the variate takes a value greater than x.
        """  
        return np.exp(-(x / scale) ** shape)        

    @staticmethod
    def apply_survival_function(shape, scale, max_value=200, cond_prob=True):
        """
        Applies the Weibull survival function to dataset.  Default output provides the conditional
        survival probability - given survived to time x, probability of surviving to time x+1. If
        shape and scale argument are not provided, defaults to class shape/scale attributes.
        """     

        # calculate survival probability over value range
        probs = np.arange(max_value + 1)
        probs = Weibull.survival_function(probs, shape=shape, scale=scale)

        # convert probability to conditional, given survival to previous value
        if cond_prob == True:
            probs = np.concatenate(([probs[1]], probs[2:] / (probs[1:-1]+0.0000001)))

        return probs
    
    @staticmethod
    def weibull_mean(shape, scale):
        """
        Given shape and scale parameters, calculates the mean of the weibull distribution.
        """
        return scale * math.gamma(1 + 1 / shape)

    @staticmethod
    def fit_weibull_params(ages, values):
        """
        Performs linear regression on survival ages, values to estimate shape and scale parameters.
        Output is returned a tuple.
        """ 
        # transform the survival parameters and fit a linear model
        transformed_ages = np.log(np.array(ages)).reshape((-1,1))
        transformed_values = np.log(np.log(1/np.array(values)))
        model = LinearRegression().fit(transformed_ages, transformed_values)

        # extract shape and scale parameters
        shape = float(model.coef_)
        scale = np.exp(round(model.intercept_) / -shape)

        return (shape, scale)

    @staticmethod
    def adjust_scale_param(shape, mean_age):
        """
        Adjust shape parameter using mean age value.
        """ 
        return (shape, mean_age / math.gamma(1 + 1 / shape))

    def _adjust_model(self, base_model, adjust_value, model_name):
        """
        This is currently a method that is used to support the asset replacement
        model that adjusts the original weibull fit but adjusting the scale based
        on a revised mean age.  This has been generalised to support adjustments
        for other models, but will not have any impact on non-weibull methods. 
        Parameters :
            - base_model (str) : name of the base model that will be used to create modified model
        adjust_value (int) : the value to be used to adjust the model
        model_name (str) : name of the new model
        Returns :
            - saved to model_params attribute - dictionary entry with key=model_name, value=model added
        """        
        shape = self.params[base_model][0]
        self.params[model_name] = self.adjust_scale_param(shape=shape, mean_age=adjust_value)
        
        return self

    def _configure_model(self, n_iters, uncertainty=0.1):
        """
        Configures model_params for use in the simulator.  This is completed individually for each
        model added to model_params.  The n_iters argument specifies the number of versions of each
        model to generate, each being modified by the uncertainty argument. 
        
        Subsequent calls to this method will overwrite previous calls. 
        
        Parameters :
            - n_iters (int) : number of model iterations, if set to
        adjust (float) : range to +/- for varying shape / scale parameters for each iteration
        Returns :
            - saved to probs attribute - pyspark.sql.dataframe.DataFrame 
        """
        setattr(self, 'probs', None)        

        # Configure parameters
        for key, value in self.params.items():            
            for j in range(1, n_iters+1):
                    
                if j==1:
                    shape_iter = value[0]
                    scale_iter = value[1]
                    probs = [{
                        "model_iteration": j, 
                        "model": key, 
                        "prob": [float(x) for x in self.apply_survival_function(shape=shape_iter, scale=scale_iter)], 
                        }]      
                else:
                    shape_iter = value[0] + rd.uniform(-uncertainty, uncertainty) * value[0]
                    scale_iter = value[1] + rd.uniform(-uncertainty, uncertainty) * value[1] 
                    probs.append({
                        "model_iteration": j, 
                        "model": key, 
                        "prob": [float(x) for x in self.apply_survival_function(shape=shape_iter, scale=scale_iter)]
                        })
            
            # Configure DataFrame
            if list(self.params)[0] == key:
                self.probs = spark.createDataFrame([Row(**i) for i in probs])
            
            else:
                self.probs = self.probs.union(spark.createDataFrame([Row(**i) for i in probs]))
                
        return self
    
    def fit(self, states, values=None, model_name = "default"):
        """
        Fits/loads a model that will be used for simulation.  The input values will
        vary depending on the method specified as part of the constructor. This is
        currently configured for two methods:
            
        The initial method call will create and assign a dictionary to the model attribute.
        Subsequent method calls will add or modify existing elements. There is no method
        to remove a model at this time. 
            
        Parameters :
            - states : array_like - data containing survival ages
            - values : array_like - data containing survival probability at given state
            - model name : string/int (optional) - name to be assigned to the model
        Returns : 
            - aved to model_params attribute - dictionary entry with key=model_name, value=model added 
        """
        
        model = self.fit_weibull_params(ages=states, values=values)
                           
        if self.params is None:    
            self.params = {model_name : model}
        else:
            self.params[model_name] = model
        
        return self
    
    def run(self, n_steps=1, uncertainty_factor=0.1):
        """
        Configures the data attribute dataframe for the number of iterations with varying model parameters.
        Run the simulation completing a discrete test for each timestep.  The discrete
        event test is coded in this method.  The correct test is performed based on the 
        method specified during initialisation.
        Parameters
        ----------
        n_timesteps : int (optional)
            Number of timesteps to complete
            
        uncertainty_factor: float (optional)
            Range to +/- for varying iteration
        Returns
        -------
        None
            results saved to data attribute 
        Raises
        ------
        None
        """
        
         # configure survival probabilities for model iterations
        self._configure_model(n_iters=self.n_iters, uncertainty=uncertainty_factor)
        
        # join survival probabilities to data attribute 
        self.data = self.data.join(self.probs, ["model_iteration", "model"], "left_outer")
        
        # set the original value in a new column 
        self.data = self.data.withColumn("step_0", F.col("state"))
        
        # simulate over each timestep
        for i in range(1, n_steps+1):

            col_name = "step"+"_"+str(i)
            
            self.data = self.data.withColumn(col_name, F.rand())
            
            self.data = self.data.withColumn(col_name, F.when(
                F.col(col_name) > self.data["prob"][F.col("state")], 0
                ).otherwise(F.col("state") + 1))

            self.data = self.data.withColumn("state", F.col(col_name))

        # remove redundant columns & materialise change
        self.data = self.data.drop("state", "prob")
        self.data = self.data.rdd.toDF(self.data.columns)

        return self
    

if __name__ == '__main__':
    from time import time
    data = spark.read.csv('../../../tests/data/weibull_data_1m.csv', header=True, inferSchema=True)

    start = time()
    sim = Weibull(data=data, n_iters=1000, 
            ).fit(states=[5,15], values=[0.95, 0.05], model_name='default'
            ).fit(states=[50,95], values=[0.95, 0.05], model_name='general'
            )
    
    interval = time()
    out = sim.run(n_steps=10)

    end = time()
    print(f'''
        fit : {interval - start}
        run : {end - interval}    
        ''')
