# Test Reactor Engine

import sys, numpy as np
from time import time

np.random.seed(0)
sys.path.append('../')

from src.engine import Engine
from src.models.weibull import Weibull

start = time()

# Initialise Engine
build = Engine(model = Weibull)

# Load input data
build.load(
    attribute='data', 
    filepath='data/weibull_data_1m.csv',
    header=True,
    inferSchema=True)

# Fit model to engine - run a function or initialise a class
build.fit(n_iters=1000)
build.weibull.fit(states=[1,3], values=[0.95, 0.05], model_name='default')
build.weibull.fit(states=[1,3], values=[0.95, 0.05], model_name='general')

# Run model
build.weibull.run(n_steps=10)

# Constrain
build.constrain(attribute='data_constrained', source_attr = 'weibull.data', constraint=2e6)

# Convert
build.convert(attribute='data_converted', source_attr='data_constrained', convert_events=True)

# Aggregate
build.aggregate(attribute='data_aggregate', source_attr='data_converted')

# Count values
build.count_values(attribute='val_counts', source_attr='data_constrained', partition_by='model_iteration')

# Save sub attribute of model class
# build.save('weibull.data', filepath='../../tests/data/weibull_run_raw.csv', index=False)

print(f'run : {round(time() - start, 0)}')