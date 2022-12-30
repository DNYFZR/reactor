<h1 align="center"><b> ⚡ SparkReactor </b></h1><br>

The Spark Reactor packages together general configuration and specific modelling capability to...

<br>

---
<h2 align="center"><b> 🎯 Design </b></h2><br>

The package is structured with a general orchestration engine, managing loading / saving, model fitting, aggreation functionality and other general tooling for managing model configurations.

The modelling core of the package is structured for flexibility, allowing developers to add / remove / update any individual item as required. 

Development of models is structured using functional & object oriented programming. 

Through utilising the object oriented approach, we can use the engine class to manage, create, move, or remove attributes of any model at run time. Allowing taylored use of the collective functionality within the package for any given use case.

One of the main reasons for using the functional / OOP approach, is the benefit of docstrings. Not only can these be used to better guide developers, but they are able to be programatically extracted and published with the package as a reference for users.

<br>

---
<h3 align="center"><b> 🔋 Engine </b></h3><br>

The [engine](../src/SparkReactor/engine.py) class contains core, common functionality to manage data and model flows. 

|**Core Method**|**Info**
|--|--
| extract_doctrings | Extracts from class object & methods, and publishes a formatted markdown document to a given filepath
| load | Fast load method, handles csv & column-name : array key-value pair
| save | Save an active & suitable attribute to a filepath
| fit | Core method which fits a model attribute to a data attribute
| constrain | Common, flexibly configured constraining functionality
| aggregate | Common, flexibly configured aggregation 
| count_values | Value counting on large spark DataFrames with optional partitioning  
| rdd_count | Create a value count dictionary from a spark RDD or DataFrame Row / Column


---
<h3 align="center"><b> 🧩 Models </b></h3><br>

|**Model**|**Info**
|--|--
|[Weibull Simulator]('../src/../../../src/SparkReactor/models/weibull.py') | Weibull modelling fit, run and general functions.
|  |  
|  |  


<br>

---
<h2 align="center"><b> 🧱 Build </b></h2><br>

See DevSetUp doc... to be added...

<br>

---
<h2 align="center"><b> 🧪 Testing </b></h2><br>

Testing is broken down along the same lines as design, the engine is tested in isolation to ensure it delivers on its core functionality, and the models are all tested as part of their individual development process. 

Testing is carried out via the Continuous Integration pipeline, and PR's must pass CI before they can be merged into the main production branch.

<br>

---
<h2 align="center"><b> 🎬 Packaging </b></h2><br>

Packaging is managed via the Continuous Delivery pipeline, triggered on a version upgrade on the main branch.

<br>

---
---
