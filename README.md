# rezAWARE

![Reservation Gateway](./img/rezgate_logo.png?raw=true "RezGateway")

The README file is intended for Rezgateway affiilates working on rezAWARE projects. It povides an introduction to getting started with the available tools and code. Thereafter, refer to the WIKI for complete documentation on the system's components, and interfaces.

## Introduction

The rezAWARE tools and processes will support an organization to mature from descriptive to cognitive analytics. For such data being the key ingrediant, the rezAWARE core components offer:
1. _Wrangler_ - for processing data extract, transform, and load automated pipelines
1. _Mining_ - Arificial Intelligence (AI) and Machine Learning (ML) analytical methods
1. _Visuals_ - interactive dashboards with visual analytics for Business Intelligence (BI)

## Getting Started

### Prerequisits
To get started with a working environment to execute the librabries, the following packages are required.<br>
___Please note that the getting started steps have not been tested and should be revised with missing steps.___

#### Required Packages
* Python >= 3.8
* postgresql >= 13.5 and above
* pyspark >= 2.3.4

#### Run requirements.txt
Run ```python3 -m pip install -r requirements.txt``` to install the remaining dependencies.

## Wrangler
Wrangler has ___five___ key classes and affiliated methods:
1. dags
1. data
1. db
1. logs
1. modules
1. notebooks
1. utils
1. app.cfg

### Database setup
The ```*.sql``` scripts for setting up the database and relevant database components like schemas, views, and functions are located in the folder: ```wrangeler/db```. After installing postgresql database, execute the ```*.sql``` scripts, in the order of any files that have the words ___create_db_schema___. Thereafter, any othe ```*.sql``` files may be executed.

### Airflow Dags
The modul-wise dags are stored in ```wrangler\dags```. These files should be copied to the airflow home dags directory, typically, located at ```~/airflow/dags```

### Notebooks
The notebooks are a templates that can be extended for any use-case. Each notebook is designed to be used withthe wrangler package classes.

### Utils
Contains abstract reusable librabries. However, these libraries, such as apache pyspark requires defining the environment settings for making invoking spark sessions. Use the ```app.cfg``` in the ```wranger/utils``` folder to define those settings. 

### The app.cfg
The ```app.cfg``` contains all the environment and common static information to perform various functions. The ```app.cfg``` in the main wrangler folder sets the main deplyment settings: _AppOwner, Hosting, Security, Database, Airflow, TimeZone, Currency_ settings. These should be ___set after the prerequists are satisfied___. 

## Mining


## Visuals
The sample_data folder contains data that wa tried with the template notebooks

## Common


### Tutorials

### 
