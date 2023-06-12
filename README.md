# rezAWARE

![Reservation Gateway](./img/rezgate_logo.png?raw=true "RezGateway")

The README file is intended for Rezgateway affiilates working on rezAWARE projects. It povides an introduction to getting started with the available tools and code. Thereafter, refer to the WIKI for complete documentation on the system's components, and interfaces.

__Wiki__ 

## Introduction

The rezAWARE tools and processes will support an organization to mature from descriptive to cognitive analytics. For such data being the key ingrediant, the rezAWARE core components offer:
1. _Mining_ - Arificial Intelligence (AI) and Machine Learning (ML) analytical methods
1. _Utils_ - set of common utility packages that can be used with any of the below apps
1. _Wrangler_ - for processing data extract, transform, and load automated pipelines
1. _Visuals_ - interactive dashboards with visual analytics for Business Intelligence (BI)

## Quickstart
__Linux Only__
* Simply download and run the setup file
   * ```python3 setup.py```

#### Setup will guide you with
* creating a new user and home directory
* starting a new conda environment
* cloning rezaware pltform
* installing all dependencies
* starting all databases and storage tools
* configuring for your environment

#### Test the new setup
Run __pytest__ by executing the command in your terminal prompt
* ```pytest```


## Developers

### Prerequisits

#### Clone rezaware
* [Install Git on you machine](https://git-scm.com/book/en/v2/Getting-Started-Installing-Git)
* Create a fork of the rezaware platform
   * ```http://github/myrepo/rezaware```
* Make a working director on you machine; e.g., in linux prompt:
   * ```mkdir ~/workspace/```
* Navigate to that director; e.g., in linux prompt: 
   * ```cd ~/workspace/```
* Initialize your git in this directory 
   * ```git init```
* Clone the rezaware platform in to you local machine,
   * ```git clone https://github.com/myrepo/rezaware.git
* Change your directory to the rezaware folder; e.g., 
   * ```cd ~/workspace/rezaware```

#### Conda environment
* It is recommended to setup a clean [Anaconda3](https://www.anaconda.com/) environment with Python 3.8.10 to avoid any distutils issues. 
* After you have installed __conda__; create a new environment using the _requirements.txt_ file:
   * ```conda create --name rezenv --file requirements.txt```
* Thereafter, check if all packages, listed in requirements.txt was installed
   * ```conda list``` will print a list to stdout like this

    ```
    
    # packages in environment at /home/nuwan/anaconda3/envs/reza:
    #
    # Name                    Version                   Build  Channel
    _libgcc_mutex             0.1                        main  
    _openmp_mutex             5.1                       1_gnu  
    absl-py                   1.2.0                    pypi_0    pypi
    alembic                   1.8.1                    pypi_0    pypi
    amqp                      5.1.1                    pypi_0    pypi
    aniso8601                 9.0.1                    pypi_0    pypi
    anyio                     3.6.1                    pypi_0    pypi
    apache-airflow            2.3.4                    pypi_0    pypi
    apache-airflow-providers-celery 3.0.0              pypi_0    pypi
    ...
    
    ```

___Please note that the getting started steps have not been tested and should be revised with missing steps.___

