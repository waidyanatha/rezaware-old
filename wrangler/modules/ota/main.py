#!/usr/bin/env python3
# -*- coding: UTF-8 -*-

class otaETL():

    '''
        CLASS ETL class will contain the functions required to run the various
        
        author(s): <nuwan.waidyanatha@rezgateway.com>
    '''


    ''' Load necessary and sufficient python librairies that are used throughout the class'''
    try:
        ''' standard python packages '''
        import os
        import sys
        import logging
        import traceback
        import configparser
        import pandas as pd
        from datetime import datetime, date, timedelta

        ''' inhouse packages '''
        ROOT_DIR = "~/workspace/rezgate/wrangler"
        MODULE_PATH = os.path.join(ROOT_DIR, 'modules/ota/')
        sys.path.insert(1,MODULE_PATH)

        import otaWebScraper as otaws

        ''' read the conicuration data'''
        CONFIG_PATH = os.path.join(MODULE_PATH, 'app.cfg')
        CONFIG_PATH = "app.cfg"
        config = configparser.ConfigParser()
        config.read(CONFIG_PATH)
        ''' get the path to the input and output data '''
        DATA_PATH = os.path.join(ROOT_DIR,config.get('STORES','DATA'))
        print("Data files path: ", DATA_PATH)
        print("All otaETL software packages loaded successfully!")
        
        config = configparser.ConfigParser()
        config.read('app.cfg')
        print(config['hosts'])

    except Exception as e:
        print("Some software packages in otaETL didn't load\n{}".format(e))


    def __init__(module,name, **kwards):

        
        
        module.name = "ota"
        if name:
            module.name = name
        print(module.name)
        
        return None

    class extract():
        def __init__(self,**kwards):
            return None
        
    class transform():
        def __init__(self,**kwards):
            return None

    class transform():
        def __init__(self,**kwards):
            return None
