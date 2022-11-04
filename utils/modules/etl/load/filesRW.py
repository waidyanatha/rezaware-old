#!/usr/bin/env python3
# -*- coding: UTF-8 -*-

''' Initialize with default environment variables '''
__name__ = "dataio"
__module__ = "etl"
__package__ = "load"
__app__ = "utils"
__ini_fname__ = "app.ini"
__conf_fname__ = "app.cfg"

''' Load necessary and sufficient python librairies that are used throughout the class'''
try:
    import os
    import sys
    import boto3
    import pandas as pd
    import configparser    
    import logging
    import traceback

    print("All %s-module %s-packages in function-%s imported successfully!"
          % (__module__,__package__,__name__))

except Exception as e:
    print("Some packages in {0} module {1} package for {2} function didn't load\n{3}"\
          .format(__module__,__package__,__name__,e))

'''
    CLASS read and write data to a given location:
        1) local directory
        2) Amazon S3 bucket

    Contributors:
        * nuwan.waidyanatha@rezgateway.com

    Resources:

'''
class FileWorkLoads():
    ''' Function
            name: __init__
            parameters:
                    @name (str)

            procedure: 

            return DataFrame

    '''
    def __init__(self,
                 desc:str="files read and write",
                 **kwargs,
                ):

        self.__name__ = __name__
        self.__package__ = __package__
        self.__module__ = __module__
        self.__app__ = __app__
        self.__ini_fname__ = __ini_fname__
        self.__conf_fname__ = __conf_fname__
        self.__desc__ = desc

        self._mode = None
        self._modes_list = ['local-fs','aws-s3-bucket']
        self._data_root = None   # holds the data root path or bucket name
        _s_fn_id = "__init__"

        ''' initiate to load app.cfg data '''
        global logger
        global pkgConf
        global appConf

        try:
            self.cwd=os.path.dirname(__file__)
            pkgConf = configparser.ConfigParser()
            pkgConf.read(os.path.join(self.cwd,__ini_fname__))

            self.rezHome = pkgConf.get("CWDS","REZAWARE")
            sys.path.insert(1,self.rezHome)
            from rezaware import Logger as logs

            ''' Set the wrangler root directory '''
            self.pckgDir = pkgConf.get("CWDS",self.__package__)
            self.appDir = pkgConf.get("CWDS",self.__app__)
            ''' get the path to the input and output data '''
            self.dataDir = pkgConf.get("CWDS","DATA")

            appConf = configparser.ConfigParser()
            appConf.read(os.path.join(self.appDir, self.__conf_fname__))

            ''' innitialize the logger '''
            logger = logs.get_logger(
                cwd=self.rezHome,
                app=self.__app__, 
                module=self.__module__,
                package=self.__package__,
                ini_file=self.__ini_fname__)
            ''' set a new logger section '''
            logger.info('########################################################')
            logger.info("%s %s",self.__name__,self.__package__)

        except Exception as err:
            logger.error("%s %s \n",_s_fn_id, err)
            print("[Error]"+_s_fn_id, err)
            print(traceback.format_exc())

        return None

    @property
    def mode(self) -> str:
        
        return self._mode

    @mode.setter
    def mode(self, mode:str) -> str:

        _s_fn_id = "function @mode.setter"
        try:
            if not mode.lower() in self._modes_list:
                raise ValueError("Invalid mode = %s. Must be in %s" % (mode,self._modes_list))

            self._mode = mode.lower()

        except Exception as err:
            logger.error("%s %s \n",_s_fn_id, err)
            print("[Error]"+_s_fn_id, err)
            print(traceback.format_exc())

        return self._mode


    @property
    def data_root(self) -> str:
        
        return self._data_root

    @data_root.setter
    def data_root(self, data_root:str) -> str:

        _s_fn_id = "function @data_root.setter"

        try:
            if self.mode == "aws-s3-bucket":
                ''' check if bucket exists '''
                s3_resource = boto3.resource('s3')
                s3_bucket = s3_resource.Bucket(name=data_root)
                count = len([obj for obj in s3_bucket.objects.all()])
#                 for obj in s3_bucket.objects.all():
#                     print(f'-- {obj.key}')
#                 if not _bucket in s3_resource.buckets.all():
                if count <=0:
                    raise ValueError("Invalid S3 Bucket = %s.\nAccessible Buckets are %s"
                                     % (str(_bucket.name),
                                        str([x for x in s3_resource.buckets.all()])))

            elif self.mode == "local-fs":
                ''' check if folder path exists '''
                if not os.path.exists(data_root):
                    raise ValueError("Invalid local folder path = %s does not exists." % (data_root))
            else:
                raise ValueError("Invalid mode = %s. First set mode to one of the %s values" 
                                 % (self.mode, str(self._modes_list)))

            self._data_root = data_root

        except Exception as err:
            logger.error("%s %s \n",_s_fn_id, err)
            print("[Error]"+_s_fn_id, err)
            print(traceback.format_exc())

        return self._data_root


    ''' Function - extract data from aws s3 bucket into a dataframe

            name: s3bucket_to_sdf
            parameters:
                    filesPath (str)
                    @enrich (dict)
            procedure: 
            return DataFrame

            author: <nuwan.waidyanatha@rezgateway.com>
            
    '''
    def s3bucket_to_sdf(self,
                   s3_bucket_name,
                   s3_obj_path,
                   **kwargs):
        pass


    ''' Function - 
            name: pdf_to_csv
            procedure: 

            return DataFrame

    '''
    def extract_data(self,
                     source_path,  # folder path to retriev files
                     source_type,  # read data as CSV, XML, JSON, or SQL
                     dest_path,    # folder path to store files
                     dest_type,    # save data as CSV, XML, JSON, or SQL
                     **kwargs,     # e.g. hold database connection information
                    ):
        pass

    ''' Function - 
            name: pdf_to_csv
            procedure: 

            return DataFrame

    '''
    def pdf_to_df(self,
                  in_pdf_fname,
                  **kwargs):
        pass

    ''' Function - 
            name: xml_to_csv
            procedure: 

            return DataFrame

    '''
    def xml_to_csv(self,
                   in_xml_fname,
                   out_csv_fname,
                   **kwargs):
        pass

    ''' Function
            name: csv_to_sql
            parameters:
                csv_df - dataframe containing the data to import into sql
                sql_scritp - script to use with importing the data into the tables
                kwargs - (i) database connection parameters
                         (ii) force replace table rows

            return error_log, stats_dict (dict) - with the database data ingestion
                            and error log with failed rows

            author: nuwan.waidyanatha@rezgateway.com
    '''
    @staticmethod
    def csv_to_sql(self,
                     csv_df,   # dataframe with the data
                     **kwargs,   # other parameters to consider
                    ):
        return None
