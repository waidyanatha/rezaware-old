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
    import json
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


    ''' Function - @property mode and @mode.setter

            parameters:
                mode - local-fs sets to read and write on your local machine file system
                       aws-s3-bucket sets to read and write with an AWS S3 bucket 
            procedure: checks if it is a valid value and sets the mode
            return (str) self._mode

            author: <nuwan.waidyanatha@rezgateway.com>
            
    '''
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


    ''' Function - @property data_root and @data_root.setter

            parameters:
                data_root - local file system root directory or (e.g. wrangler/data/ota/scraper)
                            S3 bucket name (e.g. rezaware-wrangler-source-code)
            procedure: Check it the directory exists and then set the data_root property
            return (str) self._data_root

            author: <nuwan.waidyanatha@rezgateway.com>
            
    '''
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


    ''' Function - read content from a folder

            parameters:
                key - key in the s3 bucket
            procedure: Check if the key exists and then read the content
            return (boto3.object.content) key_cont

            author: <nuwan.waidyanatha@rezgateway.com>
            
    '''
    def read_from_awss3_key(self,key_name:str, file_name:str=None, file_types:list=[]):

        _s_fn_id = "read_from_s3_obj"
        try:
            s3_resource = boto3.resource('s3')
            s3_bucket = s3_resource.Bucket(name=self.data_root)
            key_cont="read_from_awss3_obj"

        except Exception as err:
            logger.error("%s %s \n",_s_fn_id, err)
            print("[Error]"+_s_fn_id, err)
            print(traceback.format_exc())

        return key_cont

    def read_from_local_folder(self,folder_path:str, file_name:str=None, file_types:list=[]):

        _s_fn_id = "read_from_local_folder"

        try:
            folder_content="read_from_local_folder"

        except Exception as err:
            logger.error("%s %s \n",_s_fn_id, err)
            print("[Error]"+_s_fn_id, err)
            print(traceback.format_exc())

        return folder_content
    
    def read_files_into_sdf(self,folder_path:str, file_name:str=None, file_types:list=[]):

        _s_fn_id = "read_folder_into_sdf"

        try:
            if self.mode == "aws-s3-bucket":
                ''' read content from s3 bucket '''
                logger.debug("Reading files from %s",self.mode)
                return self.read_from_awss3_key(folder_path, file_name, file_types)
            elif self.mode == "local-fs":
                ''' read content from local file system '''
                logger.debug("Reading files from %s",self.mode)
                return self.read_from_local_folder(folder_path, file_name, file_types)
            else:
                raise ValueError("Invalid data_mode. Set to one of the following %s"
                                 % str(self._modes_list))

        except Exception as err:
            logger.error("%s %s \n",_s_fn_id, err)
            print("[Error]"+_s_fn_id, err)
            print(traceback.format_exc())
            return None

    ''' Function - read_file_into_json

            parameters:
                folder_path - madandatory to give the relative path w.r.t. data_root
                file_name - is mandatory and must be a json
            procedure: Check it the directory exists and then set the data_root property
            return (str) self._data_root

            author: <nuwan.waidyanatha@rezgateway.com>
            
    '''
    def read_json_into_dict(self,folder_path:str, file_name:str, file_types:list=[]) -> dict:

        _s_fn_id = "read_files_into_json"
        json_content = {}

        try:

            if self.mode == "aws-s3-bucket":
                ''' read content from s3 bucket '''
                logger.debug("Reading files from %s",self.mode)
                s3_resource = boto3.resource('s3')
                s3_bucket = s3_resource.Bucket(name=self.data_root)
                content_object = s3_resource.Object(
                    self.data_root,   # bucket
                    str(os.path.join(folder_path,file_name)))   # key
                file_content = content_object.get()['Body'].read().decode('utf-8')
                json_content = json.loads(file_content)
#                 return self.read_from_awss3_key(folder_path,file_name, file_types)

            elif self.mode == "local-fs":
                ''' read content from local file system '''
                logger.debug("Reading files from %s",self.mode)
#                 return self.read_from_local_folder(folder_path, file_name, file_types)
            else:
                raise ValueError("Invalid data_mode. Set to one of the following %s"
                                 % str(self._modes_list))

        except Exception as err:
            logger.error("%s %s \n",_s_fn_id, err)
            print("[Error]"+_s_fn_id, err)
            print(traceback.format_exc())
  
        return json_content
