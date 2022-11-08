#!/usr/bin/env python3
# -*- coding: UTF-8 -*-

''' Initialize with default environment variables '''
__name__ = "filesRW"
__module__ = "etl"
__package__ = "load"
__app__ = "utils"
__ini_fname__ = "app.ini"
__conf_fname__ = "app.cfg"

''' Load necessary and sufficient python librairies that are used throughout the class'''
try:
    import os
    import sys
    import configparser    
    import logging
    import traceback
    import boto3
    import pandas as pd
    import json
    import csv
    import functools

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

        self._storeMode = None
        self._storeModeList = [
            'local-fs',     # local hard drive on personal computer
            'aws-s3-bucket' # cloud amazon AWS S3 Bucket storage
        ]
        self._storeRoot = None   # holds the data root path or bucket name
        self._contObj = None
        self._storeData = None
        self._asTypeList = [
            'STR',   # text string ""
            'LIST',  # list of values []
            'DICT',  # dictionary {}
            'ARRAY', # numpy array ()
            'SET',   # set of values ()
            'PANDAS', # pandas dataframe
            'SPARK',  # spark dataframe
        ]   # list of data types to convert content to
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
            logger.info("%s Class",self.__name__)

        except Exception as err:
            logger.error("%s %s \n",_s_fn_id, err)
            print("[Error]"+_s_fn_id, err)
            print(traceback.format_exc())

        return None


    ''' Function - @property mode and @mode.setter

            parameters:
                store_mode - local-fs sets to read and write on your local machine file system
                           aws-s3-bucket sets to read and write with an AWS S3 bucket 
            procedure: checks if it is a valid value and sets the mode
            return (str) self._storeMode

            author: <nuwan.waidyanatha@rezgateway.com>
            
    '''
    @property
    def storeMode(self) -> str:
        
        try:
            if not self._storeMode.lower() in self._storeModeList:
                raise ValueError("Parameter storeMode is not and must be set")

        except Exception as err:
            logger.error("%s %s \n",_s_fn_id, err)
            print("[Error]"+_s_fn_id, err)
            print(traceback.format_exc())

        return self._storeMode

    @storeMode.setter
    def storeMode(self, store_mode:str) -> str:

        _s_fn_id = "function @mode.setter"
        try:
            if not store_mode.lower() in self._storeModeList:
                raise ValueError("Invalid mode = %s. Must be in %s" % (store_mode,self._storeModeList))

            self._storeMode = store_mode.lower()

        except Exception as err:
            logger.error("%s %s \n",_s_fn_id, err)
            print("[Error]"+_s_fn_id, err)
            print(traceback.format_exc())

        return self._storeMode


    ''' Function - @property store_root and @store_root.setter

            parameters:
                store_root - local file system root directory or (e.g. wrangler/data/ota/scraper)
                            S3 bucket name (e.g. rezaware-wrangler-source-code)
            procedure: Check it the directory exists and then set the store_root property
            return (str) self._storeRoot

            author: <nuwan.waidyanatha@rezgateway.com>
            
    '''
    @property
    def storeRoot(self) -> str:
        
        return self._storeRoot

    @storeRoot.setter
    def storeRoot(self, store_root:str) -> str:

        _s_fn_id = "function @store_root.setter"

        try:
            if self.storeMode == "aws-s3-bucket":
                ''' check if bucket exists '''
                logger.debug("%s %s",_s_fn_id,self.storeMode)
                s3_resource = boto3.resource('s3')
                s3_bucket = s3_resource.Bucket(name=store_root)
                count = len([obj for obj in s3_bucket.objects.all()])
                if count <=0:
                    raise ValueError("Invalid S3 Bucket = %s.\nAccessible Buckets are %s"
                                     % (str(_bucket.name),
                                        str([x for x in s3_resource.buckets.all()])))

            elif self.storeMode == "local-fs":
                ''' check if folder path exists '''
                if not os.path.exists(store_root):
                    raise ValueError("Invalid local folder path = %s does not exists." % (store_root))
            else:
                raise ValueError("Invalid mode = %s. First set mode to one of the %s values" 
                                 % (self.storeMode, str(self._storeModeList)))

            self._storeRoot = store_root

        except Exception as err:
            logger.error("%s %s \n",_s_fn_id, err)
            print("[Error]"+_s_fn_id, err)
            print(traceback.format_exc())

        return self._storeRoot


    ''' Function - @property mode and @mode.setter

            parameters:
                store_mode - local-fs sets to read and write on your local machine file system
                           aws-s3-bucket sets to read and write with an AWS S3 bucket 
            procedure: checks if it is a valid value and sets the mode
            return (str) self._storeMode

            author: <nuwan.waidyanatha@rezgateway.com>
            
    '''
    @property
    def storeData(self) -> str:
        
        return self._storeData

    @storeData.setter
    def storeData(self, storeMeta:dict):

        _s_fn_id = "function @storeData.setter"

        _fPath = None   # mandatory - file path from store dict
        _asType = None  # mandatory - data convertion type from store dict
        _fname = None   # either - file name from store dict
        _fType = None   # or - file type from store dict

        try:
            if not "filePath" in storeMeta.keys():
                raise ValueError("Missing filePath to folder. Specify relative path")
            _fPath = storeMeta['filePath']
            if (not "asType" in storeMeta.keys()) and (not storeMeta['asType'] in str(self._asTypeList)):
                raise ValueError("Missing asType to convert data. Specify " % str(self._asTypeList))
            _asType = storeMeta['asType']
            if "fileName" in storeMeta.keys():
                _fname = storeMeta['fileName']
            elif "fileType" in storeMeta.keys():
                _fType = storeMeta['fileType']
            else:
                raise ValueError("Either a fileName of fileType must be specified")
                
            self._storeData = self.get_data(
                as_type=_asType,
                folder_path=_fPath,
                file_name=_fname,
                file_type=_fType,
            )
            logger.debug("In %s from %s",_s_fn_id,self.storeMode)

        except Exception as err:
            logger.error("%s %s \n",_s_fn_id, err)
            print("[Error]"+_s_fn_id, err)
            print(traceback.format_exc())

        return self._storeData


    ''' Function - get_data with wrapper_converter

            parameters:
                as_type (str) - mandatory - define the data type to return
                folder_path(str) - madandatory to give the relative path w.r.t. store_root
                file_name (str) - is mandatory and can be any defined in self._asTypeList
                file_type:str=None    # optional - read all the files of same type

            procedure: When setting self.storeData it calls the get_data() function to extract
                        the data from the source defined in self.storeMode. Based on that value
                        the data is read into a string and the the wrapper will convert it into
                        the data type defined bt as_type

            prerequists: set self.storeMode and self.storeRoot

            return (str) self.storeData

            author(s): <nuwan.waidyanatha@rezgateway.com>
            
            resources:                     
                  * https://www.sqlservercentral.com/articles/reading-a-specific-file-from-an-s3-bucket-using-python
                  * https://realpython.com/python-boto3-aws-s3/
    '''
    def converter(func):

        from itertools import dropwhile

        @functools.wraps(func)
        def wrapper_converter(self,
                 as_type:str,   # mandatory - define the data type to return
                 folder_path:str,      # mandatory - relative path, w.r.t. self.storeRoot
                 file_name:str=None,   # optional - name of the file to read
                 file_type:str=None    # optional - read all the files of same type 
                ):
            
            file_content = func(self,as_type,folder_path,file_name,file_type)

            if as_type.upper() == 'DICT':
                self._storeData = dict(json.loads(file_content))
            elif as_type.upper() == 'TEXT':
                    self._storeData=file_content
            elif as_type.upper() == 'PANDAS':
                if file_name.endswith('json'):
                    _json_data = json.loads(file_content)
                    self._storeData = pd.json_normalize(_json_data)
                elif file_name.endswith('csv'):
                    _all_rows = file_content.splitlines()
                    _rows_list = []
                    for row in _all_rows:
                        _rows_list.append([row])
                    tmp_df = pd.DataFrame(_rows_list)
                    new_header = tmp_df.iloc[0] #grab the first row for the header
                    self._storeData = tmp_df[1:]
                    self._storeData.columns = new_header
                else:
                    print("Something was wrong")
            else:
                self._storeData=file_content

            return self._storeData

        return wrapper_converter


#     @reader
    @converter
    def get_data(
        self,
        as_type:str,   # mandatory - define the data type to return
        folder_path:str,      # mandatory - relative path, w.r.t. self.storeRoot
        file_name:str=None,   # optional - name of the file to read
        file_type:str=None    # optional - read all the files of same type
    ):

        _s_fn_id = "function <get_s3bucket_data>"
        file_content=None

        try:
            logger.debug("Reading files from %s",self.storeMode)

            if self.storeMode == 'aws-s3-bucket':
                ''' read content from s3 bucket '''
                s3_resource = boto3.resource('s3')
                s3_bucket = s3_resource.Bucket(name=self.storeRoot)
                ''' single specific file '''
                if file_name:
                    file_path = str(os.path.join(folder_path,file_name))
                    content_object = s3_resource.Object(
                        self.storeRoot,   # bucket
                        file_path)   # key
                    file_content = content_object.get()['Body'].read().decode('utf-8')

                elif file_type:
                    ''' multiple files of same file type '''
                    file_path = str(os.path.join(folder_path))
                    bucket_list = []
                    for file in s3_bucket.objects.filter(Prefix = '2019/7/8'):
                        file_name=file.key
                        if file_name.find(".csv")!=-1:
                            bucket_list.append(file.key)
                            length_bucket_list=print(len(bucket_list))
                            file_content = content_object.get()['Body'].read().decode('utf-8')
                else:
                    raise ValueError("Something was wrong")

            elif self.storeMode == 'local-fs':
                ''' read content from local file system '''
                if file_name:
                    file_path = str(os.path.join(self.storeRoot,folder_path,file_name))
                    with open(file_path, 'r') as f:
                        file_content = f.read()
                else:
                    raise ValueError("something was wrong")

            else:
                raise typeError("Invalid storage mode %s" % self.storeMode)

        except Exception as err:
            logger.error("%s %s \n",_s_fn_id, err)
            print("[Error]"+_s_fn_id, err)
            print(traceback.format_exc())

        return file_content

