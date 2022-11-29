#!/usr/bin/env python3
# -*- coding: UTF-8 -*-

''' Initialize with default environment variables '''
__name__ = "mongodbwls"
__module__ = "etl"
__package__ = "load"
__app__ = "utils"
__ini_fname__ = "app.ini"
__conf_fname__ = "app.cfg"

''' Load necessary and sufficient python librairies that are used throughout the class'''
try:
    import os
    import sys
    import functools
    import configparser    
    import logging
    import traceback
    from pymongo import MongoClient
    import pandas as pd
    from bson.objectid import ObjectId 

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
class NoSQLWorkLoads():
    ''' Function
            name: __init__
            parameters:
                    @name (str)

            procedure: 

            return DataFrame

    '''
    def __init__(self,
                 desc:str="noSQL DB collection CRUD",
                 **kwargs,
                ):

        self.__name__ = __name__
        self.__package__ = __package__
        self.__module__ = __module__
        self.__app__ = __app__
        self.__ini_fname__ = __ini_fname__
        self.__conf_fname__ = __conf_fname__
        self.__desc__ = desc

#         ''' --- database properties --- '''
#         self._hostIP = None
#         self._dbUser = None
#         self._dbPswd = None
#         self._dbAuthSource = None
#         self._dbAuthMechanism = None
#         self._dbtls = None
#         self._dbtlsKeyFile = None
#         self._dbtlsCAFile = None
        
        self._dbType = None
        self._dbTypesList = ['mongodb','firebase']
        self._dbName = None
        self._connect = None
        self._documents = None

        self._asTypeList = [
            'STR',   # text string ""
            'LIST',  # list of values []
            'DICT',  # dictionary {}
            'ARRAY', # numpy array ()
            'SET',   # set of values ()
            'PANDAS', # pandas dataframe
            'SPARK',  # spark dataframe
        ]   # list of data types to convert content to
#         self._docTypeList = [
#             'CSV',   # comma separated value
#             'JSON',  # Javascript object notation
#             'TXT',   # text file
#         ]
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

            ''' initialize the logger '''
            logger = logs.get_logger(
                cwd=self.rezHome,
                app=self.__app__, 
                module=self.__module__,
                package=self.__package__,
                ini_file=self.__ini_fname__)
            ''' set a new logger section '''
            logger.info('########################################################')
            logger.info("%s Class",self.__name__)
            logger.debug("%s initialization for %s module package %s %s done.\nStart workloads: %s."
                         %(self.__app__,
                           self.__module__,
                           self.__package__,
                           self.__name__,
                           self.__desc__))
            print("%s Class initialization complete" % self.__name__)

        except Exception as err:
            logger.error("%s %s \n",_s_fn_id, err)
            print("[Error]"+_s_fn_id, err)
            print(traceback.format_exc())

        return None

#     ''' Functions - list of fget and fset @property and @*.setter functions
#             hostIP, dbType, dbPort, dbDriver, dbName, 
#             dbUser, dbPswd, dbAuthSource, dbAuthMechanism

#             parameters:
#                 store_mode - local-fs sets to read and write on your local machine file system
#                            aws-s3-bucket sets to read and write with an AWS S3 bucket 
#             procedure: checks if it is a valid value and sets the mode
#             return (str) self._documents

#             author: <nuwan.waidyanatha@rezgateway.com>
            
#     '''
#     @property
#     def hostIP(self) -> str:
#         return self._hostIP

#     @hostIP.setter
#     def hostIP(self,host_ip:str = "127.0.0.1"):

#         _s_fn_id = "function @hostIP.setter"
#         try:
#             if not (host_ip is None and host_ip==""):
#                 self._hostIP = host_ip
                
#             elif appConf.get('HOSTS','HOSTIP'):
#                 self._hostIP = appConf.get('HOSTS','HOSTIP')
#             else:
#                 raise ConnectionError("Undefined hostip; set in app.cfg or as class property")

#         except Exception as err:
#             logger.error("%s %s \n",_s_fn_id, err)
#             print("[Error]"+_s_fn_id, err)
#             print(traceback.format_exc())

#         return self._hostIP

    @property
    def dbType(self) -> str:
        return self._dbType

    @dbType.setter
    def dbType(self,db_type:str):

        _s_fn_id = "function @dbType.setter"
        try:
            if db_type in self._dbTypesList:
                self._dbType = db_type
            elif appConf.get('NOSQLDB','DBTYPE'):
                self._dbType = appConf.get('NOSQLDB','DBTYPE')
            else:
                raise ConnectionError("Undefined dbType; set in app.cfg or as class property")

        except Exception as err:
            logger.error("%s %s \n",_s_fn_id, err)
            print("[Error]"+_s_fn_id, err)
            print(traceback.format_exc())

        return self._dbType

#     @property
#     def dbPort(self) -> str:
#         return self._dbPort

#     @dbPort.setter
#     def dbPort(self,db_port:str='27017') -> str:

#         _s_fn_id = "function @dbPort.setter"
#         try:
#             if not (db_port is None and db_port==""):
#                 self._dbPort = db_port
#             elif appConf.get('NOSQLDB','DBPORT'):
#                 self._dbPort = appConf.get('NOSQLDB','DBPORT')
#             else:
#                 raise ConnectionError("Undefined dbPort; set in app.cfg or as class property")

#         except Exception as err:
#             logger.error("%s %s \n",_s_fn_id, err)
#             print("[Error]"+_s_fn_id, err)
#             print(traceback.format_exc())

#         return self._dbPort

#     @property
#     def dbDriver(self) -> str:
#         return self._dbDriver

#     @dbDriver.setter
#     def dbDriver(self,db_driver:str) -> str:

#         _s_fn_id = "function @dbDriver.setter"
#         try:
#             if not (db_driver is None and db_driver==""):
#                 self._dbDriver = db_driver
#             elif appConf.get('NOSQLDB','DBDRIVER'):
#                 self._dbDriver = appConf.get('NOSQLDB','DBDRIVER')
#             else:
#                 raise ConnectionError("Undefined dbDriver; set in app.cfg or as class property")

#         except Exception as err:
#             logger.error("%s %s \n",_s_fn_id, err)
#             print("[Error]"+_s_fn_id, err)
#             print(traceback.format_exc())

#         return self._dbDriver

    @property
    def dbName(self) -> str:
        return self._dbName

    @dbName.setter
    def dbName(self,db_name:str) -> str:

        _s_fn_id = "function @dbName.setter"
        try:
            if not (db_name is None and db_name==""):
                self._dbName = db_name
            elif appConf.get('NOSQLDB','DBNAME'):
                self._dbName = appConf.get('NOSQLDB','DBNAME')
            else:
                raise ConnectionError("Undefined dbName; set in app.cfg or as class property")

        except Exception as err:
            logger.error("%s %s \n",_s_fn_id, err)
            print("[Error]"+_s_fn_id, err)
            print(traceback.format_exc())

        return self._dbName

#     @property
#     def dbUser(self) -> str:
#         return self._dbUser

#     @dbUser.setter
#     def dbUser(self,db_user:str) -> str:

#         _s_fn_id = "function @dbUser.setter"
#         try:
#             if not (db_user is None and db_user==""):
#                 self._dbUser = db_user
#             elif appConf.get('NOSQLDB','DBUSER'):
#                 self._dbUser = appConf.get('NOSQLDB','DBUSER')
#             else:
#                 raise ConnectionError("Undefined dbUser; set in app.cfg or as class property")

#         except Exception as err:
#             logger.error("%s %s \n",_s_fn_id, err)
#             print("[Error]"+_s_fn_id, err)
#             print(traceback.format_exc())

#         return self._dbUser

#     @property
#     def dbPswd(self) -> str:
#         return self._dbPswd

#     @dbPswd.setter
#     def dbPswd(self,db_pswd:str) -> str:

#         _s_fn_id = "function @dbPswd.setter"
#         try:
#             if not (db_pswd is None and db_pswd==""):
#                 self._dbPswd = db_pswd
#             elif appConf.get('NOSQLDB','DBPSWD'):
#                 self._dbPswd = appConf.get('NOSQLDB','DBPSWD')
#             else:
#                 raise ConnectionError("Undefined dbPswd; set in app.cfg or as class property")

#         except Exception as err:
#             logger.error("%s %s \n",_s_fn_id, err)
#             print("[Error]"+_s_fn_id, err)
#             print(traceback.format_exc())

#         return self._dbPswd

#     @property
#     def dbAuthSource(self) -> str:
#         return self._dbAuthSource

#     @dbAuthSource.setter
#     def dbAuthSource(self,db_auth_source:str) -> str:

#         _s_fn_id = "function @dbAuthSource.setter"
#         try:
#             if not (db_auth_source is None and db_auth_source==""):
#                 self._dbAuthSource = db_auth_source
#             elif appConf.get('NOSQLDB','DBAUTHSOURCE'):
#                 self._dbAuthSource = appConf.get('NOSQLDB','DBAUTHSOURCE')
#             else:
#                 raise ConnectionError("Undefined dbAuthSource; set in app.cfg or as class property")

#         except Exception as err:
#             logger.error("%s %s \n",_s_fn_id, err)
#             print("[Error]"+_s_fn_id, err)
#             print(traceback.format_exc())

#         return self._dbAuthSource

#     @property
#     def dbAuthMechanism(self) -> str:
#         return self._dbAuthMechanism

#     @dbAuthMechanism.setter
#     def dbAuthMechanism(self,db_auth_mechanism:str) -> str:

#         _s_fn_id = "function @dbAuthMechanism.setter"
#         try:
#             if not (db_auth_mechanism is None and db_auth_mechanism==""):
#                 self._dbAuthMechanism = db_auth_mechanism
#             elif appConf.get('NOSQLDB','DBAUTHSOURCE'):
#                 self._dbAuthMechanism = appConf.get('NOSQLDB','DBAUTHMECHANISM')
#             else:
#                 raise ConnectionError("Undefined dbAuthMechanism; set in app.cfg or as class property")

#         except Exception as err:
#             logger.error("%s %s \n",_s_fn_id, err)
#             print("[Error]"+_s_fn_id, err)
#             print(traceback.format_exc())

#         return self._dbAuthMechanism

    ''' Function - connect

            parameters:
                store_mode - local-fs sets to read and write on your local machine file system
                           aws-s3-bucket sets to read and write with an AWS S3 bucket 
            procedure: checks if it is a valid value and sets the mode
            return (str) self._connect

            author: <nuwan.waidyanatha@rezgateway.com>
            
    '''
    @property
    def connect(self):
        return self._connect

    @connect.setter
    def connect(self,connect_properties:dict={}):

        _db_host_ip=None
        _db_user=None
        _db_pswd=None
        _db_auth=None
        _db_mech=None
        _s_fn_id = "function @connect.setter"

        try:
            ''' check if properties in args or config file are defined '''
            if not (len(connect_properties) > 0 or "NOSQLDB" in appConf.sections()):
                raise TypeError("Input args in %s and NOSQLDB section in %s undefined"
                                % (_s_fn_id,self.__conf_fname__))
            ''' check and set DBHOSTIP from args or app config '''
            if "DBHOSTIP" in connect_properties.keys():
                    _db_host_ip = connect_properties['DBHOSTIP']
            elif appConf.get('NOSQLDB','DBHOSTIP'):
                    _db_host_ip = appConf.get('NOSQLDB','DBHOSTIP')
            else:
                raise ValueError("Undefined DBHOSTIP in function args and app config file. aborting")

            ''' check and set DBTYPE '''
            if "DBTYPE" in connect_properties.keys():
                self.dbType = connect_properties['DBTYPE']
            elif appConf.get('NOSQLDB','DBTYPE'):
                self.dbType = appConf.get('NOSQLDB','DBTYPE')
            else:
                raise ValueError("Undefined DBTYPE in function args and app config file. aborting")

            ''' check and set DBUSER from args or app config '''
            if "DBUSER" in connect_properties.keys():
                    _db_user = connect_properties['DBUSER']
            elif appConf.get('NOSQLDB','DBUSER'):
                    _db_user = appConf.get('NOSQLDB','DBUSER')
            else:
                raise ValueError("Undefined DBUSER in function args and app config file. aborting")

            ''' check and set DBPSWD from args or app config '''
            if "DBPSWD" in connect_properties.keys():
                    _db_pswd = connect_properties['DBPSWD']
            elif appConf.get('NOSQLDB','DBPSWD'):
                    _db_pswd = appConf.get('NOSQLDB','DBPSWD')
            else:
                raise ValueError("Undefined DBPSWD in function args and app config file. aborting")

            ''' check and set DBAUTHSOURCE from args or app config '''
            if "DBAUTHSOURCE" in connect_properties.keys():
                    _db_auth = connect_properties['DBAUTHSOURCE']
            elif appConf.get('NOSQLDB','DBAUTHSOURCE'):
                    _db_auth = appConf.get('NOSQLDB','DBAUTHSOURCE')
            else:
                raise ValueError("Undefined DBAUTHSOURCE in function args and app config file. aborting")

            ''' check and set DBAUTHMECHANISM from args or app config '''
            if "DBAUTHMECHANISM" in connect_properties.keys():
                    _db_mech = connect_properties['DBAUTHMECHANISM']
            elif appConf.get('NOSQLDB','DBAUTHMECHANISM'):
                    _db_mech = appConf.get('NOSQLDB','DBAUTHMECHANISM')
            else:
                raise ValueError("Undefined DBAUTHMECHANISM in function args and app config file. aborting")

            ''' initialize noSQLdbconnect '''
            if self.dbType.lower() == 'mongodb':
                self._connect = MongoClient(
                    _db_host_ip,
                    username=_db_user,
                    password=_db_pswd,
                    authSource=_db_auth,
                    authMechanism=_db_mech
                )
                logger.debug(self._connect)
            elif self.dbType.lower() == 'firebase':
                raise RuntimError("Firebase is to be included in a future release")
            else:
                raise ValueError("Undefined dbType. It must be one of %s" % self._dbTypeList)

        except Exception as err:
            logger.error("%s %s \n",_s_fn_id, err)
            print("[Error]"+_s_fn_id, err)
            print(traceback.format_exc())

        return self._connect

    ''' Function - data

            parameters:
                
            return (dtype) any data type: str, dict, list, dataframe, array, and so on

            author: <nuwan.waidyanatha@rezgateway.com>
            
    '''
    @property
    def documents(self):
        return self._documents

    @documents.setter
    def documents(self, docMeta:dict):
    
        _s_fn_id = "function @data.setter"

        _asType = None  # mandatory - data convertion type from store dict
        _dbName = None  # database name to query collections
        _dbColl = None   # mandatory - file path from store dict
        _docFind = None   # either - file name from store dict
#         _docType = None   # or - file type from store dict

        try:
            if ("ASTYPE" in [x.upper() for x in docMeta.keys()]) \
                and (docMeta['ASTYPE'].upper() in self._asTypeList):
                _asType = docMeta['ASTYPE']
            else:
                ''' set asTpe to a dict by daefulat '''
                _asType = "DICT"

            if not ("DBNAME" in [x.upper() for x in docMeta.keys()]):
                raise ValueError("Missing DBNAME and must be specified")
            _dbName = docMeta['DBNAME']

            ''' if not specified will return data from all collections '''
            if "COLLECTION" in [x.upper() for x in docMeta.keys()]:
                _dbColl = docMeta['COLLECTION']

            ''' if not specified will return data for all documents '''
            if "FIND" in [x.upper() for x in docMeta.keys()]\
                and isinstance(docMeta['FIND'],dict):
                _docFind = docMeta['FIND']
#             elif ("DOCTYPE" in [x.upper() for x in docMeta.keys()]) \
#                 and (docMeta['DOCNAME'].upper() in self._docTypeList):
#                 _docType = docMeta['DOCTYPE']
#             else:
#                 raise ValueError("Either a DOCNAME or DOCTYPE must be specified")

            self._documents = self.read_documents(
                as_type=_asType,
                db_name=_dbName,
                db_coll=_dbColl,
                doc_find=_docFind,
#                 doc_type=_fType,
            )
            logger.debug("%s execute find {%s} from %s",_s_fn_id,str(_docFind), _dbColl)

        except Exception as err:
            logger.error("%s %s \n",_s_fn_id, err)
            print("[Error]"+_s_fn_id, err)
            print(traceback.format_exc())

        return self._documents


    ''' Function - read_doc

            parameters:
                store_mode - local-fs sets to read and write on your local machine file system
                           aws-s3-bucket sets to read and write with an AWS S3 bucket 
            procedure: checks if it is a valid value and sets the mode
            return (str) self._documents

            author: <nuwan.waidyanatha@rezgateway.com>
            
    '''
    def converter(func):

        @functools.wraps(func)
        def wrapper_converter(self,
                 as_type:str,   # mandatory - define the data type to return
                 db_name:str,
                 db_coll:str,      # mandatory - relative path, w.r.t. self.storeRoot
                 doc_find:dict={},   # optional - name of the file to read
#                  doc_type:str=None    # optional - read all the files of same type 
                ):

            doc_dict = func(self,as_type,db_name,db_coll,doc_find)

            if as_type.upper() == 'DICT':
                self._documents = list(doc_dict)
            elif as_type.upper() == 'STR':
                self._documents=' '.join(list(doc_dict))
            elif as_type.upper() == 'PANDAS':
                self._documents=pd.DataFrame(list(doc_dict))
            else:
                ''' dtype unspecified return as dictionary '''
                self._documents=list(doc_dict)

            return self._documents

        return wrapper_converter

    @converter
    def read_documents(self, as_type, db_name, db_coll, doc_find):

        _s_fn_id = "function <read_documents>"
        doc_dict=None

        try:
            logger.debug("Reading files from %s",db_coll)
            
            if self.dbType.lower() == 'mongodb':
                ''' get data from MongoDB collection '''
                db = self.connect[db_name]
                if db_coll is None:
                    ''' get data from all the collections '''
                else:
                    ''' only from specified collection '''
                    _collection = db[db_coll]
                if doc_find:
                    doc_dict = db[db_coll].find(doc_find)
                else:
                    doc_dict = db[db_coll].find({})
#                 for doc in doc_dict:
#                     print(doc)
                    
            elif self.dbType.lower() == 'firebase':
                ''' get data from FireBase collection '''
                raise RuntimeError("FireBase read is tbd")
            else:
                raise ValueError("Something was wrong")

        except Exception as err:
            logger.error("%s %s \n",_s_fn_id, err)
            print("[Error]"+_s_fn_id, err)
            print(traceback.format_exc())

        return doc_dict

    ''' Function - write collection

            parameters:

            author: <nuwan.waidyanatha@rezgateway.com>
            
    '''
    def createDocs(func):

        @functools.wraps(func)
        def create_documents(
            self,
            db_name:str,   # optional - name of the file to read
            db_coll:str,   # mandatory - relative path, w.r.t. self.storeRoot
            data,   # data to be stored
            uuid_list,
        ):
            
            if isinstance(data,list):
                ''' list to collection '''
                self._documents = data
            elif isinstance(data,dict):
                self._documents = [data]
            elif isinstance(self.documents,pd.DataFrame):
                ''' dataframe to collection '''
                self._documents = [self.documents.to_dict()]
            else:
                raise TypeError("Unrecognized data type %s must be either of\n%s"
                                % (type(self.documents),str(self._asTypeList)))

            _collection=func(self,
                             db_name,
                             db_coll,
                             data,
                             uuid_list
                            )

#             if self.dbType.lower == 'mongodb':
#                 _insert_ids = _collection.insert_many(self.documents)
                
#             elif self.dbType.lower() == 'firebase':
#                 ''' get data from FireBase collection '''
#                 raise RuntimeError("FireBase write is tbd")
#             else:
#                 raise ValueError("Something was wrong")

            return self._documents 
        return create_documents
    
    @createDocs
    def write_documents(
        self,
        db_name:str,   # optional - name of the file to read
        db_coll:str,   # mandatory - relative path, w.r.t. self.storeRoot
        data=None,   # data to be stored
        uuid_list:list=[],   # unique identifier name to check if document exists
    ):

        _s_fn_id = "function <write_data>"
        _collection = None, 
        _objIds = None

        try:
            logger.debug("Writing document to %s",self.dbType)
            db = self.connect[db_name]
            
            ''' check if collection exists; else create one '''
            if self.dbType.lower() == 'mongodb':
                ''' get data from MongoDB collection '''
                if not db_coll in db.list_collection_names():
                    _collection = db[db_coll]
                    logger.info("Created a new collection %s",_collection)
                    ''' insert all the documents '''
                    results = db[db_coll].insert_many(self.documents)
                    logger.info("Inserted %d documents",db[db_coll].count_documents({}))
                else:
                    _insert_count = 0
                    _modify_count = 0
                    for data_dict in self.documents:
#                         ''' add ObjectIds if not in data '''
#                         if '_id' not in data_dict.keys() \
#                             or data_dict['_id'] is None \
#                             or data_dict['_id']=="":
#                             new_id = ObjectId()
#                             while not db[db_coll].find({"_id": {"$eq": ObjectId(new_id)}}):
#                                 new_id = ObjectId()
#                             data_dict['_id']=ObjectId(new_id)

                        ''' find and update on the key list '''
                        if not (len(uuid_list) > 0):
#                             _filter_dict = {}
#                             for _key in uuid_list:
#                                 _filter_dict[_key]=data_dict[_key]
                            uuid_list = list(data_dict.keys())
#                         else:
#                             uuid_list = data_dict.keys()

#                         print(uuid_list)
                        _filter_dict = {}
                        for _uuid in uuid_list:
                            _filter_dict[_uuid]=data_dict[_uuid]
#                         print(_filter_dict)
                        _filtered_cur = db[db_coll].find(_filter_dict)
#                         for x in _filtered_cur.clone():
#                             print(x)
                        ''' if no match then insert with new ObjectID '''
                        if len(list(_filtered_cur.clone())) <=0:
                            ''' add ObjectIds if not in data '''
                            doc = db[db_coll].insert_one(data_dict)
                            _insert_count += 1
#                             print('insert count:',doc.inserted_count)
#                             logger.info("Modified %d documents",doc.inserted_count)
#                             if '_id' not in data_dict.keys() \
#                                 or data_dict['_id'] is None \
#                                 or data_dict['_id']=="":
# #                                 _filtered_cur = data_dict
#                                 new_id = ObjectId()
#                                 while not db[db_coll].find({"_id": {"$eq": ObjectId(new_id)}}):
#                                     new_id = ObjectId()
#                                 data_dict['_id']=ObjectId(new_id)
# #                                 _filtered_cur['_id']=ObjectId(new_id)
#                             ''' insert the new data '''
#                             _filtered_cur.append(data_dict)
#                             print('inserted',doc)
                        else:
                            ''' update all matching documents '''
                            for _cur in _filtered_cur:

                                ''' update with existing ObjectId '''
#                                 doc = db[db_coll].update_one({'_id':_cur['_id']},{"$set": data_dict})
                                doc = db[db_coll].update_one(
                                    filter={
                                        '_id' : _cur['_id'],
                                    },
                                    update={
    #                                             '$setOnInsert': data_dict,
                                        '$set': data_dict,
                                    },
                                    upsert=True,
                                )
                                _modify_count += 1
                    print('Total %d documents, duccessful insert count = %d & modify count = %d'
                          %(len(self.documents),_insert_count, _modify_count))
                    logger.info("Total %d documents, duccessful insert count = %d & modify count = %d",
                                len(self.documents),_insert_count, _modify_count)

            elif self.dbType.lower() == 'firebase':
                ''' get data from FireBase collection '''
                raise RuntimeError("FireBase write is tbd")
            else:
                raise ValueError("Something was wrong")
            


        except Exception as err:
            logger.error("%s %s \n",_s_fn_id, err)
            print("[Error]"+_s_fn_id, err)
            print(traceback.format_exc())

        return db[db_coll]

    ''' convert to dict '''
    def convert_2_dict_mongodb(obj):
        result = {}
        for key, val in obj.items():
            if not isinstance(val, dict):
                result[key] = val
                continue

            for sub_key, sub_val in val.items():
                new_key = '{}.{}'.format(key, sub_key)
                result[new_key] = sub_val
                if not isinstance(sub_val, dict):
                    continue

                result.update(convert_2_dict_mongodb(result))
                if new_key in result:
                    del result[new_key]

        return result