#!/usr/bin/env python3
# -*- coding: UTF-8 -*-

''' Initialize with default environment variables '''
__name__ = "app"
__module__ = "rezaware"
__package__ = "rezaware"
__conf_file__ = "app.cfg"
__ini_fname__ = "app.ini"
__log_fname__ = "app.log"

try:
    ''' Load necessary and sufficient python librairies that are used throughout the class'''
    import os
    import sys
    from configparser import ConfigParser
    import logging
    import traceback

    print("All python packages in %s loaded successfully!" % __package__)

except Exception as e:
    print("Some in packages in {0} didn't load\n{1}".format(__package__,e))
'''
    CLASS is a composite class that will:
    1. configures the app based on the deployment settings
    2. be inherited by all packages/classes at the time of initiating an instance
    3. provides a common set of methods used by the wrangler, utils, mining, and visuals

    It specificall provides:
    1. a data repositoty for unnstructured file storage; either localstoreMethod or S3 bucket
    2. database connectivity to the type, name, and schema of a defined database
    3. pyspark instance for using spark workloads
    4. logging critical, debug, warning, and info for all containers
    5. error handling

    Contributors:
        * <nuwan.waidyanatha@rezgateway.com>

    Resources:
        https://packaging.python.org/en/latest/

'''
# class AppSettings(dict):
    
#     def get_setting()-> dict:
        
#         return None

class App:
    
    ''' The App should initialize and instantiate the mining, wrangler, utils, and visuals
        apps (containers). It will use the app.cfg data to configure the app, define the
        database connection, and establish shared data storage method and paths. 
        
        The instantition should involke the app with setting all the paramters. Thereafter,
        share it as an object with the client. Thereafter, the client can use the object
        directly, without any initialization or instantiation to use the attributes and
        methods.
        
        The class should return instantiated modules with data paths for storing unstructured
        data and logging. 
    '''

    storeMethod = None  # dir, s3bucket,
    dataStore = object
    confData = None
    database = object
    logs = object
    modules = []

    def __init__(self, app_name, module=None, package=None, **kwargs):

        print("Attempt to initializing:",app_name)
        self.__name__ = __name__
        self.__package__ = __package__
        self.confFile = __conf_file__
        self.iniFile = __ini_fname__
        self.config = None
        self.confData = None

        try:
            if not app_name in ['mining','utils','visuals','wrangler']:
                raise ValueError("Invalid app name".format(self.appName))
            self.appName = app_name
            self.module = module
            self.package = package
            self.cwd = os.path.dirname(__file__)
            self.appPath = os.path.join(self.cwd,self.appName)

            global logger
            if not os.path.dirname(os.path.join(self.appPath,"logs/")):
                os.makedirs(os.path.dirname(os.path.join(self.appPath,"logs/")))                
            logger=Logger.get_logger(self.cwd,self.appName,None,None,self.confFile)
            logger.info("%s Initialization complete for %s %s",
                        self.__name__,self.__package__,self.appName)
            print("%s Initialization complete for %s %s"
                  % (self.__name__,self.__package__,self.appName))

        except Exception as e:
            print("{0} app - {1} module - failed to initialize {2} with error:\n{3}".
                  format(self.appName,self.module,self.package,e))
            print(traceback.format_exc())


    def get_ini_data(self) -> list:

        self.confData = Config.set_conf_ini_conf(self.appPath,self.confFile)
        return self.confData


    def make_ini_files(self) -> str:
#         self.get_ini_data()
        self.appPath = os.path.join(self.cwd,self.appName)
        self.ini_file_list = Config.set_conf_ini_conf(
            reza_cwd=self.cwd,
            app_name=self.appName,
            app_path=self.appPath,
            conf_file=self.confFile)
#         ''' remove AWS credentials from cfg '''
#         aws_dict = {'AWSAUTH' : [{'awsaccesskey' : "",
#                                   'awssecuritykey' : "",
#                                   'awsregion' : "",
#                                   'awsiampolicy' : "",
#                                   'awsiamuser' : ""
#                                  }]
#                    }

#         conf_fpath = Config.set_cfg_file_data(
#             reza_cwd=self.cwd,
#             app_name=self.appName,
#             conf_file=self.confFile,
#             cfg_dict = aws_dict)
            
        return self.ini_file_list

    def get_package_logger(self):
        return Logger.get_logger(self.cwd,
                                 self.appName,
                                 self.module,
                                 self.package,
                                 self.iniFile)

    def configure(Config):
        
        config = Config.get_configuration(self.confFile)
        print(config.confFile)
        _conf = Config(self.app)
        _data = DataStore(self.app)
        _dbms = Database(self.app)
        _logs = Logger(self.app)

        return App(conf=_conf, data=_data, dbms=_dbms, logs=_logs)

#     config = Config()
#     logger = Logger()
#     dbConn = DataBase()
#     dataStore = DataStore()

    pass


'''
    *** CLASS CONFIG ***

    Initializes a ConfigParser object for any of the rezaware apps, modules, and packages.
    Makes use of the app.cfg files to generate package-wise app.ini files. Use the config
    object for reading and writing app.ini and app.cfg data.

    Contributor(s):
        * nuwan.waidyanatha@rezgateway.com

'''

class Config(ConfigParser):

    def get_config(cwd:str,
                   app:str=None,
                   module:str=None,
                   package:str=None,
                   fName:str=None) -> ConfigParser:

        config = None
        _conf_file_path = None

        try:
            if not fName:
                raise ValueError("Undefined config file name %s. Must be specified" % fName)
#             _conf_file_path = os.path.join(app, fName)
            if cwd:
                _conf_file_path = cwd
            else:
                raise ValueError("Undefined working directory %s. Must be specified" % cwd)
            if app:
                _conf_file_path = os.path.join(_conf_file_path,app)
                if module:
                    _conf_file_path = os.path.join(_conf_file_path, "modules",module)
                    if package:
                        _conf_file_path = os.path.join(_conf_file_path, package)
#             _conf_file_path = os.path.join(_conf_file_path, fName)
            ''' check if config app exists in folder path '''
            if not os.path.exists(_conf_file_path):
                raise FileNotFoundError("%s No config file %s found in:"
                                        % (fName,_conf_file_path))

            config = ConfigParser()
            config.read(os.path.join(_conf_file_path,fName))
            return config

        except Exception as e:
            print("Config had error:\n{0}".format(e))
            print(traceback.format_exc())
            return None

    def set_conf_ini_conf(reza_cwd,
                          app_name,
                          app_path,
                          conf_file) -> list:

        _config_list=[]
        _ini_conf_file_list = []

        try:
            conf_data = Config.get_config(
                cwd=reza_cwd,
                app=app_name,
                fName = conf_file,
            )

            ''' TODO change to a function that returns a dataframe() '''
            if not "LOGGER" in conf_data.sections():
                raise ValueError("No LOGGER section found in config")
            log_file_name = conf_data['LOGGER']['FILE']
            log_path = conf_data['LOGGER']['PATH']
            log_level = conf_data['LOGGER']['LEVEL']
            log_mode = conf_data['LOGGER']['MODE']
            _format_elements = conf_data['LOGGER']['FORMAT'].split(',')
            log_format = " ".join(["%("+str(elem)+")s" for elem in _format_elements])\
                            .strip().replace(' ',' - ')

#             ''' get AWS security credentials '''
#             if 'AWSAUTH' in conf_data.sections():
#                 _aws_acc_key = conf_data['AWSAUTH']['awsaccesskey']
#                 _aws_sec_key = conf_data['AWSAUTH']['awssecuritykey']
#                 _aws_region = conf_data['AWSAUTH']['awsregion']
#                 _aws_iam_pol = conf_data['AWSAUTH']['awsiampolicy']
#                 _aws_iam_usr = conf_data['AWSAUTH']['awsiamuser']

            _modules_path = os.path.join(app_path,"modules")

            if not "MODULES" in conf_data.sections():
                raise ValueError("No MODULES section found in %s" % 
                                 os.path.join(conf_data))

            for module in conf_data['MODULES']:
                _mod_path = os.path.join(_modules_path,module)
                _mod_pkgs = conf_data['MODULES'][module].split(',')
#                 _mod_pkgs=next(os.walk(_mod_path))[1]
#                 _mod_pkgs=[x for x in _mod_pkgs if not x.startswith('.')]
#                 print(_mod_pkgs)

#                 _modules_list=[]
                _pkg_list=[]
                for pkg in _mod_pkgs:
                    _pkg_path = os.path.join(_modules_path,module,pkg)
                    ''' create the __init__ file with python header '''
                    with open(os.path.join(_pkg_path,'__init__.py'),"w") as f:
                        f.write("#!/usr/bin/env python3\n# -*- coding: UTF-8 -*-")
                    f.close()

                    ''' open app.ini file to save in module structure ''' 
                    ini_file_path = os.path.join(_pkg_path,__ini_fname__)
                    _ini_conf_file = open(ini_file_path, "w")

                    ''' new configParser instance for each package creation '''
                    _ini_conf = ConfigParser()
                    ''' add the current package working directory path '''
                    _ini_conf.add_section("CWDS")
                    _ini_conf.set("CWDS",str("rezaware"), str(reza_cwd))
                    _ini_conf.set("CWDS",str(app_name), str(app_path))
                    _ini_conf.set("CWDS",str(module), str(_mod_path))
                    _ini_conf.set("CWDS",str(pkg), str(_pkg_path))

                    ''' create the data paths '''
                    data_path = os.path.join(
                        app_path,
                        "data/",
                        module,
                        pkg
                    )
                    data = {'dataPath':data_path}
                    _ini_conf.set("CWDS","DATA", str(data_path))
                    ''' construct package configuration data '''

#                     _pkg_list=[]
                    if pkg and pkg.strip():
#                         ''' create the logger parameters and file path'''
#                         _logs_path = Logger.get_file_path(
#                             cwd=reza_cwd,
#                             app=app_name,
#                             module=module,
#                             package=pkg,
#                             logFName=None,
#                         )
#                         log = {'Path':_logs_path,
#                                'Level':log_level,
#                                'Mode':log_mode,
#                                'Format':log_format,
#                               }
#                         _ini_conf.add_section("LOGGER")
#                         _ini_conf.set("LOGGER","PATH", str(_logs_path))
#                         _ini_conf.set("LOGGER",'LEVEL',str(log_level))
#                         _ini_conf.set("LOGGER",'MODE',str(log_mode))
#                         _ini_conf.set("LOGGER",'FORMAT',str(log_format))

#                         ''' add AWS auth credentials '''
#                         _ini_conf.add_section("AWSAUTH")
#                         _ini_conf.set("AWSAUTH","ACCESSKEY", str(_aws_acc_key))
#                         _ini_conf.set("AWSAUTH",'SECURITYKEY',str(_aws_sec_key))
#                         _ini_conf.set("AWSAUTH",'REGION',str(_aws_region))
#                         _ini_conf.set("AWSAUTH",'IAMUSR',str(_aws_iam_usr))
#                         _ini_conf.set("AWSAUTH",'IAMPOLICY',str(_aws_iam_pol))

                        file_list = []   # temp stores the app.ini file list
                        _ini_conf.add_section("MODULES")
                        for root, dirs, files in os.walk(
                            os.path.join(_modules_path,module,pkg)):
                            _s_pkg_list = ""
                            for file in files:
                                # Check whether file is in text format or not
                                if file.endswith(".py") and file != "__init__.py":
                                    #append the file name to the list
                                    file_list.append(
                                        os.path.splitext(file)[0])
                                    _s_pkg_list += os.path.splitext(file)[0]+" "
                            if _s_pkg_list != "" and _s_pkg_list.strip():
#                                 _s_pkg_list = "["+_s_pkg_list.strip().replace(" ",",")+"]"
                                _pkg_list = "["+_s_pkg_list.strip().replace(" ",",")+"]"
#                                 _ini_conf.set("SUBMODULE","PACKAGES",str(_s_pkg_list))
                                _ini_conf.set("MODULES",str(module),str(_s_pkg_list))
#                         _pkg_list.append(
#                             {"data":data,
#                              "logs":log,
#                              "packages":file_list,
#                             })

                        ''' create the logger parameters and file path'''
                        _logs_path = Logger.get_file_path(
                            cwd=reza_cwd,
                            app=app_name,
                            module=module,
                            package=pkg,
                            logFName=None,
                        )
#                         log = {'Path':_logs_path,
#                                'Level':log_level,
#                                'Mode':log_mode,
#                                'Format':log_format,
#                               }
                        _ini_conf.add_section("LOGGER")
                        _ini_conf.set("LOGGER","PATH", str(_logs_path))
                        _ini_conf.set("LOGGER",'LEVEL',str(log_level))
                        _ini_conf.set("LOGGER",'MODE',str(log_mode))
                        _ini_conf.set("LOGGER",'FORMAT',str(log_format))

                    ''' write ini data to file '''
                    if len(_pkg_list) > 0:
#                         _modules_list.append({pkg:_pkg_list})
                        _ini_conf.write(_ini_conf_file)
                        _ini_conf_file_list.append(str(ini_file_path))
                        _ini_conf_file.close()

#                 _config_list.append({module:_modules_list})

        except Exception as e:
            print("Error set_conf_ini_conf {0} with error:\n{1}".format(__package__,e))
            print(traceback.format_exc())

        return [*set(_ini_conf_file_list)]

    ''' Function
            name: set_cfg_file_data
            parameters:
                reza_cwd (str) - rezaware directory path
                app_name (str) - mining, utils, visuals, or wrangler
                conf_file (str)- generally app.cfg
                cfg_dict (dict)- dictionary with secotor specific key value pairs
            procedure: 
            return None
            
            author: <nuwan.waidyanatha@rezgateway.com>

    '''
    def set_cfg_file_data(
        reza_cwd,
        app_name,
        conf_file,
        cfg_dict) -> str:

        _new_conf_file = None

        try:
            conf_data = Config.get_config(
                cwd=reza_cwd,
                app=app_name,
                fName = conf_file,
            )

            for _sector in cfg_dict:
                for _key_val_pair in cfg_dict[_sector]:
                    for _key, _val in _key_val_pair.items():
                        conf_data.set(_sector,_key,_val)

#             cfgfile = open(file_path,'w')
            with open(os.path.join(reza_cwd,app_name,conf_file), 'w') as configfile:
                conf_data.write(configfile)

        except Exception as e:
            print("Error set_conf_ini_conf {0} with error:\n{1}".format(__package__,e))
            print(traceback.format_exc())

        return _new_conf_file

    

'''
    *** CLASS LOGGER ***

    Initializes a logger object for any of the rezaware apps, modules, and packages.

    Contributor(s):
        * nuwan.waidyanatha@rezgateway.com

'''

class Logger():
    
#     import logging
#     logger = logging.getLogger(__package__)

    def get_file_handler(app)-> logging.FileHandler:

        fHandler = None
        
        return fHandler

    def get_file_path(cwd:str,app:str, module:str, package:str, logFName:str):

        _log_fpath = None
        try:
            if not logFName:
                logFName = __log_fname__
            if cwd:
                _log_fpath=cwd
            else:
                raise ValueError("Undefined CWD (current working dir)")
            if app:
                _log_fpath=os.path.join(_log_fpath,app,"logs/")
            else:
                raise ValueError("Undefined app %s not in [mining, utils, visuals, wrangler]"
                                 % app)
            if module:
#                 _log_fpath = os.path.join(_log_fpath,"modules",module)
                _log_fpath = os.path.join(_log_fpath,module)
#                     _log_name = module
                if package:
                    _log_fpath = os.path.join(_log_fpath,package)

            if not os.path.isdir(_log_fpath):
                os.makedirs(_log_fpath)
            _log_fpath = os.path.join(_log_fpath,logFName)
            if not os.path.exists(_log_fpath):
                with open(_log_fpath, 'w') as fp:
                    pass

        except Exception as e:
            print("Config had error:\n{0}".format(e))
            print(traceback.format_exc())
            
        return _log_fpath

    
    def get_logger(cwd:str,app:str, module:str, package:str, ini_file:str):

        ''' TODO logging.formatter string is hard coded until the error
            can be resolved with getting format string from app.ini'''

        _log_format = '[%(levelname)s] - %(asctime)s - %(name)s - %(message)s'
        _log_name = "rezaware"
        _log_fpath = None

        try:
            _log_fpath = Logger.get_file_path(
                cwd=cwd,
                app=app,
                module=module,
                package=package,
                logFName=None)

            if not _log_fpath:
                raise ValueError("Unrecovered LOG file for {0} app in {1}"
                                 .format(app,cwd))

            ''' assing the last not None value of the three values as logger name '''
            _log_name = str([x for x in [app, module, package] if x !=None][-1])

            _conf = Config.get_config(
                cwd=cwd,
                app=app,
                module=module,
                package=package,
                fName = ini_file,
            )

            logger = logging.getLogger(_log_name)
            logger.setLevel(_conf.get('LOGGER','LEVEL'))
            if (logger.hasHandlers()):
                logger.handlers.clear()
            fh = logging.FileHandler(_log_fpath,_conf.get('LOGGER','MODE'))
            fh.setLevel(_conf.get('LOGGER','LEVEL'))
            formatter = logging.Formatter(_log_format)
            fh.setFormatter(formatter)
            logger.addHandler(fh)
            return logger

        except Exception as e:
            print("Config had error:\n{0}".format(e))
            print(traceback.format_exc())
            return None


class Database:
    pass

class DataStore(App):
    
    def get_package_data_store():
        
        try:
            print('nothing happening')
        
        except Exception as e:
            print("Config had error:\n{0}".format(e))
            print(traceback.format_exc())
            return None

# class InitApp(App):
    
#     class Argument:
#         app_args = AppSettings()
        
#         def get_args():
#             return None