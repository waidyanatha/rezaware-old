#!/usr/bin/env python3
# -*- coding: UTF-8 -*-

''' Initialize with default environment variables '''
__name__ = "app"
__package__ = "rezaware"
__conf_file__ = "app.cfg"
__ini_fname__ = "app.ini"

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

    container = "utils"   # utils, wrangler, mining, visuals
    storeMethod = None  # dir, s3bucket,
    dataStore = object
    confData = None
    database = object
    logs = object
    modules = []

    def __init__(self, container, module=None, package=None, **kwargs):

        self.__package__ = __package__
        self.confFile = __conf_file__
        self.iniFile = __ini_fname__
        self.config = None
        self.confData = None

        try:
            print("Initializing %s" % self.__package__)
            if not container in ['mining','utils','visuals','wrangler']:
                raise ValueError("Invalid app name".format(self.container))
            self.container = container

            if self.container == "mining":
                from wrangler import dags, logs, modules
            elif self.container == "utils":
                import utils
            elif self.container == "visuals":
                import visuals
            else:
                pass

            self.cwd = os.path.dirname(__file__)
            self.container_path = os.path.join(self.cwd,self.container)

            self.module = module
            self.package = package

        except Exception as e:
            print("Failed to initialize {0} with error:\n{1}".format(__package__,e))
            print(traceback.format_exc())


    def get_ini_data(self) -> list:

        self.confData = Config.set_conf_ini_conf(self.container_path,self.confFile)
        return self.confData


    def make_ini_files(self) -> str:
#         self.get_ini_data()
        self.container_path = os.path.join(self.cwd,self.container)
        self.confData,self.ini_file_list = Config.set_conf_ini_conf(
            reza_cwd=self.cwd,
            container=self.container,
            container_path=self.container_path,
            conf_file=self.confFile)
        return self.confData,self.ini_file_list

    def get_package_logger(self):
        return Logger.get_logger(self.cwd,
                                 self.container,
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


class Config(ConfigParser):

    def get_config(cwd:str,
                   container:str=None,
                   module:str=None,
                   package:str=None,
                   fName:str=None) -> ConfigParser:

        config = None
        try:
            if not fName:
                raise ValueError("Undefined config file name %s. Must be specified" % fName)
#             _conf_file_path = os.path.join(container, fName)
            if container:
                _conf_file_path = os.path.join(cwd,container)
            if module:
                _conf_file_path = os.path.join(_conf_file_path, "modules",module)
                if package:
                    _conf_file_path = os.path.join(_conf_file_path, package)
#             _conf_file_path = os.path.join(_conf_file_path, fName)
            ''' check if config container exists in folder path '''
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
                          container,
                          container_path,
                          conf_file) -> list:

        try:
            conf_data = Config.get_config(
                cwd=reza_cwd,
                container=container,
                fName = conf_file,
            )

            if not "LOGGING" in conf_data.sections():
                raise ValueError("No LOGGING section found in config")
            log_file_name = conf_data['LOGGING']['LOGFILE']
            log_path = conf_data['LOGGING']['LOGPATH']
            log_level = conf_data['LOGGING']['LOGLEVEL']
            log_mode = conf_data['LOGGING']['LOGMODE']
            _format_elements = conf_data['LOGGING']['LOGFORMAT'].split(',')
            log_format = " ".join(["%("+str(elem)+")s" for elem in _format_elements])\
                            .strip().replace(' ',' - ')

            _modules_path = os.path.join(container_path,"modules")
#             _mod_conf = Config.get_config(
#                 container=container,
#                 fName = conf_file,
#             )

            if not "MODULES" in conf_data.sections():
                raise ValueError("No MODULES section found in %s" % 
                                 os.path.join(conf_data))
            _config_list=[]
            _ini_conf_file_list = []
#             for module in _mod_conf['MODULES']:
#                 _sub_modules = _mod_conf['MODULES'][module].split(',')
            for module in conf_data['MODULES']:
                _sub_modules = conf_data['MODULES'][module].split(',')
                _modules_list=[]
                for pkg in _sub_modules:
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
                    _ini_conf.set("CWDS",str(container), str(container_path))
                    _ini_conf.set("CWDS",str(pkg), str(_pkg_path))
                    
                    ''' construct package configuration data '''
                    _pkg_list=[]
                    if pkg and pkg.strip():
                        ''' create the logger parameters and file path'''
                        _logs_path = os.path.join(
                            container_path,
                            log_path,
                            module,
                            pkg,
                            log_file_name,
                        )
                        log = {'Path':_logs_path,
                               'Level':log_level,
                               'Mode':log_mode,
                               'Format':log_format,
                              }
                        _ini_conf.add_section("LOGGER")
                        _ini_conf.set("LOGGER","PATH", str(_logs_path))
                        _ini_conf.set("LOGGER",'LEVEL',str(log_level))
                        _ini_conf.set("LOGGER",'MODE',str(log_mode))
                        _ini_conf.set("LOGGER",'FORMAT',str(log_format))

                        ''' create the data paths '''
                        data_path = os.path.join(
                            container_path,
                            "data/",
                            module,
                            pkg
                        )
                        data = {'dataPath':data_path}
                        _ini_conf.add_section("DATA")
                        _ini_conf.set("DATA","PATH", str(data_path))

                        file_list = []
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
                            if _s_pkg_list and _s_pkg_list.strip():
                                _s_pkg_list = "["+_s_pkg_list.strip().replace(" ",",")+"]"
#                                 _ini_conf.set("SUBMODULE","PACKAGES",str(_s_pkg_list))
                                _ini_conf.set("MODULES",str(module),str(_s_pkg_list))
                        _pkg_list.append(
                            {"data":data,
                             "logs":log,
                             "packages":file_list,
                            })
                    if len(_pkg_list) > 0:
                        _modules_list.append({pkg:_pkg_list})
                        _ini_conf.write(_ini_conf_file)
                        _ini_conf_file_list.append(str(ini_file_path))
                        _ini_conf_file.close()

                _config_list.append({module:_modules_list})

        except Exception as e:
            print("Error set_conf_ini_conf {0} with error:\n{1}".format(__package__,e))
            print(traceback.format_exc())

        return _config_list, [*set(_ini_conf_file_list)]

            
class Logger():
    
#     import logging
#     logger = logging.getLogger(__package__)

    def get_file_handler(container)-> logging.FileHandler:

        fHandler = None
        
        return fHandler
    
    def get_logger(cwd:str,container:str, module:str, package:str, ini_file):

        ''' TODO logging.formatter string is hard coded until the error
            can be resolved with getting format string from app.ini'''

        _log_format = '[%(levelname)s] - %(asctime)s - %(name)s - %(message)s'
        try:
            if not ini_file:
                ini_file = __ini_fname__

            if not cwd:
                raise ValueError("Undefined CWD (current working dir) - parameter set using clsHandler.cwd")

            if not container:
                raise ValueError("Undefined CONTAINER parameter set using clsHandler.container")

            if not module:
                raise ValueError("Undefined MODULE parameter set using clsHandler.module")

            if not package:
                raise ValueError("Undefined PACKAGE parameter set using clsHandler.module")

            _pkg_path = os.path.join(cwd,container,"modules",module,package)
            pkg_conf = Config.get_config(
                cwd=cwd,
                container=container,
                module=module,
                package=package,
                fName = ini_file,
            )
#             pkg_conf = Config.get_config(
#                 container=os.path.dirname(__file__),
#                 fName = ini_file,
#             )
            logger = logging.getLogger(module)
            logger.setLevel(pkg_conf.get('LOGGER','LEVEL'))
            if (logger.hasHandlers()):
                logger.handlers.clear()
            # create file handler which logs even debug messages
            _log_fpath = pkg_conf.get('LOGGER','PATH')
            if not os.path.dirname(_log_fpath):
                os.makedirs(os.path.dirname(_log_fpath))
            if not os.path.exists(_log_fpath):
                with open(_log_fpath, 'w') as fp:
                    pass
            fh = logging.FileHandler(_log_fpath,pkg_conf.get('LOGGER','MODE'))
            fh.setLevel(pkg_conf.get('LOGGER','LEVEL'))
#             _log_format = str(pkg_conf['LOGGER']['FORMAT'])
#             formatter = logging.Formatter(_log_format)
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