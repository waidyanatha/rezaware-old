#!/usr/bin/env python3
# -*- coding: UTF-8 -*-
''' Initialize with default environment variables '''
__name__ = "ClusterWorkLoads"
__package__ = "cluster"
__module__ = "ml"
__app__ = "utils"
__ini_fname__ = "app.ini"
__conf_fname__ = "app.cfg"

''' Load necessary and sufficient python librairies that are used throughout the class'''
try:
    ''' essential python packages '''
    import os
    import sys
    import logging
    import traceback
    import functools
    import configparser
    ''' function specific python packages '''
    import pandas as pd
    import json
    from datetime import datetime, date, timedelta
    import numpy as np
    from sklearn.cluster import DBSCAN,AffinityPropagation,OPTICS,MeanShift,AgglomerativeClustering,Birch
    from sklearn.cluster import KMeans,SpectralClustering
    from sklearn.neighbors import NearestNeighbors
#     import hdbscan, pyamg
    from sklearn.preprocessing import StandardScaler
    from sklearn.datasets import make_blobs
    from sklearn.metrics.pairwise import haversine_distances

    print("All functional %s-libraries in %s-package of %s-module imported successfully!"
          % (__name__.upper(),__package__.upper(),__module__.upper()))

except Exception as e:
    print("Some packages in {0} module {1} package for {2} function didn't load\n{3}"\
          .format(__module__.upper(),__package__.upper(),__name__.upper(),e))

'''
    CLASS spefic to providing reusable functions for clustering point data
'''

class ClusterWorkLoads():

    ''' Function
            name: __init__
            parameters:

            procedure: Initialize the class
            return None

            author: <nuwan.waidyanatha@rezgateway.com>
    '''
    def __init__(self, desc : str="ClusterWorkLoads Class", **kwargs):

        self.__name__ = __name__
        self.__package__ = __package__
        self.__module__ = __module__
        self.__app__ = __app__
        self.__ini_fname__ = __ini_fname__
        self.__conf_fname__ = __conf_fname__
        self.__desc__ = desc
        
        self._data = None
        self._clusters = None
        self._category = None
        self._category_list = [
            'DBSCAN',
#             'HDBSCAN',
            'AFFINITYPROPAGATION',
            'OPTICS',
            'MEANSHIFT',
            'AGGLOMERATIVE',
            'BIRCH',
            'KMEANS',
            'SPECTRAL',    # spectral clustering with roots in graph theory
            'NEARESTNEIGHBORS',
            'DENCLUE',
        ]
        self._cluster_methods_list = ["xi","dbscan","eom","leaf","arpack", "lobpcg", "amg"]
        self._cluster_method=None
        self._centroidInit=None
        self._max_distance=None
        self._epsilon=0.5
        self._minimum_samples = 3
        self._minimum_cluster_size = 1
        self._cluster_std=0.4
        self._n_clusters=None
        self._randState=None
        self._maxIters=None
        self._algorithm='ball_tree'
        self._metric='haversine'
        self._fit_predict = False
        self._gen_min_span_tree=True
        self._prediction_data=True

        global pkgConf
        global logger

        try:
            self.cwd=os.path.dirname(__file__)
            pkgConf = configparser.ConfigParser()
            pkgConf.read(os.path.join(self.cwd,self.__ini_fname__))

            self.rezHome = pkgConf.get("CWDS","REZAWARE")
            sys.path.insert(1,self.rezHome)
            
            self.pckgDir = pkgConf.get("CWDS",self.__package__)
            self.appDir = pkgConf.get("CWDS",self.__app__)
#             ''' DEPRECATED: get the path to the input and output data '''
#             self.dataDir = pkgConf.get("CWDS","DATA")

#             ''' set app configparser '''
#             appConf = configparser.ConfigParser()
#             appConf.read(os.path.join(self.appDir, self.__conf_fname__))
            
            ''' innitialize the logger '''
            from rezaware import Logger as logs
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
            logger.error("%s %s \n",__s_fn_id__, err)
            print("[Error]"+__s_fn_id__, err)
            print(traceback.format_exc())

        return None


    ''' Function  --- DATA ---
            name: data @property and @setter functions
            parameters:

            procedure: The data must be a pandas dataframe or numpy array.
                        If dataframe, then the cluster labels will be appended
                        as a new column. Therefore, leave the data as a 
                        pandas dataframe.
            return self._data

            author: <nuwan.waidyanatha@rezgateway.com>
    '''

    @property
    def data(self):

        __s_fn_id__ = "function <@property data>"

        try:
            if not self._data is None and \
                not isinstance(self._data,pd.DataFrame) and \
                not isinstance(self._data,(np.ndarray, np.generic)):
                raise AttributeError("Invalid data type, data must be a valid "+\
                                     "pandas dataframe or numpy array")

        except Exception as err:
            logger.error("%s %s \n",__s_fn_id__, err)
            logger.debug(traceback.format_exc())
            print("[Error]"+__s_fn_id__, err)

        return self._data

    @data.setter
    def data(self,data):

        __s_fn_id__ = "function <@data.setter>"

#         self.session.conf.set("spark.sql.execution.arrow.enabled","true")

        try:
            if data is None:
                raise AttributeError("Dataset cannot be NoneType")
            if isinstance(data,(np.ndarray, np.generic)) and np.isfinite(data).all():
                self._data = data
                logger.debug("data is a numpy array with dimensions %s",str(self._data.shape))
            elif isinstance(data,pd.DataFrame) and \
                data.isna().sum().sum() == 0:
                self._data = data
                logger.debug("data is a pandas dataframe with dimensions %s",str(self._data.shape))
            else:
                raise ValueError("Invalid data set! Data must be a numpy array "+\
                                "or pandas dataframe without any NAN values in the cells")

        except Exception as err:
            logger.error("%s %s \n",__s_fn_id__, err)
            logger.debug(traceback.format_exc())
            print("[Error]"+__s_fn_id__, err)

        return self._data

    ''' Function -- CLUSTER PROPERTIES ---
            name: cluster @property and @setter functions
            procedure: The cluster property values are given as **kwargs key/value
                       pairs through a @classmethod. The @ property and @setter will 
                       validate the property value.
            parameters:
                @property - returns the inhenerent value
                @setter - sets the property value

            return self._* (* is the name of the property)

            author: <nuwan.waidyanatha@rezgateway.com>
    '''

    ''' --- CLUSTERS --- '''
    @property
    def clusters(self):
        return self._clusters

    @clusters.setter
    def clusters(self,clusters:list=[]) -> list:
                                     
        __s_fn_id__ = "function <@clusters.setter>"

        try:
            if len(clusters) != self.data.shape[0]:
                raise ValueError("Mismatch in cluster lables %d and data rows %d"
                                % (len(clusters),self.data.shape[0]))
            self._clusters =  clusters
            logger.debug("clusters set to %s",self._clusters)

        except Exception as err:
            logger.error("%s %s \n",__s_fn_id__, err)
            logger.debug(traceback.format_exc())
            print("[Error]"+__s_fn_id__, err)

        return self._clusters

    ''' --- CATEGORY --- '''
    @property
    def category(self):

        __s_fn_id__ = "function <@property category>"

        try:
            if not self._category in self._category_list:
                raise AttributeError("%s is an invalid clustering method, must be %s"
                                     % (self._category,str(self._category_list)))
                
        except Exception as err:
            logger.error("%s %s \n",__s_fn_id__, err)
            logger.debug(traceback.format_exc())
            print("[Error]"+__s_fn_id__, err)

        return self._category

    @category.setter
    def category(self,category):

        __s_fn_id__ = "function <@method.setter>"

#         self.session.conf.set("spark.sql.execution.arrow.enabled","true")

        try:
            if not category in self._category_list:
                raise AttributeError("%s is an invalid clustering method, must be %s"
                                     % (category,str(self._category_list)))
            self._category = category
            logger.debug("category set to %s",self._category)
                
        except Exception as err:
            logger.error("%s %s \n",__s_fn_id__, err)
            logger.debug(traceback.format_exc())
            print("[Error]"+__s_fn_id__, err)

        return self._category

    ''' --- N-CLUSTERS --- '''
    @property
    def n_clusters(self) -> int:
        
        __s_fn_id__ = "function <@n_clusters.setter>"

        try:
            if self._n_clusters < 2:
                logger.warning("%d clusters imply the data must be uniform")

        except Exception as err:
            logger.error("%s %s \n",__s_fn_id__, err)
            logger.debug(traceback.format_exc())
            print("[Error]"+__s_fn_id__, err)

        return self._n_clusters

    @n_clusters.setter
    def n_clusters(self,num_clusters:int=3) -> int:
                                     
        __s_fn_id__ = "function <@n_clusters.setter>"

        try:
            if num_clusters < 2:
                raise ValueError("Cannot create %d clusters must be > 1" % num_clusters)
            self._n_clusters =  num_clusters
            logger.debug("n_clusters set to %d",self._n_clusters)
                         
        except Exception as err:
            logger.error("%s %s \n",__s_fn_id__, err)
            logger.debug(traceback.format_exc())
            print("[Error]"+__s_fn_id__, err)

        return self._n_clusters

    ''' --- MAXIMUM ITERATIONS --- '''
    @property
    def maxIters(self):
        return self._maxIters

    @maxIters.setter
    def maxIters(self,max_iters:int=200) -> int:
                                     
        __s_fn_id__ = "function <@maxIters.setter>"

        try:
            if max_iters < 1:
                raise ValueError("Maximum interations must be > 1 and not %d" % max_iters)
            self._maxIters =  max_iters
            logger.debug("maxIters set to %d",self._maxIters)

        except Exception as err:
            logger.error("%s %s \n",__s_fn_id__, err)
            logger.debug(traceback.format_exc())
            print("[Error]"+__s_fn_id__, err)

        return self._maxIters

    ''' --- CENTROID INIT --- '''
    @property
    def centroidInit(self):
        return self._centroidInit

    @centroidInit.setter
    def centroidInit(self,centroid_init:int=5) -> int:
                                     
        __s_fn_id__ = "function <@centroidInit.setter>"

        try:
            if centroid_init < 5:
                raise ValueError("Ideal centroid init must be => 5 and not %d" % centroid_init)
            self._centroidInit =  centroid_init
            logger.debug("centroidInit set to %d",self._centroidInit)

        except Exception as err:
            logger.error("%s %s \n",__s_fn_id__, err)
            logger.debug(traceback.format_exc())
            print("[Error]"+__s_fn_id__, err)

        return self._centroidInit

    ''' --- RANDOM STATE --- '''
    @property
    def randState(self):
        return self._randState

    @randState.setter
    def randState(self,rand_state:int=0) -> int:
                                     
        __s_fn_id__ = "function <@randState.setter>"

        try:
            if rand_state < 0:
                raise ValueError("Ideal centroid init must be => 0 and not %d" % rand_state)
            self._randState =  rand_state
            logger.debug("randState set to %d",self._randState)

        except Exception as err:
            logger.error("%s %s \n",__s_fn_id__, err)
            logger.debug(traceback.format_exc())
            print("[Error]"+__s_fn_id__, err)

        return self._randState


    ''' Function
            name: label_data_clusters
            parameters:

            procedure: 
            return self._data

            author: <nuwan.waidyanatha@rezgateway.com>
    '''
    def buildClusters(func):
        
        @functools.wraps(func)
        def build_wrapper(self,data,category,columns,**kwargs):

            
            _cl_category = func(self,data,category,columns,**kwargs)

            ''' check for non-numeric columns '''
            if isinstance(self.data,pd.DataFrame):
                if len(columns) > 1 and (lambda x: x.isdigit(),self.data[columns]):
                    _data_array = np.array(self.data[columns])
                elif len(self.data.columns) > 1 and (lambda x: x.isdigit(),self.data):
                    _data_array = np.array(self.data)
                else:
                    raise ValueError("Invalid pandas dataframe with non-numeric values")
            else:
                _data_array = self.data
        
            if self.category == 'KMEANS':
                scaler = StandardScaler()
                scaled_features = scaler.fit_transform(_data_array)
                ''' init="random" or "k-means++"
                    n_init=10 (Number of runs with different centroid seeds)
                    max_iter=300 (Maximum number of iterations for a single run)
                    random_state=5 (Determines random number generation for centroid initialization)
                '''
                _cluster_model = KMeans(
                    init='k-means++',
                    n_clusters=self.n_clusters,
                    n_init=self.centroidInit,
                    max_iter=self.maxIters,
                    random_state=self.randState
                )
                _cluster_model.fit(scaled_features)
                self._clusters = _cluster_model.labels_
            else:
                logger.info("Undefined clustering category")

            return self._clusters

        return build_wrapper

#    @mergeClusters
    @buildClusters
    def cluster_n_label_data(
        self,
        data,     # pandas dataframe
        category, # clustering method see self._categorys_list
        columns,  # list of subset of columns to use
        **kwargs, # method specific clustering properties 
    ):

        __s_fn_id__ = "function <cluster_n_label_data>"

        try:
            self.data = data
            self.category = category
            if "NCLUSTERS" in kwargs.keys():
                self.n_clusters = kwargs["NCLUSTERS"]
            if "MAXITERATIONS" in kwargs.keys():
                self.maxIters = kwargs["MAXITERATIONS"]
            if "CENTROIDINIT" in kwargs.keys():
                self.centroidInit = kwargs["CENTROIDINIT"]
            if "RANDOMSTATE" in kwargs.keys():
                self.randState = kwargs["RANDOMSTATE"]

        except Exception as err:
            logger.error("%s %s \n",__s_fn_id__, err)
            logger.debug(traceback.format_exc())
            print("[Error]"+__s_fn_id__, err)

        return self._category

