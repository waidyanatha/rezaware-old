#!/usr/bin/env python3
# -*- coding: UTF-8 -*-

''' Initialize with default environment variables '''
__name__ = "NatLanWorkLoads"
__package__ = "natlang"    # natural language processing
__module__ = "ml"   # machine learning
__app__ = "utils"   # rezaware utils
__ini_fname__ = "app.ini"
__conf_fname__ = "app.cfg"

''' Load necessary and sufficient python librairies that are used throughout the class'''
try:
    import os
    import sys
    ''' for processing sentences '''
    import pandas as pd
    from sentence_transformers import SentenceTransformer, util
    from nltk.corpus import stopwords
    from nltk import trigrams, bigrams, edit_distance
    from collections import Counter
    from difflib import SequenceMatcher
    import re
    ''' pyspark packaes '''
    from pyspark.sql import functions as F
    from pyspark.sql import DataFrame

    ''' loading env vars, logging, and error reporting '''
    import configparser    
    import logging
    import traceback

    print("All functional %s-libraries in %s-package of %s-module imported successfully!"
          % (__name__.upper(),__package__.upper(),__module__.upper()))

except Exception as e:
    print("Some packages in {0} module {1} package for {2} function didn't load\n{3}"\
          .format(__module__.upper(),__package__.upper(),__name__.upper(),e))

'''
    CLASS runs pyspark nlp workloads
'''
class NatLanWorkLoads():
    ''' Function --- INIT ---

            author: <nuwan.waidyanatha@rezgateway.com>
    '''
    def __init__(self, desc : str="NLP workloads",   # identifier for the instances
                 **kwargs:dict,   # can contain hostIP and database connection settings
                ):
        """
        Decription:
            Initializes the ExtractFeatures: class property attributes, app configurations, 
                logger function, data store directory paths, and global classes 
        Attributes:
            desc (str) identify the specific instantiation and purpose
        Returns:
            None
        """

        self.__name__ = __name__
        self.__package__ = __package__
        self.__module__ = __module__
        self.__app__ = __app__
        self.__ini_fname__ = __ini_fname__
        if desc is None or "".join(desc.split())=="":
            self.__desc__ = " ".join([self.__app__,
                                      self.__module__,
                                      self.__package__,
                                      self.__name__])
        else:
            self.__desc__ = desc

        global config
        global logger
        
        __s_fn_id__ = f"{self.__name__} function <__init__>"

        try:
            self.cwd=os.path.dirname(__file__)

            config = configparser.ConfigParser()
            config.read(os.path.join(self.cwd,__ini_fname__))
            self.pckgDir = config.get("CWDS",self.__package__)
            self.containerDir = config.get("CWDS",self.__app__)
            ''' get the path to the input and output data '''
            self.dataDir = config.get("CWDS","DATA")

            self.rezHome = config.get("CWDS","REZAWARE")
            sys.path.insert(1,self.rezHome)
            from rezaware import Logger as logs
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
            from utils.modules.lib.spark import execSession as spark
            clsSpark = spark.Spawn(desc=self.__desc__)
            ''' set tmp storage location from app.cfg '''
            self.tmpDIR = None
            if "WRITE_TO_FILE" in kwargs.keys():
                self.tmpDIR = os.path.join(self.dataDir,"tmp/")
                if not os.path.exists(self.tmpDIR):
                    os.makedirs(self.tmpDIR)

            logger.debug("%s initialization for %s module package %s %s done.\nStart workloads: %s."
                         %(self.__app__,
                           self.__module__,
                           self.__package__,
                           self.__name__,
                           self.__desc__))

            print("%s Class initialization complete" % self.__name__)

        except Exception as err:
            logger.error("%s %s \n",_s_fn_id, err)
            logger.debug(traceback.format_exc())
            print("[Error]"+__s_fn_id__, err)

        return None

    ''' Function --- CLASS PROPERTIES ---

            author: <nuwan.waidyanatha@rezgateway.com>
                    <ushan.jayasuriya@colombo.rezgateway.com>
    '''
    ''' --- DATA --- '''
    @property
    def data(self):
        """
        Description:
            data @property and @setter functions. make sure it is a valid spark dataframe
        Attributes:
            data in @setter will instantiate self._data    
        Returns (dataframe) self._data
        """

        __s_fn_id__ = f"{self.__name__} function <@property data>"

        try:
            if self._data is not None and not isinstance(self._data,DataFrame):
                self._data = clsSFile.session.createDataFrame(self._data)
                logger.debug("%s converted non pyspark data object to %s",
                             __s_fn_id__,type(self._data))

        except Exception as err:
            logger.error("%s %s \n",__s_fn_id__, err)
            logger.debug(traceback.format_exc())
            print("[Error]"+__s_fn_id__, err)

        return self._data

    @data.setter
    def data(self,data):

        __s_fn_id__ = f"{self.__name__} function <@setter data>"

        try:
            if data is None:
                raise AttributeError("Invalid data attribute, must be a valid pyspark dataframe")
            if not isinstance(data,DataFrame):
                self._data = clsSFile.session.createDataFrame(data)
                logger.debug("%s converted %s object to %s",
                             __s_fn_id__,type(data),type(self._data))
            else:
                self._data = data

        except Exception as err:
            logger.error("%s %s \n",__s_fn_id__, err)
            logger.debug(traceback.format_exc())
            print("[Error]"+__s_fn_id__, err)

        return self._data


    ''' Function -- SENTENCE EMBEDDINGS ---

            author: <nuwan.waidyanatha@rezgateway.com>

            TODO: complete the functions

    '''
    @staticmethod
    def get_sentence_embeddings(
        sentences:list=[],   # list of word ngrams 
        model_name:str='',   # https://www.sbert.net/docs/pretrained_models.html
        **kwargs
    ): 
        """
        Description:
            For a given list of text sentences, the function generate an embedding.
            It makes use of a pretrained model to encode the sentences. 
        Attributes:
            sentences (list) - list of space seperated word ngrams
            model_name (str) - https://www.sbert.net/docs/pretrained_models.html
        Returns:
            sentences (list)
            embeddings (list)
        Exceptions:
        """

        __s_fn_id__ = f"{self.__name__} function <create_embedding>"

        pre_trained_models = [
            "all-MiniLM-L6-v2"
        ]
        __def_pre_train_model__ = "all-MiniLM-L6-v2"
        embeddings = None

        try:
            if model_name in pre_trained_models:
                model = SentenceTransformer(model_name)
            else:
                model = SentenceTransformer(__def_pre_train_model__)
                logger.warning("%s invalid pretrained model, adopting default: %s",
                               __s_fn_id__,__def_pre_train_model__)

            if "NOSTOPWORDS" in kwargs.keys() and kwargs["NOSTOPWORDS"]==True:
                sentences = sentences.apply(lambda x: ' '\
                                            .join([word for word in x.split()\
                                                   if word not in (stop)]))

            if "LOWER" in kwargs.keys() and kwargs["LOWER"]==True:
                sentences = [sentence.lower()\
                             for sentence in sentences]
                             
            ''' encoded all the sentences in the list '''
            embeddings = model.encode(sentences)
            
        except Exception as err:
            logger.error("%s %s \n", __s_fn_id__,err)
            logger.debug(traceback.format_exc())
            print("[Error]"+__s_fn_id__, err)

        return sentences, embeddings


    ''' Function

            author: <nuwan.waidyanatha@rezgateway.com>

            TODO: complete the functions

    '''
    def get_similarity_scores(self, input_sentences:list, lookup_sentences:list, **kwargs):
        """
        Description:
        Attributes:
        Returns:
        Exceptions:
        """

        __s_fn_id__ = f"{self.__name__} function <get_similarity_scores>"
        logger.info("Executing %s %s" % (self.__package__, __s_fn_id__))
        
        sim_scores = None
        return_sim_scores = None
        _l_sim_scores = []
        _sim_scores_df = pd.DataFrame()

        try:
            if not isinstance(input_sentences,list) and isinstance(lookup_sentences,list):
                raise ValueError("inputs must be of type list")

            _metric = 'COSIN'
            if "METRIC" in kwargs.keys():
                _metric = kwargs['METRIC']

            _head_scores = 1
            if "MAX_SCORES" in kwargs.keys():
                _head_scores = kwargs['MAX_SCORES']

            ''' get the embeddings for the lookup reference sentences '''
            _lukup_sents, _lukup_embeds = self.get_sentence_embeddings(lookup_sentences,**kwargs)

            ''' compute the similarity for each and evey input sentence '''
            for sentence in input_sentences:
                ''' embed the sentenceal sentece '''
                _inp_sentence, _inp_embedding = self.get_sentence_embeddings([sentence],**kwargs)
                
                if _metric == 'COSIN':
                    #Compute cosine similarity between my sentence, and each one in the corpus
                    sim_scores = util.cos_sim(_inp_embedding, _lukup_embeds)
                elif _metric == 'JACCARD':
                    pass
                else:
                    raise ValueError("Invalid similarity metricy %s" % _metric)
                # lets go through our array to construct a dataframe of all the values
                for arr in sim_scores:
                    for i, each_val in enumerate(arr):
                        _l_sim_scores.append({'input_sentence' :sentence,
                                              'lookup sentence':_lukup_sents[i],
                                              'score':each_val})
                _sim_scores_df = pd.concat([_sim_scores_df,pd.DataFrame(_l_sim_scores)])

        except Exception as err:
            logger.error("%s %s \n", __s_fn_id__,err)
            logger.debug(traceback.format_exc())
            print("[Error]"+__s_fn_id__, err)

        return _sim_scores_df


    ''' Function -- WORDS TO NUMBERS ---

            authors:<nuwan.waidyanatha@rezgateway.com>
                    <ushan.jayasuriya@colombo.rezgateway.com>

            TODO: complete the functions
    '''
    def remove_stop_words(
        self,
        data: DataFrame,   # dataframe that is pyspark.sql.DataFrame compatible
        col_names :dict,   # key=column with sentences to use & value = new column name
        stop_words:list,   # list of stopwords to remove from the sentences
        **kwargs,
    ) -> DataFrame:
        """
        Description:
            For a given set of columns, the process will remove the list of defined
            (or default) stop words from the column text.
        Attributes :
            data (DataFrame) - dataframe that is pyspark.sql.DataFrame compatible
            col_names (dict) - key=column with sentences to use & value = new column name
            stop_words(list) - list of stopwords to remove from the sentences
        returns :
            self._data (DataFrame) with additional column with stop word removed sentences
        Exceptions :
            data cannot be converted to pyspark dataframe, throw exception
            col_names is an invalid dict, throw exception
        """
        __s_fn_id__ = f"{self.__name__} function <remove_stop_words>"

        ''' declare variables & default values '''
        __col_pofix__ = "no_stop_word"

        try:
            ''' validate input attributes '''
            self.data = data
            if not isinstance(col_names,dict) and len(col_names)<=0:
                raise AttributeError("Specify, at least, one column to remove stop words")
            if not isinstance(stop_words,list) or len(stop_words)<=0:
                stop_words = stop
            ''' loop through each columns dictionary to remove stop words '''
            for _col,_new_col in col_names.items():
                try:
                    if _col not in self._data.columns:
                        raise ValueError("%s is an invalid collumn name" % (_col))
                    if _new_col is None or "".join(_new_col.split())=="":
                        _new_col = "_".join([_col,__col_pofix__])
                        
                        ''' ADD LOGIC >>>'''
#                         self._data = self._data.withColumn(_new_col,<<PYSPARK ADD LOGIC>>)
#                         sentences = sentences.apply(lambda x: ' '\
#                                                     .join([word for word in x.split()\
#                                                            if word not in (stop_words)]))

                    
                except Exception as col_err:
                    logger.warning("%s column had errors %s \n",__s_fn_id__, col_err)

            
            ''' check if function produced a valid return object '''
            if self._data is None or self._data.count() <= 0:
                raise RuntimeError("retuned an empty %s bow dict",type(self._data))
            logger.debug("%s successfully completed with enriching dataframe: %d rows and %d columns",
                         __s_fn_id__,self._data.count(),len(self._data.columns))

        except Exception as err:
            logger.error("%s %s \n",__s_fn_id__, err)
            logger.debug(traceback.format_exc())
            print("[Error]"+__s_fn_id__, err)

        return self._data


    ''' Function -- WORDS TO NUMBERS ---

            author: <ushan.jayasuriya@colombo.rezgateway.com>

    '''
    def words_to_numbers(
        self,
        data: DataFrame, # dataframe that is pyspark.sql.DataFrame compatible
        col_names: dict, # key=column with sentences to use & value = new column name
        num_type : str,  # the type of number to convert decimal, integer, roman numeral, etc
        **kwargs,
    ) -> DataFrame:
        """
        Description:
        Attributes :
        Returns :
        Exceptions :
        """
        __s_fn_id__ = f"{self.__name__} function <words_to_numbers>"

        ''' declare variables & default values '''

        try:
            ''' validate input attributes '''
            self._data = data
            
            ''' << ADD THE LOGIC HERE >> '''

            
            ''' check if function produced a valid return object '''
            if self._data is None or self._data.count() <= 0:
                raise RuntimeError("retuned an empty %s bow dict",type(self._data))
            logger.debug("%s successfully completed with enriching dataframe: %d rows and %d columns",
                         __s_fn_id__,self._data.count(),len(self._data.columns))

        except Exception as err:
            logger.error("%s %s \n",__s_fn_id__, err)
            logger.debug(traceback.format_exc())
            print("[Error]"+__s_fn_id__, err)

        return self._data


    ''' Function -- NGRAM COUNTER ---

            author: <ushan.jayasuriya@colombo.rezgateway.com>

            TODO: complete the functions

    '''
    @staticmethod
    def ngram_counter(df, col, **kwargs):
        """
        Description:
        Attributes:
        Returns:
        Exceptions:
        """
        L = [x for x in df[col].unique() for x in bigrams(x.split())]
        c = Counter(L)
        top = c.most_common(20)
        return c


    ''' Function -- DIFFERENCE SCORES --
    
            author: <nuwan.waidyanatha@rezgateway.com>
            
            TODO: complete the functions

    '''
    def diff_scores(self, df1, df2, col, **kwargs):
        """
        Description:
        Attributes:
        Returns:
        Exceptions:
        """
        _room_desc = pd.read_csv(os.path.join(DATA_DIR,"room_descriptions.csv"), delimiter=',')
        _l_result = []

        for text in df1[col][0:99]:
            max_seq_dist = -1
            best_seq_desc = None
            max_edit_dist = 100
            best_edit_desc = None
            for desc in df2[col]:
                seq = difflib.SequenceMatcher(None,text,desc)
                seq_d = seq.ratio()*100
                edit_d = edit_distance(text,desc)
                if seq_d > max_dist:
                    max_seq_dist = seq_d
                    best_seq_desc = desc
                if edit_d < max_edit_dist:
                    max_edit_dist = edit_d
                    best_edit_desc = desc
            _l_result.append({ 'room_type':text,
                              'seq_desc':best_seq_desc,
                              'seq_dist':max_seq_dist,
                              'edit_desc':best_edit_desc,
                              'edit_dist':max_edit_dist,
                             })
        return _l_result