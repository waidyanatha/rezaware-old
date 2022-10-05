#!/usr/bin/env python3
# -*- coding: UTF-8 -*-

''' Initialize with default environment variables '''
__name__ = "nlp"
__package__ = "NatLanWorkLoads"
# __root_dir__ = "/home/nuwan/workspace/rezgate/wrangler/"   # need for logger path
__utils_dir__ = '/home/nuwan/workspace/rezgate/utils/'
__conf_fname__ = 'app.cfg'
__logs_dir__ = 'logs/'
__log_fname__ = 'app.log'

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

    ''' loading env vars, logging, and error reporting '''
    import configparser    
    import logging
    import traceback

    print("All packages in SparkWorkLoads loaded successfully!")

except Exception as e:
    print("Some {0} packages didn't load\n{1}".format(__package__,e))

'''
    CLASS run relevant natural language processing tasks
        1) 

    Resources:
        
'''
class NatLanWorkLoads():
    ''' Function
            name: __init__
            parameters:
                    @desc (str)
                    @enrich (dict)
            procedure: 
            return None
            
            author: <nuwan.waidyanatha@rezgateway.com>

    '''
    def __init__(self, desc : str="NLP workloads",   # identifier for the instances
                 **kwargs:dict,   # can contain hostIP and database connection settings
                ):

        self.__name__ = __name__
        self.__package__ = __package__
        self.__desc__ = desc
        _s_fn_id = "__init__"

        try:
            print("Initialing %s class for %s with instance %s"
                  % (self.__package__, self.__name__, self.__desc__))
            ''' initiate to load app.cfg data '''
            global confUtil
            confUtil = configparser.ConfigParser()

            ''' Set the utils root directory, it can be changed using kwargs '''
            self.utilsDir = __utils_dir__
            if "UTILS_DIR" in kwargs.keys():
                self.utilsDir = kwargs['UTILS_DIR']
            ''' load the utils config env vars '''
            self.utilConfFPath = os.path.join(self.utilsDir, __conf_fname__)
            confUtil.read(self.utilConfFPath)

            ''' get the file and path for the logger '''
            self.logDir = os.path.join(self.utilsDir,confUtil.get('LOGGING','LOGPATH'))
            if not os.path.exists(self.logDir):
                os.makedirs(self.logDir)
            self.logFPath = os.path.join(self.logDir,confUtil.get('LOGGING','LOGFILE'))
            ''' innitialize the logger '''
            global logger
            logger = logging.getLogger(__package__)
            self.logLevel = confUtil.get('LOGGING','LOGLEVEL')
            if self.logLevel == 'debug':
                logger.setLevel(logging.DEBUG)
            elif self.logLevel == 'info':
                logger.setLevel(logging.info)
            else:
                logger.setLevel(logging.DEBUG)
            if (logger.hasHandlers()):
                logger.handlers.clear()
            # create file handler which logs even debug messages
            fh = logging.FileHandler(self.logFPath, confUtil.get('LOGGING','LOGMODE'))
            fh.setLevel(logging.DEBUG)
            formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            fh.setFormatter(formatter)
            logger.addHandler(fh)
            ''' set a new logger section '''
            logger.info('########################################################')
            logger.info(__name__)
            logger.info('Utils Path = %s', self.utilsDir)

            ''' set tmp storage location from app.cfg '''
            self.tmpDIR = os.path.join(self.utilsDir,confUtil.get('STORES','TMPDATA'))
            ''' override if defined in kwargs '''
            if "TMP_DIR" in kwargs.keys():
                self.tmpDIR = kwargs['TMP_DIR']
            if not os.path.exists(self.tmpDIR):
                os.makedirs(self.tmpDIR)

            logger.debug("Connection complete! ready to load data.")
            print("Logging %s info, warnings, and error to %s" % (self.__package__, self.logFPath))

        except Exception as err:
            _s_fn_id = "Class <SparkWorkLoads> Function <__init__>"
            logger.error("%s %s \n",_s_fn_id, err)
            print("[Error]"+_s_fn_id, err)
            print(traceback.format_exc())

        return None

    ''' Function
            name: ngram_counter
            parameters:

                kwargs - 

            procedure: 

            return:  (Counter object)

            author: <nuwan.waidyanatha@rezgateway.com>

            TODO: complete the functions

    '''
    def get_sentence_embeddings(self, sentences:list, **kwargs):

        _s_fn_id = "function <create_embedding>"
        logger.info("Executing %s %s" % (self.__package__, _s_fn_id))
        
        embeddings = None

        try:
            model = SentenceTransformer('all-MiniLM-L6-v2')
            if "MODEL_NAME" in kwargs.keys():
                model = SentenceTransformer(kwags['MODEL_NAME'])

            if "NO_STOP_WORDS" in kwargs.keys() and kwargs["NO_STOP_WORDS"]==True:
                sentences = sentences.apply(lambda x: ' '\
                                            .join([word for word in x.split()\
                                                   if word not in (stop)]))

            if "LOWER" in kwargs.keys() and kwargs["LOWER"]==True:
                sentences = [sentence.lower()\
                             for sentence in sentences]
                             
            ''' encoded all the sentences in the list '''
            embeddings = model.encode(sentences)
            
        except Exception as err:
            logger.error("%s %s \n", _s_fn_id,err)
            print("[Error]"+_s_fn_id, err)
            print(traceback.format_exc())

        return sentences, embeddings


    ''' Function
            name: get_similarity_scores
            parameters:

                kwargs - 

            procedure: 

            return:  (list)

            author: <nuwan.waidyanatha@rezgateway.com>

            TODO: complete the functions

    '''
    def get_similarity_scores(self, input_sentences:list, lookup_sentences:list, **kwargs):

        _s_fn_id = "function <get_similarity_scores>"
        logger.info("Executing %s %s" % (self.__package__, _s_fn_id))
        
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
            logger.error("%s %s \n", _s_fn_id,err)
            print("[Error]"+_s_fn_id, err)
            print(traceback.format_exc())

        return _sim_scores_df


    ''' Function
            name: ngram_counter
            parameters:

                kwargs - 

            procedure: 

            return:  (Counter object)

            author: <nuwan.waidyanatha@rezgateway.com>

            TODO: complete the functions

    '''
    def ngram_counter(self, df, col, **kwargs):
        L = [x for x in df[col].unique() for x in bigrams(x.split())]
        c = Counter(L)
        top = c.most_common(20)
        return c

    ''' Function
            name: diff_scores
            parameters:

                kwargs - 

            procedure: 

            return:  (list object)

            author: <nuwan.waidyanatha@rezgateway.com>
            
            TODO: complete the functions

    '''
    def diff_scores(self, df1, df2, col, **kwargs):
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