#!/usr/bin/env python3
# -*- coding: UTF-8 -*-

''' Initialize with default environment variables '''
__name__ = "NatLanWorkLoads"
__package__ = "natlang"
__module__ = "ml"   # machine learning
__app__ = "utils"
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
        self.__module__ = __module__
        self.__app__ = __app__
        self.__ini_fname__ = __ini_fname__
        self.__desc__ = desc
        _s_fn_id = "__init__"

        global config
        global logger
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
    def get_sentence_embeddings(
        self, 
        sentences:list=[],   # list of word ngrams 
        model_name:str='',   # https://www.sbert.net/docs/pretrained_models.html
        **kwargs
    ): 

        _s_fn_id = "function <create_embedding>"
        logger.info("Executing %s %s" % (self.__package__, _s_fn_id))
        
        embeddings = None

        try:
            if model_name in pre_trained_models:
                model = SentenceTransformer(model_name)
            else:
                model = SentenceTransformer('all-MiniLM-L6-v2')
                logger.warning("Invalid pretrained model, adopting defaule model: all-MiniLM-L6-v2")
#             if "MODEL_NAME" in kwargs.keys():
#                 model = SentenceTransformer(kwags['MODEL_NAME'])

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