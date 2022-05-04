#!/usr/bin/env python3
# -*- coding: UTF-8 -*-

'''
    CLASS to import data from various data sources:
        1) Import PDF, XML, JSON, Text, Streams, and other file formats
        2) Transform them into CSV or SQL tables
        3) Produce data import reports to identify data issues
'''
class DataImport():
    ''' Function
            name: __init__
            parameters:
                    @name (str)

            procedure: 

            return DataFrame

    '''
    def __init__(self,
                 name : str="extract_data",
                 source_path : str=Null,
                 destination_path : str=Null):

        self.name = name
        self._procs = ["all"]

        print(params)
        return None


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

