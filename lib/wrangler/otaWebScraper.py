#!/usr/bin/env python3
# -*- coding: UTF-8 -*-

''' Load necessary and sufficient python librairies that are used throughout the class'''
try:
    import os
    from numpy import isin
    import numpy as np
    from datetime import datetime, timedelta, date, timezone
    import pandas as pd
    import traceback

    print("All packages in OTAWebScraper loaded successfully!")

except Exception as e:
    print("Some packages didn't load\n{}".format(e))

'''
    CLASS spefic to scraping date from Online Travel Ageny web sites; e.g. booking.com, hotel.com,
    Use a scheduler to run main.py which will execute the relavent functions to scrape and file the data
        1) Load the ota_input_data.json comprising the url with parameter list and specific data find html tags
        2) separate the scrape inputr paramenter list for the url from the scrape data find html tags
        3) Inset the input parameters into the url to build a list of scrapable urls
        4) Scraped data is parsed, using the html tags find
        5) data is stored according to the specified folder and structure
'''

class OTAWebScraper():

    ''' Function
            name: __init__
            parameters:

            procedure: Initialize the class
            return None

            author: nuwan.waidyanatha@rezgateway.com
    '''
    def __init__(self, name : str="data"):
        
        self.name = name
        self.path = "../../data/hospitality/bookings/scraper"
        self.ratesStoragePath = "../../data/hospitality/bookings/scraper/rates"
        self.file = "ota_input_urls.json"

        self.scrape_start_date = date.today()
        self.scrape_end_date = self.scrape_start_date + timedelta(days=1)
        self.page_offset = 10
        self.page_upper_limit = 150
        self.checkout_offset = 1

        ''' TODO move to a CSV or JSON format with any other relavent data elements'''
        self.destination_id = ["Las Vegas",
                               "New York",
                               "Orlando",
#                          "Boston, USA",
#                          "Colombo, Sri Lanka",
                         ]
        self.scrape_columns = ["search_dt", # date&time scraping ran
                               "property_name",   # hotel name
                               "checkin_date",    # anticipated checkin date
                               "destination_id",  # hotel destination id
                               "destination_name",   # hotel located city
                               "destination_country",# hotel located country
                               "room_type",    # room type; e.g., double bed, queen bed
                               "adult_count",  # number of adults
                               "child_count",  # numbe of children
                               "room_rate",    # price per night
                               "review_score", # rating
                               "other_info",   # any other relevant text
                              ]
        print("Initialing OTAWebScraper class for ",self.name)
        return None

    ''' Function
            name: get_url_list
            parameters:
                dirPath - the relative or direct path to the file with urls
                fileName - the name of the file containing all the urls for scraping
            procedure: read the list of urls from the CSV file and compile a list
            return list (url_list)

            author: nuwan.waidyanatha@rezgateway.com
    '''
    def load_ota_list(self, dirPath:str, fileName:str):

        import os         # apply directory read functions
        import csv        # to read the csv
        import json       # to read the json file

        url_list = []     # initialize the return parameter
        property_data = {}
        
        try:

            ''' Get the list of urls from the CSV file '''        
            if dirPath:
                self.path = dirPath
            _l_files = os.listdir(self.path)
            ''' if fileName is not null check if it is in the director '''
            if fileName and fileName in _l_files:
                self.file = fileName
            else:
                raise ValueError("Invalid file name %s in dir: %s. Does not exist!" % (fileName, self.path))

            ''' read the list of urls from the file '''
            with open(self.path+"/"+self.file, newline='') as f:
                property_data = json.load(f)

        except Exception as err:
            _s_fn_id = "Class <WebScraper> Function <get_url_list>"
            print("[Error]"+_s_fn_id, err)
            print(traceback.format_exc())

        return property_data

    ''' Function
            name: get_scrape_input_params
            parameters:
                url - string comprising the url with place holders
                **kwargs - contain the plance holder key value pairs

            procedure: build the url by inserting the values from the **kwargs dict
            return string (url)
            
            author: nuwan.waidyanatha@rezgateway.com

            TODO - change the ota_scrape_tags_df to a list of dictionaries
    '''
    def get_scrape_input_params(self, property_dict:dict):
        try:
            if not property_dict:
                raise ValueError("Invalid properties dictionary")

            ''' loop through the dict to construct the scraper parameters '''
            ota_param_list = []
            _l_tag=[]
            for prop_detail in property_dict:
                param_dict = {}
                tag_dict = {}
                ''' create a dict with input params '''
                param_dict['ota'] = prop_detail
                for detail in property_dict[prop_detail]:
                    param_dict['url'] = detail['url']
                    param_dict['inputs'] = detail['inputs']
                    param_dict['destinations'] = detail['destinations']
                    ''' append the input parameters into a list'''
                    ota_param_list.append(param_dict)
      
        except Exception as err:
            _s_fn_id = "Class <WebScraper> Function <get_scrape_input_params>"
            print("[Error]"+_s_fn_id, err)
            print(traceback.format_exc())

        return ota_param_list #, ota_scrape_tags_df

    ''' Function
            name: get_scrape_output_params
            parameters:
                property_dict - obtained from loading the property scraping parameters from the JSON

            procedure: loop through the loaded dictionary to retrieve the output variable names, tags, and values.
                        Then construcct and return a dataframe for all corresponding OTAs
            return dataframe (_scrape_tags_df)

            author: nuwan.waidyanatha@rezgateway.com
    '''
    def get_scrape_html_tags(self, property_dict:dict):

        _scrape_tags_df = pd.DataFrame()

        try:
            if not property_dict:
                raise ValueError("Invalid properties dictionary")

            ''' loop through the dict to construct html tags to retrieve the data elements '''
            for prop_detail in property_dict:
                for _prop_params in property_dict[prop_detail]:
                    for _out_vars in _prop_params['outputs']:
                        _out_vars['ota'] = prop_detail
                        _scrape_tags_df = pd.concat([_scrape_tags_df,\
                                                     pd.DataFrame([_out_vars.values()], columns=_out_vars.keys())],
                                                   ignore_index=False)

        except Exception as err:
            _s_fn_id = "Class <WebScraper> Function <get_scrape_output_params>"
            print("[Error]"+_s_fn_id, err)
            print(traceback.format_exc())

        return _scrape_tags_df

    ''' Function
            name: build_scrape_url_list
            parameters:
                dirPath - path to the directory with property parameters JSON
                fileName - JSON file containing those parameters

            procedure: use the get_scrape_input_params function to load the the JSON file. 
                        Thenloop through all the OTAs to extract the input parameters
                        For each OTA template url use the insert_params_in_url function to
                        construct the list of parameterized URLs
            return: list with the set of urls (scrape_url_list)
            author: nuwan.waidyanatha@rezgateway.com

            TODO: the nested looping code to add the location, checkin, checkout, page is dirty
                    Need a better method rather than nested loop because not all OTAs will consist
                    of the same input parameters
    '''
    def get_destination_ids(self, fileName, dirPath=None, col_name=None):

        _l_dests = []

        try:
            if not fileName:
                raise ValueError("Invalid file name")

            ''' see if the file exists '''
            if not dirPath:
                dirPath = self.path
            if dirPath[-1] != "/":
                dirPath +="/"
            file_path = dirPath+fileName
            if not os.path.exists(file_path):
                raise ValueError("File %s does not exisit in folder %s" %(fileName,dirPath))

            ''' read the destination ids into the list '''
            dest_df = pd.read_csv(file_path, sep=',', quotechar='"')
            if not col_name:
                col_name = "destinationID"
            _l_dests = list(dest_df[col_name])


        except Exception as err:
            _s_fn_id = "Class <WebScraper> Function <get_scrape_output_params>"
            print("[Error]"+_s_fn_id, err)
            print(traceback.format_exc())

        return _l_dests


    ''' Function
            name: insert_params_in_url
            parameters:
                url - string comprising the url with place holders
                **kwargs - contain the plance holder key value pairs

            procedure: build the url by inserting the values from the **kwargs dict
            return string (url)

            author: nuwan.waidyanatha@rezgateway.com
    '''
    def insert_params_in_url(self, url: str, **kwargs ):

        import re

        url_w_params = None

        try:
            if not url:
                raise ValueError("Invalid url string %s" % (url))
            url_w_params = url

            ''' match the keys in dict with the placeholder string in the url''' 
            for key in kwargs.keys():
                _s_regex = r"{"+key+"}"
                urlRegex = re.compile(_s_regex, re.IGNORECASE)
                param = urlRegex.search(url_w_params)
                if param:
                    _s_repl_val = str(kwargs[key]).replace(" ","%20")
                    url_w_params = re.sub(_s_regex, _s_repl_val, url_w_params)
            
        except Exception as err:
            _s_fn_id = "Class <WebScraper> Function <insert_params_in_url>"
            print("[Error]"+_s_fn_id, err)
            print(traceback.format_exc())

        return url_w_params

    ''' Function
            name: build_scrape_url_list
            parameters:
                dirPath - path to the directory with property parameters JSON
                fileName - JSON file containing those parameters

            procedure: use the get_scrape_input_params function to load the the JSON file. 
                        Thenloop through all the OTAs to extract the input parameters
                        For each OTA template url use the insert_params_in_url function to
                        construct the list of parameterized URLs
            return: list with the set of urls (scrape_url_list)
            author: nuwan.waidyanatha@rezgateway.com

            TODO: the nested looping code to add the location, checkin, checkout, page is dirty
                    Need a better method rather than nested loop because not all OTAs will consist
                    of the same input parameters
    '''
    def build_scrape_url_list(self, dirPath:str, fileName:str, **kwargs):

#        scrape_url_list = []
        _scrape_url_dict = {}
        _ota_parameterized_url_list = []

        try:
            ''' retrieve OTA input and output property data from json '''
            property_dict = self.load_ota_list(dirPath, fileName)
            if len(property_dict) <= 0:
                raise ValueError("No data found with %s with defined scrape properties"
                                 % (path+"/"+fileName))
            else:
                print("Loaded %d properties to begin scraping OTA data." % (len(property_dict)))
            ''' check and initialize **kwargs '''
            if 'pageOffset' in kwargs:
                self.page_offset = kwargs['pageOffset']
            if 'pageUpperLimit' in kwargs:
                self.page_upper_limit = kwargs['pageUpperLimit']
            if 'startDate' in kwargs:
                self.scrape_start_date = kwargs['startDate']
            else:
                print("Invalid scrape start date. Setting start date to today %s" %(str(self.scrape_start_date)))
            if 'endDate' in kwargs:
                if self.scrape_start_date < kwargs['endDate']:
                    self.scrape_end_date = kwargs['endDate']
                else:
                    self.scrape_end_date = self.scrape_start_date + timedelta(days=1)
                    print("Invalid scrape end date. Setting end date to %s %s" 
                          %(str(self.scrape_start_date),str(self.scrape_end_date)))
            if 'checkoutOffset' in kwargs:
                self.checkout_offset = kwargs['checkoutOffset']

            ''' set the directory path to csv files with destination ids '''
            if dirPath[-1] != "/":
                _dest_dir_path = dirPath+"/"+"destinations"
            else:
                _dest_dir_path = dirPath+"destinations"

            ''' get the input parameters from the properties file '''
            _ota_input_param_list = self.get_scrape_input_params(property_dict)
            
            ''' loop through the  ota list to create the url list for each ota '''
            for ota in _ota_input_param_list:
                print("Processing %s ..." % (ota['ota']))
                _inert_param_dict = {}
                try:
                    _ota_url = None
                    if not ota['url']:
                        raise ValueError("Invalid url skip to next")

                    ''' get the list of destination ids '''
                    if "destinations" in ota.keys():
                        _l_dest = self.get_destination_ids(fileName=ota["destinations"],   # filename with destination ids
                                                           dirPath=_dest_dir_path,   # path to the desitnation folder
                                                           col_name="destinationID"  # column name with destination ids
                                                          )
                        if len(_l_dest) > 0:
                            self.destination_id = _l_dest

                    ''' build the dictionary to replace the values in the url place holder '''
                    for destID in self.destination_id:
                        _inert_param_dict['destinationID'] = destID

                        if 'checkIn' in ota['inputs']:
                            for day_count in range(0,(self.scrape_end_date - self.scrape_start_date).days):
                                _inert_param_dict['checkIn'] = self.scrape_start_date + timedelta(days=day_count)
                                ''' if checkout date is a necessary parameter then add 1 day to checkout date'''
                                if 'checkOut' in ota['inputs']:
                                    _inert_param_dict['checkOut'] = self.scrape_start_date \
                                                                    + timedelta(days=day_count+self.checkout_offset)
                                if "page" in ota['inputs']:
                                    page_offset = 0
                                    _parameterized_url = None
                                    while page_offset <= self.page_upper_limit:
                                        _inert_param_dict['page'] = page_offset
                                        _parameterized_url = self.insert_params_in_url(ota['url'],**_inert_param_dict)
#                                        scrape_url_list.append(_parameterized_url)
                                        _scrape_url_dict = {}     # init otherwise will overwrite the list
                                        _scrape_url_dict['ota']=ota['ota']
                                        _scrape_url_dict['destination_id']=destID
                                        _scrape_url_dict['checkin']=_inert_param_dict['checkIn']
                                        _scrape_url_dict['page_offset']=page_offset
                                        _scrape_url_dict['url']=_parameterized_url
                                        _ota_parameterized_url_list.append(_scrape_url_dict)
                                        page_offset += self.page_offset

                    ''' add to dict the paramterized url list for OTA as key '''
#                    print(scrape_url_list)
#                    _scrape_url_dict[ota] = scrape_url_list

                except Exception as err:
                    ''' skip to processing the next OTA url if this one is None '''
                    print(err)

        except Exception as err:
            _s_fn_id = "Class <WebScraper> Function <build_scrape_url_list>"
            print("[Error]"+_s_fn_id, err)
            print(traceback.format_exc())

        return _scrape_url_dict, _ota_parameterized_url_list


    ''' Function
            name: read_folder_csv_to_df
            parameters:
                dirPath - string with folder path to the csv files
                **kwargs - contain the plance holder key value pairs
                            columns: list
                            start_date: datetime.date
                            end_date: datetime.date
            procedure: 
            return string (_s3Storageobj)

            author: nuwan.waidyanatha@rezgateway.com
    '''
    def get_search_data_dir_path(self,dirPath, **kwargs):

        _SearchDataDir = None
        _search_dt = datetime.now()
        
        try:
            ''' establish the storage block '''
            if dirPath:
                self.ratesStoragePath = dirPath
            ''' add the folder if not exists '''
            if self.ratesStoragePath[-1] != "/":
                self.ratesStoragePath +="/"
            if not os.path.exists(self.ratesStoragePath):
                os.makedirs(self.ratesStoragePath)

            if "search_datetime" in kwargs.keys():
                _search_dt = kwargs["search_datetime"]

            ''' TODO - change the function to round search datetime to nearest 30 min
                using _distinct_df['search_datetime']=_distinct_df['search_datetime'].dt.round('30min') '''
            ''' pick the year, month, day, hour, min from search date time ''' 
            _minute = _search_dt.minute
            if _minute < 30:
                _minute = 0
            else:
                _minute = 30

            ''' folder is a concaternation of date hour and minute; where minute < 30 --> 0 and 30 otherwise'''
            _SearchDataDir = self.ratesStoragePath+\
                            str(_search_dt.year)+"-"+str(_search_dt.month)+"-"+str(_search_dt.day)\
                            +"-"+str(_search_dt.hour)+"-"+str(_minute)+"/"     # csv file name

            ''' add the folder if not exists '''
            if not os.path.exists(_SearchDataDir):
                os.makedirs(_SearchDataDir)

        except Exception as err:
            _s_fn_id = "Class <WebScraper> Function <scrape_data_to_csv>"
            print("[Error]"+_s_fn_id, err)
            print(traceback.format_exc())

        return _SearchDataDir

    ''' Function
            name: scrape_ota_to_csv
            parameters:
                url - string comprising the url with place holders
                **kwargs - contain the plance holder key value pairs

            procedure: build the url by inserting the values from the **kwargs dict
            return string (url)

            author: nuwan.waidyanatha@rezgateway.com
    '''
    def scrape_data_to_csv(self,url,_scrape_tags_df,fileName, path):

        from bs4 import BeautifulSoup # Import for Beautiful Soup
        import requests # Import for requests
        import lxml     # Import for lxml parser
        import csv
        from csv import writer

        try:
            if _scrape_tags_df.shape[0] <= 0:
                raise ValueError("Invalid scrape tags no data scraped")
            if not fileName:
                raise ValueError("Invalid file name no data scraped")
            if not path:
                raise ValueError("Invalid path name no data scraped")

            ''' define generic header '''
            headers = {'User-Agent':'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/99.0.4844.82 Safari/537.36'}
            response = requests.get(url, headers=headers)
            # Make it a soup
            soup = BeautifulSoup(response.text,"lxml")
#            soup = BeautifulSoup(response.text,"html.parser")

            ''' extract the list of values from content block '''
            _cont_block = (_scrape_tags_df.loc[_scrape_tags_df['variable']=='content_block']).head(1)
            _l_scrape_text = soup.select(_cont_block.tag.item())

            if len(_l_scrape_text) <= 0:
                raise ValueError("no content block (area) for %s" %(_cont_block))

            ''' get the attribute list '''
            _l_col_names = list(_scrape_tags_df.variable)
            _l_col_names.remove('content_block')

            ''' init dataframe to store the scraped categorical text '''
            _prop_data_df = pd.DataFrame()

            ''' loop through the list to retrieve values from tags '''
            for row in _l_scrape_text:
                _scraped_data_dict = {}
                for colName in _l_col_names:
                    _tag = _scrape_tags_df.loc[_scrape_tags_df.variable==colName, 'tag'].item()
                    _code = _scrape_tags_df.loc[_scrape_tags_df.variable==colName, 'code'].item()

                    try:
                        _scraped_data_dict[colName] = row.find(_tag, class_ = _code).text

                    except Exception as err:
                        pass
                        
                if _scraped_data_dict:
                    _prop_data_df = pd.concat([_prop_data_df, pd.DataFrame(_scraped_data_dict, index=[0])])

        except Exception as err:
            _s_fn_id = "Class <WebScraper> Function <scrape_data_to_csv>"
            print("[Error]"+_s_fn_id, err)
            print(traceback.format_exc())

        return _prop_data_df

    ''' Function
            name: _scrape_bookings_to_csv
            parameters:
                url - string comprising the url with place holders
                **kwargs - contain the plance holder key value pairs

            procedure: build the url by inserting the values from the **kwargs dict
            return string (url)

            author: nileka.desilva@rezgateway.com
    '''
    def _scrape_bookings_to_csv(self,
                                url,   # parameterized url
                                checkin_date, # intended checkin date
                                search_dt,    # scrape run date time
                                destination_id, # location searched for
                                fileName,     # store in csv file
                                path          # directory path to store csv
                               ):

        from bs4 import BeautifulSoup # Import for Beautiful Soup
        import requests # Import for requests
        import lxml     # Import for lxml parser
        import csv
        from csv import writer

        saveTo = None
        _save_df = pd.DataFrame()

        try:
            headers = {'User-Agent':'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/99.0.4844.82 Safari/537.36'}
            response = requests.get(url, headers=headers)
            # Make it a soup
            soup = BeautifulSoup(response.text,"lxml")

            lists = soup.select(".d20f4628d0")
            lists2 = soup.select(".c8305f6688")

            saveTo = path+"/"+fileName

            if len(lists) <= 0:
                raise ValueError("No data received for %s" % (url))

            for _list in lists:
                _data_dict = {}
                _data_dict['search_dt'] = search_dt,
                _data_dict['checkin_date'] = checkin_date,
                _data_dict['property_name'] = _list.find('div', class_='fcab3ed991 a23c043802').text
                _data_dict['room_type'] = _list.find('span', class_='df597226dd').text
                _data_dict['room_rate'] = _list.find('span', class_='fcab3ed991 bd73d13072').text
                _data_dict['review_score'] = _list.find('div', class_='b5cd09854e d10a6220b4').text
                _data_dict['destination_id'] = destination_id,
                _data_dict['location_desc'] = _list.find('div', class_='a1fbd102d9').text
                _data_dict['other_info'] = _list.find('div', class_='d22a7c133b').text

                if bool(_data_dict):
                    _save_df = pd.concat([_save_df,pd.DataFrame(_data_dict)])

            if _save_df.shape[0] > 0:
                _save_df.to_csv(saveTo,index=False, sep=',')
            
        except Exception as err:
            _s_fn_id = "Class <WebScraper> Function <_scrape_bookings_to_csv>"
            print("[Error]"+_s_fn_id, err)
            print(traceback.format_exc())

        return saveTo


    ''' Function
            name: scrape_url_list
            parameters:
                ota_url_list - string with folder path to the csv files
                **kwargs - contain the plance holder key value pairs
                            columns: list
                            start_date: datetime.date
                            end_date: datetime.date
            procedure: reads the all the csv files in the entire folder and
                        appends the data for the relevant columns defined in
                        the dictionary into a dataframe
            return dataframe (ota_bookings_df)

            author: nuwan.waidyanatha@rezgateway.com
    '''

    def scrape_url_list(self,ota_url_list: list, searchDT: datetime, dirPath:str):

        saveTo = None   # init file name
        _l_saved_files = []

        try:
            if ota_url_list == []:
                raise ValueError("Invalid list of URLs")

            ''' loop through the list of urls to scrape and save the data'''
            for ota_dict in ota_url_list:
#                _ota_tags_df = _scrape_tags_df.loc[_scrape_tags_df['ota']==ota_dict['ota']]

                ''' file name is concaternation of ota name + location + checkin date + page offset and .csv file extension'''
                _fname = str(ota_dict['ota'])+"."+\
                        str(ota_dict['destination_id'])+"."+\
                        str(ota_dict['checkin'])+"."+\
                        str(ota_dict['page_offset']).zfill(3)+\
                        ".csv"
                _fname=_fname.replace(" ",".")

#                print("Processsing ota=%s location=%s for checkin=%s and page=%s" 
#                      % (ota_dict['ota'],ota_dict['location'],str(ota_dict['checkin']),str(ota_dict['page_offset'])))
                ''' TODO add search_datetime'''
                if ota_dict['ota'] == 'booking.com':
                    saveTo = self._scrape_bookings_to_csv(ota_dict['url'],      # constructed url with parameters
                                                          ota_dict['checkin'],  # booking intended checkin date
                                                          searchDT,   # date & time scraping was executed
                                                          ota_dict['destination_id'],  # destingation id to lookup the name
                                                          _fname,       # csv file name to store in
                                                          dirPath     # folder name to save the files
                                                         )
                    _l_saved_files.append(saveTo)
#                    print("Data saved to %s" % saveTo)
#                else:
#                    print("Define a scraper function for %s, no data saved" % ota_dict['ota'])
#    _scraped_data_df = clsScraper.scrape_data_to_csv(ota_dict['url'],_ota_tags_df,_fname, _s3object)
#    _scraped_data_df = clsScraper._scrape_data_to_csv(ota_dict['url'],_fname, _s3object)
#            print("Scraping and data save to csv complete!")

        except Exception as err:
            _s_fn_id = "Class <WebScraper> Function <scrape_url_list>"
            print("[Error]"+_s_fn_id, err)
            print(traceback.format_exc())

        return _l_saved_files

    ''' Function
            name: remove_empty_files
            parameters:
                dirPath - string with folder path to the csv files
                **kwargs - contain the plance holder key value pairs
                            columns: list
                            start_date: datetime.date
                            end_date: datetime.date
            procedure: reads the all the csv files in the entire folder and
                        appends the data for the relevant columns defined in
                        the dictionary into a dataframe
            return dataframe (ota_bookings_df)

            author: nuwan.waidyanatha@rezgateway.com
    '''

    def remove_empty_files(self,path):

        _l_removed_files = []
        try:
            if not path:
                path = self.path
            
#            print(list(os.walk(path)))
            for (dirpath, folder_names, files) in os.walk(path):
                for filename in files:
                    file_location = dirpath + '/' + filename  #file location is location is the location of the file
                    if os.path.isfile(file_location):
                        if os.path.getsize(file_location) == 0:#Checking if the file is empty or not
                            os.remove(file_location)  #If the file is empty then it is deleted using remove method
                            _l_removed_files.append(filename)
#                        else:
#                            with open(file_location, 'r') as infile, \
#                                open(file_location, 'w') as outfile:
#                                data = infile.read()
#                                data = data.replace("•", " ")
#                                outfile.write(data)


        except Exception as err:
            _s_fn_id = "Class <WebScraper> Function <scrape_url_list>"
            print("[Error]"+_s_fn_id, err)
            print(traceback.format_exc())

        return _l_removed_files

    
    ''' Function
            name: read_folder_csv_to_df
            parameters:
                dirPath - string with folder path to the csv files
                **kwargs - contain the plance holder key value pairs
                            columns: list
                            start_date: datetime.date
                            end_date: datetime.date
            procedure: reads the all the csv files in the entire folder and
                        appends the data for the relevant columns defined in
                        the dictionary into a dataframe
            return dataframe (ota_bookings_df)

            author: nuwan.waidyanatha@rezgateway.com
    '''

    def read_folder_csv_to_df(self,dirPath: str, **kwargs):

        ota_rates_df = pd.DataFrame()     # initialize the return var
        _tmp_df = pd.DataFrame()
        _start_dt = None
        _end_dt = None
        _l_cols = []
        try:
            ''' check if the folder and files exists '''
            if not dirPath:
                raise ValueError("Invalid folder path %s" % dirPath)
            filelist = os.listdir(dirPath)
            if not (len(filelist) > 0):
                raise ValueError("No data files found in director: %s" % (dirPath))

            ''' extract data from **kwargs if exists '''
            if 'columns' in kwargs.keys():
                self.scrape_columns = kwargs['columns']
            if 'start_datetime' in kwargs.keys():
                _start_dt = kwargs['start_datetime']
            if 'end_datetime' in kwargs.keys():
                _start_dt = kwargs['end_datetime']

            '''' loop through files to get the data  '''
            for _s_file in filelist:
                if _s_file.endswith(".csv"):
                    _s_csv_file = dirPath+_s_file
                    _tmp_df = pd.read_csv(_s_csv_file, 
                                          sep=",",
                                          quotechar='"',
                                          skip_blank_lines=True,
#                                          false_values=[" • "]
                                         )
                    ota_rates_df = pd.concat([ota_rates_df,_tmp_df])
#                    print(ota_rates_df.shape)
            ota_rates_df.columns = self.scrape_columns
            ota_rates_df.reset_index(drop=True)

        except Exception as err:
            _s_fn_id = "Class <WebScraper> Function <read_folder_csv_to_df>"
            print("[Error]"+_s_fn_id, err)
            print(traceback.format_exc())

        return ota_rates_df