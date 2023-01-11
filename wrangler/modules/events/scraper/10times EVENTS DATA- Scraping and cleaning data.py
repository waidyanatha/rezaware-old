#!/usr/bin/env python
# coding: utf-8

# # Niagara Falls Events list - 10Times.com
# 
# * List of all events in Niagara Falls for the next 11 months (from search month)
# 
# 1) Scrape all event names and event URLs
# 
# 2) Create and clean data frame with event names and URLs - into CSV file
# 
# 3) Loop and open each URL from CSV file and scrape event name, date, GIS locations- addresses

# # 1) Scrape all event names and event URLs

# In[1]:


#Load all libraries

from bs4 import BeautifulSoup
import requests
import lxml
import datetime
import csv
from csv import writer
import pandas as pd
import numpy as np
import re


# In[2]:


#10times site url - main page
url = "https://10times.com/niagarafalls-ca"


# In[30]:


#DOESN'T WORK - THE WRITER FINCTION TO CREATE CSV NEEDS TO BE CHANGED - NOT SURE HOW TO DOWNLOAD THE CSV FILE THROUGH THE FUNCTION 

def _scrape_event_urls_to_csv():
    
    
    
    try:
        headers = {'User-Agent':'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/99.0.4844.82 Safari/537.36'}
        response = requests.get(url, headers=headers)
        if response.status_code != 200:
            raise RuntimeError("Invalid response with code %d" % response.status_code)
            


        # Make it a soup
        soup = BeautifulSoup(page.text,'html.parser')
        lists= soup.find_all(class_='row py-2 mx-0 mb-3 bg-white deep-shadow event-card')

   
        with open('10timesScrapedEventURLs-RAW.csv', 'w', encoding='utf8', newline='') as f:
            fieldnames = ['Event name','URL']
            thewriter = csv.DictWriter(f, fieldnames=fieldnames)
            thewriter.writeheader()
            
        
        
            for event_title in lists:
                events= event_title.find_all('a', class_='text-decoration-none c-ga xn')

                for _event in events:
                    event_name=[]
                    try:
                        event_name = _event.get('data-ga-label')
                    except:
                        event_name.append("N/A")
                   
                    event_url=[]
                    try:
                        event_url = _event.get('href')
                    except:
                        event_url.append("N/A")

        thewriter.writerow({'Event name': event_name, 'URL': event_url})


# # 2) Filtering and cleaning dataframe
# 
# * Clean data and create final dataframe with Niagara Falls events names and URLs

# In[31]:


eventLinks10Times = pd.read_csv('10timesScrapedEventURLs-RAW.csv')
eventLinks10Times


# In[32]:


def _clean_df_with_events_and_urls():
    
    eventLinks10Times = eventLinks10Times.dropna()
    eventLinks10Times= eventLinks10Times[eventLinks10Times["Event name"].str.contains("Event name")==False]
    eventLinks10Times["Event name"] = eventLinks10Times["Event name"].apply(lambda x: x.replace("To", ""))
    eventLinks10Times = eventLinks10Times.reset_index(drop=True)
    


# # 3) Loop and open each URL, scrape event name, date, locations- addresses and other info into new dataframe

# In[27]:


links = eventLinks10Times["URL"] 


# In[28]:


#DOESN'T WORK - THE WRITER FINCTION TO CREATE CSV NEEDS TO BE CHANGED - NOT SURE HOW TO DOWNLOAD THE CSV FILE THROUGH THE FUNCTION 

def _open_event_urls_and_scrape_data_to_csv():
    
    try:
        headers = {'User-Agent':'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/99.0.4844.82 Safari/537.36'}
            response = requests.get(links, headers=headers)
            # check the response code
            if response.status_code != 200:
                raise RuntimeError("Invalid response with code %d" % response.status_code)




    for link in links:

        response = requests.get(link)
        soup = BeautifulSoup(response.text, 'html.parser')





        details = []

        try:
            details.append(soup.find(class_='position-relative col-md-8 pe-3'))
        except:
            details.append("N/A")



        information = []

        try:
            information.append(soup.find(class_='box'))
        except:
            information.append("N/A")



        locations = []

        try:
            locations.append(soup.find(class_='col-md-6'))
        except:
            locations.append("N/A")



        with open('niagaraFallsEventsDetails.csv', 'a', encoding='utf8', newline='') as f:
            fieldnames = ['Search date', 'Event title', 'Date', 'Labels', 'Tickets', 'Turnout', 'Latitude', 'Longitude', 'Address', 'Category', 'Other']
            thewriter = csv.DictWriter(f, fieldnames=fieldnames)
            thewriter.writeheader()
            currentDateTime = datetime.datetime.now()




            for detail in details:

                name=[]
                try:
                    name.append(detail.find('h1', class_='mb-0').text)
                except:
                    name.append("N/A")

                eventDate=[]   
                try:
                    eventDate.append(detail.find('div', class_='header_date position-relative text-orange').text)
                except:
                    eventDate.append("N/A")


                category=[]
                try:
                    category.append(detail.find('div', class_='mt-1 text-muted').text)
                except:
                    category.append("N/A")




            for info in information:

                tickets=[]
                try:
                    tickets.append(info.find('a', class_='text-decoration-none text-muted').text)
                except:
                    tickets.append("N/A")


                turnout=[]
                try:
                    turnout.append(info.find('div', class_='col-md-4').text)
                except:
                    turnout.append("N/A")


                labels=[]    
                try:
                    labels.append(info.find(id='hvrout2').text)
                except:
                    labels.append("N/A")


                other=[]
                try:
                    other.append(info.find('table', class_='table noBorder mng w-100 trBorder').text)
                except:
                    other.append("N/A")




            for location in locations:

                latitude=[]
                try:
                    latitude.append(location.find('span', id='event_latitude').text)
                except:
                    latitude.append("N/A")

                longitude=[]    
                try:
                    longitude.append(location.find('span', id='event_longude').text)
                except:
                    longitude.append("N/A")


                venue=[]    
                try:
                    venue.append(location.find('div', class_='mb-1').text)
                except:
                    venue.append("N/A")




            thewriter.writerow({'Search date': currentDateTime, 'Event title': name, 'Date': eventDate, 'Labels': labels, 'Tickets': tickets, 'Turnout': turnout, 'Latitude': latitude, 'Longitude': longitude, 'Address': venue, 'Category': category, 'Other': other})


# # Filter and Clean Dataframe

# In[29]:


def _clean_df_with_events_data():
    
    #remove unwanted characters and clean dataframe
    events = pd.read_csv('niagaraFallsEventsDetails.csv')
    events =events.drop_duplicates()
    events = events.drop(events.index[1])
    events = events.reset_index(drop=True)
    events.index = np.arange(1, len(events) + 1)
    events = events.applymap(lambda x: x.replace("[", "").replace("]", "").replace("'", ""))
    events=events.replace('N/A',np.NaN)
    events= events.dropna(subset = ['Event title', 'Date'])
    events = events.reset_index(drop=True)
    events['Address']=events['Address'].replace(r'\\n', '', regex=True)
    
    #clean date column and define start date and end date 
    events['Date']=events['Date'].str.replace("Add a Review","")
    dateRange= pd.DataFrame(events['Date'].str.split('-',1).to_list(),columns = ['Start date','End date'])
    dateRange['End date'].fillna(dateRange['Start date'], inplace=True)
    dateRange[['End day','End month','End year']] = dateRange['End date'].str.extract(r"^(.*)\s+(\S+)\s+(\d+)$", expand=True)
    dateRange[['Start day','Start month','Start year']] = dateRange['Start date'].str.extract(r"^(.*)\s+(\S+)*\s+(\d+)*$", expand=True)
    dateRange['Start day'].fillna(dateRange['Start date'], inplace=True)
    dateRange['Start month'].fillna(dateRange['End month'], inplace=True)
    dateRange['Start year'].fillna(dateRange['End year'], inplace=True)
    dateRange['Start']=dateRange['Start day'].astype(str)+dateRange['Start month'].astype(str)+ dateRange['Start year'].astype(str)
    dateRange['End']=dateRange['End day'].astype(str)+dateRange['End month'].astype(str)+dateRange['End year'].astype(str)
    dateRange['Start']=dateRange['Start day']+dateRange['Start month']+ dateRange['Start year']
    dateRange['End date']=dateRange['End']+dateRange['End month']+dateRange['End year']
    events['Start date']= pd.to_datetime(dateRange['Start'])
    events['End date']= pd.to_datetime(dateRange['End'])
    del events['Date']
    
    #Define event labels and category
    events['Labels']=events['Labels'].str.replace("Category & Type","")
    events['Category']=events['Category'].str.replace('[^a-zA-Z]+', '')
    events['Category']=events['Category'].str.replace('Ratingsxaxaxaxa', '')
    events['Category']=events['Category'].str.replace('Ratingxaxaxaxa', '')
    events['Labels']=events['Labels'].str.replace('Conference', '')
    events['Labels']=events['Labels'].str.replace('Trade Show', '')
    
    events['Labels'] = events['Labels'].replace(r"(\w)([A-Z])", r"\1 | \2", regex=True)
    events= events.assign(Labels=events['Labels'].str.split('|')).explode('Labels')
    events = events.rename(columns = {'index':'Event No.'})
    events.reset_index(inplace=True)
    events.index = np.arange(1, len(events) + 1)





