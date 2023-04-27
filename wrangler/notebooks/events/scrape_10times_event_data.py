#!/usr/bin/env python
# coding: utf-8

'''Events list - 10Times.com
* List of all events in 7 locations for the next 11 months (from search month)
1) Scrape all event names and event URLs
2) Create and clean data frame with event names and URLs - download into CSV files
3) Loop and open each URL from CSV file and scrape event name, date, GIS locations- addresses'''

'''Load necessary and sufficient python libraries'''
from bs4 import BeautifulSoup # Import for Beautiful Soup
import requests # Import for requests
import lxml # Import for lxml parser
import datetime
from datetime import datetime
import csv
from csv import writer
import pandas as pd
import numpy as np
import re
from selenium.common.exceptions import NoSuchElementException
import boto3
from io import StringIO
import time
from time import sleep
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.common.exceptions import NoSuchElementException
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException,StaleElementReferenceException
from selenium.webdriver.support.ui import WebDriverWait as wait

'''Define all 7 destinations to scrape event data from'''
destinationIDs= ["boston-us", "chicago-us", "lasvegas-us", "newyork-us","niagarafalls-ca", "orlando-us", "washington-us"]

'''Scrape all event title and event URLs and add to df'''
for destinationID in destinationIDs:
    try:
        main_link = f"https://10times.com/{destinationID}"
        headers = {'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/109.0.0.0 Safari/537.36 Edg/109.0.1518.61'}
        # Request for the URL
        page = requests.get(main_link, headers=headers)
        # Make it a soup
        soup = BeautifulSoup(page.text,'html.parser')
        event_titles_df = pd.DataFrame(columns=['Event_Title','Event URL', 'Event Type'])    
        events= soup.find_all('a', class_='text-decoration-none c-ga xn')
        for event in events:
            try:
                url= event.get('href')
            except:
                url.append("N/A")
            try:
                name= event.get('data-ga-label')
            except:
                name.append("N/A")             
            try:
                event_type= event.get('data-ga-action')
            except:
                event_type.append("N/A")                 

            event_titles_df = event_titles_df.append({'Event_Title':name,'Event URL': url, 'Event Type': event_type}, ignore_index=True)
            event_titles_df = event_titles_df.dropna()
            event_titles_df= event_titles_df[event_titles_df["Event_Title"].str.contains("Event_Title")==False]
            event_titles_df["Event_Title"] = event_titles_df["Event_Title"].apply(lambda x: x.replace("To", ""))
            event_titles_df= event_titles_df.reset_index(drop=True)
            event_titles_df= event_titles_df[event_titles_df['Event Type'] =='Event Listing | Event Snippet']
    except:
        pass
    
    links = event_titles_df["Event URL"]
    event_details_df = pd.DataFrame(columns=['Search date', 'Event title', 'Date', 'Category', 'Labels', 'Turnout', 'Latitude', 'Longitude', 'Address', 'Event link'])
    for link in links:
        global driver
        driver= webdriver.Chrome(r'C:/home/nileka/anaconda3/lib/python3.9/site-packages/selenium/webdriver/chrome/webdriver.py')
        driver.get(link)
        try:
            driver.implicitly_wait(30)
            button_element= driver.find_element("xpath", '/html/body/div/div[1]/div/div[2]/span')
            driver.execute_script("arguments[0].click();",button_element)
        except NoSuchElementException:
            pass
        event_link= link
        soup = BeautifulSoup(driver.page_source, 'html.parser')
        currentDate = datetime.now().strftime("%m-%d-%Y")
        detail=soup.find(class_='position-relative col-md-8 pe-3')
        try:
            name=detail.find('h1', class_='mb-0').text
        except:
            name.append("N/A")
        try:
            eventDate=detail.find('div', class_='header_date position-relative text-orange me-5').text
        except:
            eventDate.append("N/A")
        try:
            category=detail.find('div', class_='mt-1 text-muted').text
        except:
            category.append("N/A")
        information=soup.find('table', class_='table noBorder mng w-100 trBorder')
        try:
            turnout=information.find('a', class_='text-decoration-none').text
        except:
            turnout.append("N/A")
        try:
            labels=information.find(id='hvrout2').text
        except:
            labels.append("N/A")
        location=soup.find('div', class_='row fs-14 box p-0')
        try:
            latitude=location.find('span', id='event_latitude').text
        except:
            latitude.append("N/A")
        try:
            longitude=location.find('span', id='event_longude').text
        except:
            longitude.append("N/A")
        try:
            venue=location.find('div', class_='mb-1').text
        except:
            venue.append("N/A")

        event_details_df=event_details_df.append({'Search date': currentDate, 'Event title': name, 'Date': eventDate,'Category': category, 'Labels': labels, 'Turnout': turnout, 'Latitude': latitude, 'Longitude': longitude, 'Address': venue, 'Event link': event_link}, ignore_index=True)    

        df2=event_details_df
        df2=df2.drop_duplicates()
        df2=df2.replace('N/A',np.NaN)
        df2['Address']=df2['Address'].str.slice(start=2)
        df2= df2.dropna(subset = ['Event title', 'Date'])
        df2['Address'] = df2['Address'].str.replace("nue", "Venue")
        df2['Date'] = df2['Date'].str.replace('LIVE', '')
        df2 = df2.reset_index(drop=True)
        df2['Date']=df2['Date'].str.replace("Add a Review","")
        df2['Category']=df2['Category'].str.replace("Add a Review","")
        df2['Date']=df2['Date'].str.replace("Add a review","")
        df2['Category']=df2['Category'].str.replace("Add a review","")
        df2['Category']=df2['Category'].str.replace("Ratings","")
        df2['Category']=df2['Category'].str.replace("Rating","")
        df2['Category']= df2['Category'].str.replace(r'•', '', regex=True).str.replace(r'.', '', regex=True)
        df2['Category']= df2['Category'].str.replace(r'\[.*?\]|\(.*?\)|\d+', '', regex=True)
        #clean date column and define start date and end date 
        dateRange= pd.DataFrame(df2['Date'].str.split('-',1).to_list(),columns = ['Start date','End date'])
        dateRange['End date'].fillna(dateRange['Start date'], inplace=True)
        dateRange[['End day','End month','End year']] = dateRange['End date'].str.extract(r"^(.*)\s+(\S+)\s+(\d+)$", expand=True)
        dateRange[['Start day','Start month','Start year']] = dateRange['Start date'].str.extract(r"^(.*)\s+(\S+)*\s+(\d+)*$", expand=True)
        dateRange['Start day'].fillna(dateRange['Start date'], inplace=True)
        dateRange['Start month'].fillna(dateRange['End month'], inplace=True)
        dateRange['Start year'].fillna(dateRange['End year'], inplace=True)
        dateRange['Start']=dateRange['Start day'].astype(str)+ " "+ dateRange['Start month'].astype(str)+" "+ dateRange['Start year'].astype(str)
        dateRange['End']=dateRange['End day'].astype(str)+" "+dateRange['End month'].astype(str)+" "+dateRange['End year'].astype(str)
        df2['Start date']= pd.to_datetime(dateRange['Start'])
        df2['End date']= pd.to_datetime(dateRange['End'])
        del df2['Date']  

        df2['Labels']=df2['Labels'].str.replace("IT","Information Technology")
        df2['Labels']=df2['Labels'].str.replace("Category & Type","")
        df2['Labels']=df2['Labels'].str.replace('Conference', '')
        df2['Labels']=df2['Labels'].str.replace('Trade Show', '')
        df2['Labels'] = df2['Labels'].replace(r"(\w)([A-Z])", r"\1 | \2", regex=True)
        df2= df2.assign(Labels=df2['Labels'].str.split('|')).explode('Labels')
        df2.reset_index(inplace=True)
        df2.index = np.arange(1, len(df2) + 1)
        df2['Turnout']=df2['Turnout'].str.replace("&","").replace("([A-Z][a-z]+)", "").replace("IT", "")
        df2['Turnout']=df2['Turnout'].str.replace("([A-Z][a-z]+)", "")
        df2['Turnout']=df2['Turnout'].str.replace("IT", "")
        df2['Turnout']=df2['Turnout'].replace(r'^\s*$', np.nan, regex=True)
        df2 = df2.rename(columns = {'index':'Event ID'})
        currentDate = datetime.now().strftime("%m-%d-%Y")
        
        def upload_s3(df,i):
            s3 = boto3.client("s3",
            aws_access_key_id="#####################",  # Security Key of IAM User
            aws_secret_access_key="#############################" # Secret Access Key of IAM User
            )
            csv_buf = StringIO()
            df.to_csv(csv_buf, header=True, index=False)
            csv_buf.seek(0)    
            s3.put_object(Bucket="rezgcorp-data-science", Body=csv_buf.getvalue(), Key='uploads/'+i)


        upload_s3(df2, f"Events_in_{destinationID}_10TimesSite_{currentDate}.csv")
