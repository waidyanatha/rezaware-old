''' 
    ONLINE TRAVEL AGENCY (OTA) DEPLOYMENT SETTINGS
    
    should be set to suppport the class libraries and the application
    to use the deplyment specific paramters for the OTA work loads
    
    See the respective sections: Implementation, Data Stores
'''

''' DATA STORES '''
#--data hosting root location or S3 Bucket/Object
#  default ../data/
data = "../data/"
#--scraper url and input parameter file
#  default: /data/hospitality/bookings/scraper/
scrape_inputs = "/data/hospitality/bookings/scraper/"
#--destination id file location
#  default: /data/hospitality/bookings/scraper/destinations
destinations = "/data/hospitality/bookings/scraper/destinations"
