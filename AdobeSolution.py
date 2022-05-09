import pandas as pd
import tldextract
from pandas import Series
import numpy as np
from dateutil.relativedelta import relativedelta, MO
from datetime import datetime

class AdobeSolution:
    FileName=''
    #Initialize the class arguments
    def __init__(self, Filename):
        self.FileName = Filename
    
    # Function to Extract the Input File into a Dataframe    
    def ExtractDataFile(self):
        FileName=self.FileName
        hit_file_df=pd.read_csv(FileName,sep='\t',dtype=str)
        return (hit_file_df)
    
    # Function to clean up the data read 
    def DataCleanup(self,hit_file_df):

        #Filter to the columns we are interested in 
        hit_file_df=hit_file_df[['event_list','referrer','product_list']]
        
        # Consider only the records where purchase event has happended using the Event_list values
        # Purchase event is “1” and if that is set in “event_list” it means a purchase happened.
        #Fill nan's with blanks
        hit_file_df=hit_file_df.replace('NaN',np.nan) 
        hit_file_df.fillna('',inplace=True)
        
        #Spliting the event list in order to do exact match with Purchase event='1. Because this field is command seperated values
        hit_file_df['Event List Split']=hit_file_df['event_list'].str.replace(',',' ').str.strip()
        hit_file_df=hit_file_df[hit_file_df['Event List Split'].str.strip()=='1']
        
        #Extract the Search Engine Domain from the referrer column in the input file
        hit_file_df['Search Engine Domain']=hit_file_df['referrer'].apply(lambda x: '.'.join(tldextract.extract(x)[1:]))

        #Split the product list seperated by comma into different rows
        s = hit_file_df['product_list'].str.split(',').apply(Series, 1).stack()
        s.index = s.index.droplevel(-1) # to line up with df's index
        s.name = 'product_list' # needs a name to join
        del hit_file_df['product_list']
        hit_file_df=hit_file_df.join(s).reset_index(drop=True)

        #Split the product list attributes seperated by ; to capture the Revenue data
        # [Category];{Product Name];[Number of Items];[Total Revenue];[Custom Event]|[Custom
        # Event];[Merchandizing eVar],.

        hit_file_df[['Category','Product Name','Number of Items','Total Revenue','Custom Event']] = hit_file_df.product_list.str.split(';',expand=True).iloc[:,0:5]

        
        #Rename the Product Name as Search Keyword
        hit_file_df=hit_file_df.rename(columns={'Product Name':'Search Keyword','Total Revenue':'Revenue'})
        return hit_file_df
    
    #Generate the output file with Search Engine, Search Keyword and Revenue and Sorted by Revenus Desc
    def GenerateOutputFile(self,cleaned_df):  
        cleaned_df=cleaned_df.sort_values('Revenue',ascending=False)
        cleaned_df=cleaned_df[['Search Engine Domain','Search Keyword','Revenue']]
        
        #Print Today's Date
        datenow=pd.Timestamp.today().date().strftime('%Y-%m-%d')
        OutputFileName=str(datenow)+'_SearchKeywordPerformance.tab'
        cleaned_df.to_csv(OutputFileName,sep='\t',index=False)
                

# if __name__ == "__main__":
FileName='data.tsv'
AdobeS=AdobeSolution(FileName)
print("Reading the file",AdobeS.FileName)
hit_file_df=AdobeS.ExtractDataFile()

print("Cleaning the File started")
cleaned_df=AdobeS.DataCleanup(hit_file_df)
print("Done Cleaning the File",cleaned_df.shape)

print("Generating the output File Now")
AdobeS.GenerateOutputFile(cleaned_df)


        
    