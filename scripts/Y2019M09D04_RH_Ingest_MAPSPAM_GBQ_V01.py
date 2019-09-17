
# coding: utf-8

# In[1]:

""" Ingest Mapspam 2010 data into google bigquery.
-------------------------------------------------------------------------------

Ingest all variables except value of production to 
Google Bigquery. The script changes the column names for the crops to allow 
vertical stacking. 



Author: Rutger Hofste
Date: 20190904
Kernel: python36
Docker: rutgerhofste/gisdocker:ubuntu16.04

"""
TESTING = 0

SCRIPT_NAME = "Y2019M09D04_RH_Ingest_MAPSPAM_GBQ_V01"
OUTPUT_VERSION = 5

BQ_PROJECT_ID = "aqueduct30"
GCS_BUCKET = "aqueduct30_v01"
PREFIX = "Y2019M09D04_RH_Ingest_MAPSPAM_GBQ_V01"

MAPSPAM_CROPNAMES = "https://raw.githubusercontent.com/wri/MAPSPAM/master/metadata_tables/mapspam_names.csv"

gcs_input_path = "gs://{}/{}/input_V01".format(GCS_BUCKET,PREFIX)
gcs_output_path = "gs://{}/{}/output_V{:02.0f}/".format(GCS_BUCKET,PREFIX,OUTPUT_VERSION)
ec2_input_path = "/volumes/data/{}/input_V{:02.0f}".format(SCRIPT_NAME,OUTPUT_VERSION)
ec2_output_path = "/volumes/data/{}/output_V{:02.0f}".format(SCRIPT_NAME,OUTPUT_VERSION)

GBQ_OUTPUT_DATASET = "MAPSPAM_2010v10"


# Download all csv files except production (has different schema) from 
# http://mapspam.info/data/
# 
# Unzip and upload to Google Cloud Storage.
# 
# Rename:
# spam2010v1r0_global_yield.csv -> spam2010V1r0_global_yield.csv  
# spam2010v1r0_global_val_prod_agg.csv -> spam2010V1r0_global_val_prod_agg.csv

# In[2]:

import time, datetime, sys
dateString = time.strftime("Y%YM%mD%d")
timeString = time.strftime("UTC %H:%M")
start = datetime.datetime.now()
print(dateString,timeString)
sys.version


# In[3]:

import os
import subprocess
import pandas as pd
from tqdm import tqdm
from google.cloud import bigquery


# In[4]:

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/.google.json"
os.environ["GOOGLE_CLOUD_PROJECT"] = "aqueduct30"
client = bigquery.Client(project=BQ_PROJECT_ID)


# In[5]:

get_ipython().system('rm -r {ec2_input_path}')
get_ipython().system('rm -r {ec2_output_path}')
get_ipython().system('mkdir -p {ec2_input_path}')
get_ipython().system('mkdir -p {ec2_output_path}')


# In[6]:

get_ipython().system('gsutil -m cp -r {gcs_input_path}/* {ec2_input_path}')


# In[7]:

variable_dict = {"yield":{"variable_short":"yield","shorthand":"Y"},
                 "production":{"variable_short":"prod","shorthand":"P"},
                 "harvested_area":{"variable_short":"harv_area","shorthand":"H"},
                 "physical_area":{"variable_short":"phys_area","shorthand":"A"}}
                 #"value_of_production":{"variable_short":"val_prod_agg","shorthand":"V_agg"}}

technologies =  ["A","I","H","L","S","R"] # see metadata


# In[8]:

def load_df(variable_short,technology):
    folder_name = "spam2010V1r0_global_{}.csv".format(variable_short) 
    filename = "spam2010V1r0_global_{}_T{}.csv".format(shorthand,technology)
    input_path = "{}/{}/{}".format(ec2_input_path,folder_name,filename)
    df_raw = pd.read_csv(input_path,encoding="iso-8859-1")
    if TESTING:
        df = df_raw[0:100]
    else:
        df = df_raw
    return df

def rename_crop_columns(df,technology):
    """
    The csv files in Mapspam have the technology in the column names. The technology
    is also stored in the column tech_type and therefore redundant. It prevents 
    vertically stacking the data.
    
    Args:
        df(dataframe): Dataframe with old crop columns.
        technology(string):technology.
    Returs:
        df_renamed: Dataframe with renames columns
    
    """
    
    
    df_cropnames = pd.read_csv(MAPSPAM_CROPNAMES)
    new_crop_names = list(df_cropnames["SPAM_name"])
    old_crop_names = list(map(lambda x: x+"_{}".format(technology.lower()), new_crop_names))
    dictje = dict(zip(old_crop_names, new_crop_names))
    df_renamed = df.rename(columns=dictje)
    
    return df_renamed


# In[9]:

for technology in tqdm(technologies):
    print(technology)
    for variable, values in variable_dict.items():
        print(variable)
        variable_short = values["variable_short"]
        shorthand = values["shorthand"]
        df = load_df(variable_short,technology)
        df_renamed = rename_crop_columns(df,technology)
        
        filename = "spam2010V1r0_global_{}_T{}.csv".format(shorthand,technology)
        output_path = "{}/{}".format(ec2_output_path,filename)
        
        df_renamed.to_csv(path_or_buf=output_path,
                          encoding="UTF-8")
        

        
        gbq_dataset_name = "MAPSPAM_2010v1r0"        
        table_name = technology
        destination_table= "{}.output_v{:02.0f}".format(gbq_dataset_name,OUTPUT_VERSION)
        
       
        df_renamed.to_gbq(project_id=BQ_PROJECT_ID,
                          destination_table=destination_table,
                          chunksize=100000,
                          if_exists="append")
                          
        


# In[10]:

get_ipython().system('gsutil -m cp -r {ec2_output_path} {gcs_output_path}')


# In[11]:

end = datetime.datetime.now()
elapsed = end - start
print(elapsed)


# previous run:  
# 1:43:49.833856
# 
