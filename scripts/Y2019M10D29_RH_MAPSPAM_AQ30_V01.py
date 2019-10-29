
# coding: utf-8

# In[1]:

""" Join Aqueduct 30 data to MAPSPAM and upload to gbq.
-------------------------------------------------------------------------------

Sample aqueduct 30 raster data using the centroids of the mapspam cells.



Author: Rutger Hofste
Date: 20191029
Kernel: python35
Docker: rutgerhofste/gisdocker:ubuntu16.04

"""

TESTING = 0

SCRIPT_NAME = "Y2019M10D29_RH_MAPSPAM_AQ30_V01"
OUTPUT_VERSION = 3

BQ_PROJECT_ID = "aqueduct30"
BQ_INPUT_DATASET = "MAPSPAM_2010v1r0"
BQ_INPUT_TABLE_NAME = "output_v05"

BQ_OUTPUT_DATASET = "MAPSPAM_aqueduct"

GCS_INPUT_PATH_AQ30 = "gs://aqueduct30_v01/Y2019M05D21_RH_AQ30VS21_Rasterize_AQ30_EE_V01/output_V06"

ec2_input_path = "/volumes/data/{}/input_V{:02.0f}".format(SCRIPT_NAME,OUTPUT_VERSION)
ec2_output_path = "/volumes/data/{}/output_V{:02.0f}".format(SCRIPT_NAME,OUTPUT_VERSION)

s3_output_path = "s3://wri-projects/Aqueduct30/processData/{}/output_V{:02.0f}/".format(SCRIPT_NAME,OUTPUT_VERSION)


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
import rasterio
from google.cloud import bigquery


# In[4]:

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/.google.json"
os.environ["GOOGLE_CLOUD_PROJECT"] = "aqueduct30"
client = bigquery.Client(project=BQ_PROJECT_ID)


# In[5]:

get_ipython().system('rm -r {ec2_input_path}')
get_ipython().system('rm -r {ec2_output_path}')


# In[6]:

os.makedirs(ec2_input_path,exist_ok=True)
os.makedirs(ec2_output_path,exist_ok=True)


# In[7]:

get_ipython().system('gsutil -m cp -r {GCS_INPUT_PATH_AQ30}/* {ec2_input_path}')


# In[8]:

files = os.listdir(ec2_input_path)


# In[9]:

files


# In[10]:

sql = 'SELECT cell5m, x, y  FROM `{}.{}.{}` WHERE rec_type = "A" AND tech_type = "A"'.format(BQ_PROJECT_ID,BQ_INPUT_DATASET,BQ_INPUT_TABLE_NAME)


# In[11]:

print(sql)


# In[12]:

df  = pd.read_gbq(query=sql,dialect="standard")


# In[13]:

df.shape


# In[14]:

coords = [(x,y) for x, y in zip(df.x, df.y)]


# In[15]:

def split_filename(filename):
    base, extension = filename.split(".")
    indicator, rest = base.split("_")
    return indicator, base, extension


# In[16]:

for fname in files:
    print(fname)
    indicator, base, extension  = split_filename(fname)
    input_path = "{}/{}".format(ec2_input_path,fname)
    src = rasterio.open(input_path)
    df[base] = [x for x in src.sample(coords)]
    df[base] = df[base].apply(lambda x: x[0])


# In[17]:

output_filename = "Y2019M10D29_RH_MAPSPAM_AQ30_V01"


# In[18]:

output_path = "{}/{}.csv".format(ec2_output_path,output_filename)


# In[19]:

df.to_csv(output_path,encoding="UTF-8")


# In[20]:

get_ipython().system('aws s3 cp {ec2_output_path} {s3_output_path} --recursive')


# In[21]:

destination_table = "{}.{}".format(BQ_OUTPUT_DATASET,"{}_V{:02.0f}".format(SCRIPT_NAME,OUTPUT_VERSION))


# In[25]:

df.to_gbq(destination_table=destination_table,
          project_id=BQ_PROJECT_ID,
          chunksize=100000,
          if_exists="replace")


# In[26]:

end = datetime.datetime.now()
elapsed = end - start
print(elapsed)


# 0:25:21.794033

# In[ ]:



