
# coding: utf-8

# In[1]:

""" Ingest MAPSPAM 2010 data into earthengine
-------------------------------------------------------------------------------

Author: Rutger Hofste
Date: 20190617
Kernel: python36
Docker: rutgerhofste/gisdocker:ubuntu16.04

"""

SCRIPT_NAME = "Y2019M06D17_RH_Ingest_MAPSPAM_EE_V01"
OUTPUT_VERSION = 1

GCS_BUCKET = "aqueduct30_v01"
PREFIX = "Y2019M06D17_RH_MAPSPAM_V01"


# In[ ]:




# In[ ]:




# In[2]:

import os
from google.cloud import storage


# In[4]:

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/.google.json"


# In[20]:

client = storage.Client()


# In[21]:

bucket = client.get_bucket(GCS_BUCKET)


# In[28]:

blobs = bucket.list_blobs(prefix=PREFIX)


# In[29]:

blobs = list(blobs)


# In[30]:

for blob in blobs:
    filename = blob.name.split("/")[-1]
    base, extension = filename.split(".")
    if extension == "tif":
        print(filename)
        #"gs://{}".format(blob.name)
    else:
        pass


# In[18]:

extension


# In[9]:

blob.name


# In[ ]:




# In[ ]:




# In[ ]:

def get_parameters(blob):
    """
    Obtain a dictionary and google storage URL fromt the blob name. 
    The script will skip files other than .tif
    
    For a list of parameters, see the MAPSPAM metadata.
    https://s3.amazonaws.com/mapspam/2010/v1.0/ReadMe_v1r0_Global.txt
    
    Args:
        blob(google.cloud.storage.blob.Blob): Blob
    Returns:
        gcs_url(string) : string that can be used with earthengine upload. gs://...
        params(dictionary): dictionary with parameters contained in filename. 
    
    """


# In[ ]:

blob.name


# In[ ]:



