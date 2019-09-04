
# coding: utf-8

# In[1]:

""" Ingest MAPSPAM 2010 data into earthengine
-------------------------------------------------------------------------------

Author: Rutger Hofste
Date: 20190617
Kernel: python36
Docker: rutgerhofste/gisdocker:ubuntu16.04

"""
TESTING = 0

SCRIPT_NAME = "Y2019M06D17_RH_Ingest_MAPSPAM_EE_V01"
OUTPUT_VERSION = 2

NODATA_VALUE = -1

GCS_BUCKET = "aqueduct30_v01"
PREFIX = "Y2019M06D17_RH_MAPSPAM_V01"

EE_OUTPUT_PATH = "projects/WRI-Aquaduct/Y2019M06D17_RH_Ingest_MAPSPAM_EE_V01"

URL_STRUCTURES = "https://raw.githubusercontent.com/wri/MAPSPAM/master/metadata_tables/structure.csv"
URL_TECHS = "https://raw.githubusercontent.com/wri/MAPSPAM/master/metadata_tables/technologies.csv"
URL_CROPS = "https://raw.githubusercontent.com/wri/MAPSPAM/master/metadata_tables/mapspam_names.csv"
URL_UNITS = "https://raw.githubusercontent.com/wri/MAPSPAM/master/metadata_tables/units.csv"
URL_STRUCTURE_B = "https://raw.githubusercontent.com/wri/MAPSPAM/master/metadata_tables/structure_b.csv"


EXTRA_PARAMS = {"script_name":SCRIPT_NAME,
                "output_version":OUTPUT_VERSION,
                "ingested_by":"Rutger Hofste"}


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
from google.cloud import storage


# Create imageCollection manually using UI. 
# Add link to GitHub using the following command. (replace command as needed)

# In[4]:

#command = "/opt/anaconda3/envs/python35/bin/earthengine asset set -p metadata='https://github.com/wri/MAPSPAM' projects/WRI-Aquaduct/Y2019M06D17_RH_Ingest_MAPSPAM_EE_V01/output_V01/mapspam2010v1r0"


# In[5]:

#subprocess.check_output(command,shell=True)


# In[6]:

df_structures = pd.read_csv(URL_STRUCTURES)
df_crops = pd.read_csv(URL_CROPS)
df_techs = pd.read_csv(URL_TECHS)
df_units = pd.read_csv(URL_UNITS)
df_structure_b = pd.read_csv(URL_STRUCTURE_B)


# In[7]:

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/.google.json"


# In[8]:

client = storage.Client()


# In[9]:

bucket = client.get_bucket(GCS_BUCKET)


# In[10]:

blobs = bucket.list_blobs(prefix=PREFIX)


# In[11]:

blobs = list(blobs)


# In[12]:

if TESTING:
    blobs = blobs[0:3]


# In[13]:

df_units


# In[14]:

def get_structure(variable):
    structure = df_structures.loc[df_structures["variable"]==variable]["structure"].iloc[0]
    return structure


# In[15]:

def get_parameters_structure_a(base):
    """
    Obtain a dictionary from the filename using structure a.
    
    `spam_version, extent, variable, mapspam_cropname, technology`
    
    The structure depends on the variable, see mapspam metadata:
    https://s3.amazonaws.com/mapspam/2010/v1.0/ReadMe_v1r0_Global.txt
    
    Args:
        base(string): the file basename.
    Returns:
        params(dictionary): dictionary with parameters contained in filename. 
    
    """
    
    structure = "a"
    spam_version, extent, variable, mapspam_cropname, technology  = base.split("_")
    
    crop_name = df_crops.loc[df_crops["SPAM_name"]==mapspam_cropname]["name"].iloc[0]
    crop_type = df_crops.loc[df_crops["SPAM_name"]==mapspam_cropname]["type"].iloc[0]
    crop_number = df_crops.loc[df_crops["SPAM_name"]==mapspam_cropname]["crop_number"].iloc[0]
    
    # added later
    crop_group = df_crops.loc[df_crops["SPAM_name"]==mapspam_cropname]["crop_group"].iloc[0]
    crop_color = df_crops.loc[df_crops["SPAM_name"]==mapspam_cropname]["crop_color"].iloc[0]
    crop_group_color = df_crops.loc[df_crops["SPAM_name"]==mapspam_cropname]["crop_group_color"].iloc[0]
    
    technology_full = df_techs.loc[df_techs["technology"]==technology]["technology_full"].iloc[0]
    unit = df_units.loc[df_units["variable"]==variable]["unit"].iloc[0]

    params = {"structure":structure,
              "spam_version":spam_version,
              "extent":extent,
              "variable":variable,
              "crop_name_short":mapspam_cropname,
              "technology_short":technology,
              "technology_full":technology_full,
              "crop_name":crop_name,
              "crop_type":crop_type,
              "crop_number":crop_number,
              "crop_group":crop_group,
              "crop_color":crop_color,
              "crop_group_color":crop_group_color,              
              "unit":unit}

    params = {**params , **EXTRA_PARAMS}
    return params


# In[16]:

def get_parameters_structure_b(base):
    """
    Obtain a dictionary from the filename using structure b.
    
    `spam_version, extent, variable, technology`
    
    The structure depends on the variable, see mapspam metadata:
    https://s3.amazonaws.com/mapspam/2010/v1.0/ReadMe_v1r0_Global.txt
    
    Args:
        base(string): the file basename.
    Returns:
        params(dictionary): dictionary with parameters contained in filename. 
    
    """
    structure = "b"
    components  = base.split("_")
    spam_version = components[0]
    extent = components[1]
    variable = components[2]
    technology_list = components[3:]
    technology = "_".join(technology_list[:-1])
    technology_full = df_structure_b.loc[df_structure_b["technology_base"]==technology]["description"].iloc[0]
    unit = df_units.loc[df_units["variable"]==variable]["unit"].iloc[0]
    
    params = {"structure":structure,
              "spam_version":spam_version,
              "extent":extent,
              "variable":variable,
              "technology_short":technology,
              "technology_full":technology_full,
              "unit":unit
             }
    
    params = {**params , **EXTRA_PARAMS}
    
    return params


# In[17]:

def dictionary_to_EE_upload_command(d):
    """ Convert a dictionary to command that can be appended to upload command
    -------------------------------------------------------------------------------
     
    
    Args:
        d (dictionary) : Dictionary with metadata. nodata_value                         
    
    Returns:
        command (string) : string to append to upload string.    
    
    """
    command = ""
    for key, value in d.items():            
        if key == "nodata_value":
            command = command + " --nodata_value={}".format(value)
        else:
            
            if isinstance(value, str):
                command = command + " -p '(string){}={}'".format(key,value)
            else:
                command = command + " -p '(number){}={}'".format(key,value)
            
            

    return command


# In[18]:

for blob in blobs:
    filename = blob.name.split("/")[-1]
    base, extension = filename.split(".")
    
    if extension == "tif":
        components = base.split("_")
        variable = components[2]  
        structure = get_structure(variable)
        
        if structure == "a":
            params = get_parameters_structure_a(base)
        elif structure == "b":
            params = get_parameters_structure_b(base)  
        else:
            break
        
        params["nodata_value"] = NODATA_VALUE
        meta_command = dictionary_to_EE_upload_command(params) 
        
        output_ic_name = "mapspam2010v1r0"
        image_name = base
        destination_path = "{}/output_V{:02d}/{}/{}".format(EE_OUTPUT_PATH,OUTPUT_VERSION,output_ic_name,image_name)
        source_path = "gs://aqueduct30_v01/{}".format(blob.name)
        
        command = "/opt/anaconda3/envs/python35/bin/earthengine upload image --asset_id={} {} {}".format(destination_path,meta_command,source_path)
        print(command)
        subprocess.check_output(command,shell=True)
        
    else:
        print("skipping file",filename)


# In[19]:

end = datetime.datetime.now()
elapsed = end - start
print(elapsed)


# previous run:  
# 0:52:23.953127  
# 0:53:01.081204
# 
# 
# 

# In[ ]:



