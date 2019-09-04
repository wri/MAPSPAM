
# coding: utf-8

# In[1]:

""" Ingest MAPSPAM 2010 data into earthengine
-------------------------------------------------------------------------------

Author: Rutger Hofste
Date: 20190624
Kernel: python36
Docker: rutgerhofste/gisdocker:ubuntu16.04

for all techs and variables except value of production, aggregate all crops.



"""
TESTING = 0

SCRIPT_NAME = "Y2019M06D24_RH_Aggregate_Crops_Mapspam_EE_V01"
OUTPUT_VERSION = 1

EE_INPUT_PATH = "projects/WRI-Aquaduct/Y2019M06D17_RH_Ingest_MAPSPAM_EE_V01/output_V02/"
EE_OUTPUT_PATH = "projects/WRI-Aquaduct/{}/output_V{:02d}".format(SCRIPT_NAME,OUTPUT_VERSION)

URL_STRUCTURES = "https://raw.githubusercontent.com/wri/MAPSPAM/master/metadata_tables/structure.csv"
URL_TECHS = "https://raw.githubusercontent.com/wri/MAPSPAM/master/metadata_tables/technologies.csv"
URL_CROPS = "https://raw.githubusercontent.com/wri/MAPSPAM/master/metadata_tables/mapspam_names.csv"
URL_UNITS = "https://raw.githubusercontent.com/wri/MAPSPAM/master/metadata_tables/units.csv"
URL_STRUCTURE_B = "https://raw.githubusercontent.com/wri/MAPSPAM/master/metadata_tables/structure_b.csv"

X_DIMENSION_5MIN = 4320
Y_DIMENSION_5MIN = 2160
Y_DIMENSION_5MIN_NOPOLAR = 2148 # was (21600)

xScale = 360/X_DIMENSION_5MIN
xShearing = 0
xTranslation = -180
yShearing = 0
yScale = -179/Y_DIMENSION_5MIN_NOPOLAR
yTranslation = 89.5

CRS_TRANSFORM_5MIN_NOPOLAR = [xScale, xShearing, xTranslation, yShearing, yScale, yTranslation]

EXTRA_PARAMS = {"script_name":SCRIPT_NAME,
                "output_version":OUTPUT_VERSION,
                "ingested_by":"Rutger Hofste"}

CRS = "EPSG:4326"


# In[2]:

import time, datetime, sys
dateString = time.strftime("Y%YM%mD%d")
timeString = time.strftime("UTC %H:%M")
start = datetime.datetime.now()
print(dateString,timeString)
sys.version


# In[3]:

import ee
import os
import subprocess
import pandas as pd
ee.Initialize()


# In[4]:

#command = "/opt/anaconda3/envs/python35/bin/earthengine asset set -p metadata='https://github.com/wri/MAPSPAM' projects/WRI-Aquaduct/Y2019M06D24_RH_Aggregate_Crops_Mapspam_EE_V01/output_V01/mapspam2010v1r0"


# In[5]:

global_region = ee.Geometry.Polygon(coords=[[-180.0, -90.0], [180,  -90.0], [180, 90], [-180,90]], proj= ee.Projection('EPSG:4326'),geodesic=False )


# In[6]:

global_region_client = global_region.getInfo()['coordinates']


# In[7]:

dimensions_5min_nopolar = "{}x{}".format(X_DIMENSION_5MIN,Y_DIMENSION_5MIN_NOPOLAR)


# In[8]:

df_structures = pd.read_csv(URL_STRUCTURES)
df_crops = pd.read_csv(URL_CROPS)
df_techs = pd.read_csv(URL_TECHS)
df_units = pd.read_csv(URL_UNITS)
df_structure_b = pd.read_csv(URL_STRUCTURE_B)


# In[9]:

ic = ee.ImageCollection("projects/WRI-Aquaduct/Y2019M06D17_RH_Ingest_MAPSPAM_EE_V01/output_V02/mapspam2010v1r0");
ic = ic.filterMetadata("variable","not_equals","value-of-production")

crop_names = ic.distinct(["crop_name"]).aggregate_array("crop_name").getInfo()
techs = ic.distinct(["technology_short"]).aggregate_array("technology_short").getInfo()
variables = ic.distinct(["variable"]).aggregate_array("variable").getInfo()


# In[10]:

if TESTING:
    techs  = techs[0:2]
    variables = variables[0:1]


# In[11]:

for variable in variables:
    for tech in techs:
        print(variable,tech)
        ic_filter = ic.filterMetadata("technology_short","equals",tech)                       .filterMetadata("variable","equals",variable)
        reducer = ee.Reducer.sum()
        print(ic_filter.size().getInfo())
        i_sum = ic_filter.reduce(reducer)
        
        i_meta = ee.Image(ic_filter.first())
        
        i_sum = i_sum.copyProperties(source=i_meta,
                                     exclude=["crop_color",
                                              "crop_group",
                                              "crop_group_color",
                                              "crop_name",
                                              "crop_type",
                                              "crop_number",
                                              "extent"])
        i_sum = ee.Image(i_sum)
        i_sum = i_sum.set({"crop_name":"all crops",
                           "crop_name_short":"all",
                           "extent":"global_nopolar"})
        i_sum = i_sum.set(EXTRA_PARAMS)
        
        description = "spam2010v1r0_globalnopolar_{}_all_{}".format(variable,tech)
        asset_id = "{}/mapspam2010v1r0/{}".format(EE_OUTPUT_PATH,description)
        
        
        task = ee.batch.Export.image.toAsset(
                image =  ee.Image(i_sum),
                description = description,
                assetId = asset_id,
                crs = CRS,
                crsTransform = CRS_TRANSFORM_5MIN_NOPOLAR,
                #region= global_region_client,
                dimensions=dimensions_5min_nopolar,
                maxPixels = 1e10   
        )
        task.start()
        


# In[12]:

end = datetime.datetime.now()
elapsed = end - start
print(elapsed)


# previous run:  
# 0:01:02.898908
# 
# 

# In[ ]:



