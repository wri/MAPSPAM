# MapSPAM
Process MAPSPAM 2010v1 data and make available in additional formats.  

http://mapspam.info/  
    
[original metadata](https://s3.amazonaws.com/mapspam/2010/v1.0/ReadMe_v1r0_Global.txt)  


Spatial resolution: 5 arc minute  
Nodata value: -1  
Data type: float32  

MapSPAM variables and parameter structure:

**variable**|**variable_short**|**parameter\_structure**|**unit**|
:-----:|:-----:|:-----:|:-----:|
physical-area|A|a|ha
harvested-area|H|a|ha
production|P|a|mt
yield|Y|a|kg/ha
value-of-production|V_agg|b|I$

parameter structure **a** (and options)  

| parameter        | options                                                                              |
|------------------|--------------------------------------------------------------------------------------|
| spam_version     | spam2010v1r0                                                                         |
| extent           | global                                                                               |
| variable         | see above                                                                            |
| mapspam_cropname | [link](https://github.com/wri/MAPSPAM/blob/master/metadata_tables/mapspam_names.csv) |
| technology       | [link](https://github.com/wri/MAPSPAM/blob/master/metadata_tables/technologies.csv)  |


parameter structure **b** (and options)

| parameter        | options                                                                              |
|------------------|--------------------------------------------------------------------------------------|
| spam_version     | spam2010v1r0                                                                         |
| extent           | global                                                                               |
| variable         | see above                                                                            |
| technology       | [link](https://github.com/wri/MAPSPAM/blob/master/metadata_tables/structure_b.csv)   |






## Formats

1. EarthEngine  
`path = projects/WRI-Aquaduct/Y2019M06D17_RH_Ingest_MAPSPAM_EE_V01/output_V01/mapspam2010v1r0`  


## Notes:

Find unique property values using:
`var icUniques = ic.distinct(["property"]).aggregate_array("property")`

https://code.earthengine.google.com/6a451f98e9f6b9d0e6c031bd70a0704b
