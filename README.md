# MapSPAM
Process MAPSPAM 2010v1 data and make available in additional formats.  

http://mapspam.info/  

MapSPAM variables and parameter structure:

**variable**|**parameter\_structure**
:-----:|:-----:
physical-area|a
harvested-area|a
production|a
yield|a
value-of-production|b

parameter structure a (and options)
**spam\_version**|**spam2010v1r0**
:-----:|:-----:
extent|global
variable|see above









## Formats

1. EarthEngine  
`path = projects/WRI-Aquaduct/Y2019M06D17_RH_Ingest_MAPSPAM_EE_V01/output_V01/mapspam2010v1r0`  


Find unique property values using:
`var icUniques = ic.distinct(["property"]).aggregate_array("property")`


