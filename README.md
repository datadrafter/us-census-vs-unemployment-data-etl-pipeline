<This is the Basic example of an ETL pipeline>

1) requirements.txt
""" 
to install all packages required for our pipeline
"""

2) data folder
Holds two different types of data
-> population data
   downloaded from : https://www2.census.gov/programs-surveys/popest/technical-documentation/file-layouts/2020-2021/CBSA-EST2021-ALLDATA.pdf
-> unemployment data 
   downloaded from : https://www.ers.usda.gov/data-products/county-level-data-sets/download-data

3) db.sqlite file
    this is the output files containing our final table of all the attributes required for our study
    This file will get generated upon successful execution of pipeline.py

4) pipeline.py
    this is the portion of code which holds two classes with individual functions
    first class contains three functs to extract, transform and load data
    and second function contains Create table statements for two output tables with proper datatypes and then passing in bunch of print statements for pretty'ing the output process