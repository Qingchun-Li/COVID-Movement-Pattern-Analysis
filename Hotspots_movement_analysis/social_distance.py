# Liam Bessell, 4/7/20
# For Social Distancing Data

# Library imports
import os
import sys
import pandas as pd
import numpy as np
import pickle
import keplergl
import geopandas as gpd

# Globals
cbgDir = "./v1/safegraph_open_census_data/"
cbgGeographicFile = cbgDir + "metadata/cbg_geographic_data.csv"
year = "2020"
geocodedDistancingDataDir = "./v1/social-distancing/geocoded/"                                                                              # Directory for geocoded pattern data files
geocodedDistancingDataFiles = os.listdir(geocodedDistancingDataDir)                                                                         # Geocoded pattern data file names
CHUNK_SIZE = 2**9                                                                                                                           # Chunk size, i.e. how many rows of .csv file to read at a time                      

def main():
    # Define month
    month = "nil"
    if len(sys.argv) < 2:
        month = input("Enter a month: ")
    else:
        month = sys.argv[1]

    # Load CBG Geography mapping
    dfCBGGeography = pd.read_csv(cbgGeographicFile)

    # Geocode data
    



if __name__ == "__main__":
    main()