# Liam Bessell, 5/19/20
# For Weekly-Pattern Data

# Library imports
import os
import sys
import pandas as pd
import pickle
import json

import csv
csv.field_size_limit(sys.maxsize)

os.chdir("/home/liam/docs/c19-response/")

# Globals

safegraphDataDir = "./data/main-file/v2/safegraph-files/"
safegraphDataFiles = os.listdir(safegraphDataDir)
patternsDataDir = "./data/main-file/v2/weekly-patterns/standard/"                                                                    # Directory for pattern data files             
patternsDataFiles = os.listdir(patternsDataDir)                                                                                      # Pattern data file names
coreDataDir = "./data/core/v2/"                                                                                                      # Directory for core data files
coreDataFiles = os.listdir(coreDataDir)                                                                                              # Core data file names
geocodedPatternsDataDir = "./data/main-file/v2/weekly-patterns/geocoded/"                                                            # Directory for geocoded pattern data files
geocodedPatternsDataFiles = os.listdir(geocodedPatternsDataDir)                                                                      # Geocoded pattern data file names
CHUNK_SIZE = 2**9                                                                                                                    # Chunk size, i.e. how many rows of .csv file to read at a time                                                                                                   

cityFile = "./data/core_cities.json"
cities = {}                                                

# Acronyms
# CBG: census block group

def main():
    # Load city list
    with open(cityFile) as f:
        cities = json.load(f)
    
    # Geocode the data?
    bGeocode = False
    if len(sys.argv) < 2:
        bGeocode = True if input("Geocode the data? (Y/N): ") in ["Y", "y", "Yes", "yes", "True", "true", "Geocode", "geocode"] else False
    else:
        bGeocode = True if sys.argv[1] in ["Y", "y", "Yes", "yes", "True", "true", "Geocode", "geocode"] else False

    for city in cities.keys():
		
        print(city)
        region = cities[city]

        ### Get .csv and .pkl files
        patternsCsvFiles = []
        for fileName in patternsDataFiles:
            if fileName.endswith(".csv.gz"):
                patternsCsvFiles.append(fileName)
        
        patternsPklFiles = []
        for fileName in patternsDataFiles:
            if fileName.endswith(".pkl"):
                patternsPklFiles.append(fileName)
        
        coreCsvFiles = []
        corePklFiles = []
        for fileName in coreDataFiles:
            if fileName.endswith(".csv.gz"):
                coreCsvFiles.append(fileName)
            elif fileName.endswith(".pkl"):
                corePklFiles.append(fileName)

        ### Read in pattern data
        patternsFrames = []
        print("---Starting Weekly Pattern Preprocessing---")
        ctr = 0
        for fileName in patternsCsvFiles:
            # File string without the file extension
            patternsDataName = fileName[:26]
            
            # Is there a .pkl file for this week?
            bPklFile = patternsDataName + "-" + city + ".pkl" in patternsPklFiles

            # If no .pkl file, then read the .csv file, else read the .pkl file
            if not bPklFile:
                df = pd.DataFrame()
                
                # Read the .csv file in chunks in order to preserve memory
                for chunk in pd.read_csv(safegraphDataDir + patternsDataName + ".csv.gz", chunksize=CHUNK_SIZE, engine="python"):     # Changing engine to c will increase performance for large files, but has limited support
                    # Only add the row(s) to the dataframe if the "city" column is the specified city
                    if df.size == 0:
                        df = pd.DataFrame(chunk[(chunk.city == city) & (chunk.region == cities[city])])                                # There are duplicate cities in the data, so we need to also check if it's in the correction region, i.e. state
                    else: 
                        df = df.append(chunk[(chunk.city == city) & (chunk.region == cities[city])])                                   # Might be a more efficient method than this
                
                if df.size > 0:
                    patternsFrames.append(df)
                    df.to_pickle(patternsDataDir + patternsDataName + "-" + city + ".pkl")                                           # Pickle the data frame for later use
                    patternsPklFiles.append(patternsDataName + "-" + city + ".pkl")
            
            else:
                df = pd.read_pickle(patternsDataDir + patternsDataName + "-" + city + ".pkl")                                        # Pickled data has already been parsed such that only the specified city's rows are in the data frame
                patternsFrames.append(df)

            ctr += 1
            print("{0}/{1}".format(ctr, len(patternsCsvFiles)))


        ### Pickle the core data files if not done already
        coreFrames = []                                                                                  
        for fileName in coreCsvFiles:
            # File string without the file extension
            coreDataName = fileName[:14]

            bPklFile = coreDataName + "-" + city + ".pkl" in corePklFiles
            
            if not bPklFile:
                df = pd.DataFrame()
                for chunk in pd.read_csv(coreDataDir + coreDataName + ".csv.gz", chunksize=CHUNK_SIZE, engine="python"):
                    if df.size == 0:
                        df = pd.DataFrame(chunk[(chunk.city == city) & (chunk.region == cities[city])])
                    else:
                        df = df.append(chunk[(chunk.city == city) & (chunk.region == cities[city])])
                
                if df.size > 0:
                    coreFrames.append(df)
                    df.to_pickle(coreDataDir + coreDataName + "-" + city + ".pkl")
                    corePklFiles.append(coreDataName + "-" + city + ".pkl")
            
            else:
                df = pd.read_pickle(coreDataDir + coreDataName + "-" + city + ".pkl")
                coreFrames.append(df)


        ### Geocode the data frames
        dfCoreComplete = pd.concat(coreFrames, ignore_index=True)
        print("---Starting Geocoding---")
        ctr = 0
        for fileName in patternsPklFiles:
            # File string without the file extension
            patternFileName = fileName[:26]

            bGeocodedFile = patternFileName + "-" + city + "-geocoded.pkl" in geocodedPatternsDataFiles

            if not bGeocodedFile:
                dfGeocodedPattern = pd.read_pickle(patternsDataDir + fileName)
                dfGeocodedPattern = pd.merge(dfGeocodedPattern, dfCoreComplete[["safegraph_place_id", "latitude", "longitude"]], on="safegraph_place_id", how="left")
                dfGeocodedPattern.to_pickle(geocodedPatternsDataDir + patternFileName + "-" + city + "-geocoded.pkl")

            ctr += 1
            print("{0}/{1}".format(ctr, len(patternsPklFiles)))

    return 0

if __name__ == "__main__":
    main()
