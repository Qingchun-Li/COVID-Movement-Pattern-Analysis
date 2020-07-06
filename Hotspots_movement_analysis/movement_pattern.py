# Liam Bessell, 3/31/20
# For Weekly-Pattern Data

# Library imports
import os
import sys
import pandas as pd
import pickle

# Globals
patternsDataDir = "./v1/main-file/"                                                                                                     # Directory for pattern data files             
patternsDataFiles = os.listdir(patternsDataDir)                                                                                         # Pattern data file names
coreDataDir = "./v1/core/"                                                                                                              # Directory for core data files
coreDataFiles = os.listdir(coreDataDir)                                                                                                 # Core data file names
geocodedPatternsDataDir = "./v1/main-file/geocoded/"                                                                                    # Directory for geocoded pattern data files
geocodedPatternsDataFiles = os.listdir(geocodedPatternsDataDir)                                                                         # Geocoded pattern data file names
cities = {
    "Houston": "TX", "Seattle": "WA", 
    "New York": "NY", "Albany": "NY",
    "San Francisco": "CA", "San Diego": "CA",
    "Detroit": "MI", "Randolph": "GA", 
    "Terrell": "GA", "Early": "GA", 
    "Essex": "NJ", "Westchester": "NY",
    "Nassau": "NY", "Decatur": "IN",
    "Hudson": "NJ", "Bergen": "NJ",
	"Mitchell": "GA", "St. John the Baptist": "LA",
    "Union": "NJ", "Austin": "TX",
    "Charlotte": "NC", "Dallas": "TX",
    "Los Angeles": "CA", "San Diego": "CA",
	"San Jose": "CA", "Chicago": "IL",
	"Fort Worth": "TX", "Jacksonville": "FL",
	"Philadelphia": "PA", "Phoenix": "AZ",
	"San Antonio": "TX", "Boston": "MA",
	"Baltimore": "MD", "Olympia": "WA",
	"Denver": "CO", "Portland": "OR",
	"Tacoma": "WA", "Spokane": "WA",
	"Everett": "WA", "Sacramento": "CA",
	"Milwaukee": "WI", "Cleveland": "OH",
	"San Antonio": "TX", "Dougherty": "GA",
	"Toole": "MT"}                                                        
CHUNK_SIZE = 2**9                                                                                                                       # Chunk size, i.e. how many rows of .csv file to read at a time                                                                                                   

# Acronyms
# cbg --> census block group

def main():
    # Define city
    city = "nil"
    if len(sys.argv) < 2:
        city = input("Enter a city from the list {0}: ".format(cities))
        while not city in cities.keys():
            print("Error: City not in list...")
            city = input("Enter a city from the list {0}: ".format(cities))
    else:
        city = sys.argv[1]
        while not city in cities.keys():
            print("Error: City not in list")
            city = input("Enter a city from the list {0}: ".format(cities))

    # Define region
    region = "nil"
    if len(sys.argv) < 3:
        region = input("Enter a region from the list {0}: ".format(cities))
        while not region in cities.values():
            print("Error: region not in list...")
            region = input("Enter a region from the list {0}: ".format(cities))
    else:
        region = sys.argv[2]
        while not region in cities.values():
            print("Error: region not in list")
            region = input("Enter a region from the list {0}: ".format(cities))

    # Geocode the data?
    bGeocode = False
    if len(sys.argv) < 4:
        bGeocode = True if input("Geocode the data? (Y/N): ") in ["Y", "y", "Yes", "yes", "True", "true", "Geocode", "geocode"] else False
    else:
        bGeocode = True if sys.argv[3] in ["Y", "y", "Yes", "yes", "True", "true", "Geocode", "geocode"] else False
    
    # Get .csv and .pkl files
    patternsCsvFiles = []
    patternsPklFiles = []
    for fileName in patternsDataFiles:
        if fileName.endswith(".csv.gz"):
            patternsCsvFiles.append(fileName)
        elif fileName.endswith(".pkl"):
            patternsPklFiles.append(fileName)
    coreCsvFiles = []
    corePklFiles = []
    for fileName in coreDataFiles:
        if fileName.endswith(".csv.gz"):
            coreCsvFiles.append(fileName)
        elif fileName.endswith(".pkl"):
            corePklFiles.append(fileName)

    # Read in pattern data
    patternsFrames = []                                                                                                            # Data frames
    for fileName in patternsCsvFiles:
        # File string without the file extension
        patternsDataName = fileName[:26]
        
        # Is there a .pkl file for this week?
        bPklFile = patternsDataName + "-" + city + ".pkl" in patternsPklFiles

        # If no .pkl file, then read the .csv file, else read the .pkl file
        if not bPklFile:
            df = pd.DataFrame()
            # Read the .csv file in chunks in order to preserve memory
            for chunk in pd.read_csv(patternsDataDir + patternsDataName + ".csv.gz", chunksize=CHUNK_SIZE, engine="python"):     # Changing engine to c will increase performance for large files, but has limited support
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
  
    # Pickle the core data files if not done already
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

    # Geocode the data frames
    dfCoreComplete = pd.concat(coreFrames, ignore_index=True)
    for fileName in patternsPklFiles:
        # File string without the file extension
        patternFileName = fileName[:26]

        bGeocodedFile = patternFileName + "-" + city + "-geocoded.pkl" in geocodedPatternsDataFiles

        if not bGeocodedFile:
            dfGeocodedPattern = pd.read_pickle(patternsDataDir + fileName)
            dfGeocodedPattern = pd.merge(dfGeocodedPattern, dfCoreComplete[["safegraph_place_id", "latitude", "longitude"]], on="safegraph_place_id", how="left")
            dfGeocodedPattern.to_pickle(geocodedPatternsDataDir + patternFileName + "-" + city + "-geocoded.pkl")

    return 0

if __name__ == "__main__":
    main()
