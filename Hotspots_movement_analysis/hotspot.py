# Liam Bessell, TAMU Urban Resilience Lab, 4/28/20

# Library imports
import os
import pandas as pd
import geopandas as gpd
import numpy as np
import pickle
import json
import networkx as nx
from datetime import datetime

# Directory Changes
os.chdir("/home/liam/docs/c19-response/")

# Globals
patternsDataDir = "./v1/main-file/"                                             # Directory for pattern data files             
patternsDataFiles = os.listdir(patternsDataDir)                                 # Pattern data file names
geocodedPatternsDataDir = "./v1/main-file/geocoded/"                            # Directory for geocoded pattern data files
geocodedPatternsDataFiles = os.listdir(geocodedPatternsDataDir)                 # Geocoded pattern data file names
CHUNK_SIZE = 2**9                                                               # Chunk size, i.e. how many rows of .csv file to read at a time                                                                 
cbgGeographicFile = "./v1/safegraph_open_census_data/metadata/cbg_geographic_data.csv"
networkHotspotDataDir = "./v1/main-file/networks/hotspots/"
networkHotspotFiles = os.listdir(networkHotspotDataDir)
hotspotFile = "./notebooks/hotspots.json"
hotspotAlgorithmFile = "./algorithms/"

# Must run the whole notebook if you change the city
city = "Houston"

def writeNodeArr(nodeFluxList, file):
    for node in nodeFluxList:
        file.write("{0}\n".format(node))

def writeNodeFluxDict(nodeFluxDict, file):
    for key in nodeFluxDict.keys():
        file.write("{0}, {1}\n".format(key, nodeFluxDict[key]["flux"]))

### Hotspot Algorithm
# Hamedmoghadam et al
def findHotSpots(nodeFluxList, nodeFluxDict, results):
    """
    Input:
    - nodeFluxList: list of node identifiers (i.e. CBG Number for origin nodes and safe_graph_id for destination nodes) sorted via their flux
    - nodeFluxDict: dictionary of nodes (i.e. Origin or Destination nodes) from the graph
    Output:
    - Index of nodeFluxList, representing the separation point between hotspot and non-hotspot nodes
    """
    startTime = datetime.now()
    # Solving Equation (finding separation point, c)
    # Time Complexity: O(n^3) (no clear way around this, could try a memoization approach?)
    n = len(nodeFluxList)
    # for c in range(n):
    #     sum_1 = 0
    #     scalar_sum_1 = 1/(c+1)
    #     for i in range(c):
    #         q_i = nodeFluxDict[nodeFluxList[i]]["flux"]
    #         sum_1_inner = 0
    #         for k in range(c):
    #             q_k = nodeFluxDict[nodeFluxList[k]]["flux"]
    #             sum_1_inner += q_k

    #         sum_1 += abs(q_i - scalar_sum_1*sum_1_inner)

    #     sum_2 = 0
    #     scalar_sum_2 = 1/(n-c+1)
    #     for j in range(c+1, n):
    #         q_j = nodeFluxDict[nodeFluxList[j]]["flux"]
    #         sum_2_inner = 0
    #         for l in range(c+1, n):
    #             q_l = nodeFluxDict[nodeFluxList[l]]["flux"]
    #             sum_2_inner += q_l

    #         sum_2 += abs(q_j - scalar_sum_2*sum_2_inner)

    #     results.append(sum_1+sum_2)

    nodeArrFile = "nodeArr.txt"
    with open(nodeArrFile, "w+") as f:
        writeNodeArr(nodeFluxList, f)

    nodeFluxDictFile = "nodeFluxMap.txt"
    with open(nodeFluxDictFile, "w+") as f:
        writeNodeFluxDict(nodeFluxDict, f)

    print("Time elapsed: {0}".format(datetime.now() - startTime))
    # Destination Hot Spot separation point: argmin of the Solving Equation results list
    #return results.index(min(results))
    return 0

def loadJson(arg1):
    return json.loads(arg1)

def mapCBGNetwork(row):
    return_list = []
    for (key, value) in row["visitor_home_cbgs"].items():
        return_list.append([row["safegraph_place_id"], row["longitude"], row["latitude"], key, value])
    return return_list

def mapCBGCoordinates(row, dfCensus):
    return_list = row
    
    for censusRow, index in dfCensus.iterrows():
        if row["cbg"] == censusRow["cbg"]: 
            return_list["cbg_latitude"] = censusRow["cbg_latitude"]
            return_list["cbg_longitude"] = censusRow["cbg_longitude"]
    
    return return_list

def mapHotspots(row, idHotspotNodes, cbgHotspotNodes):
    return_list = row
    if row["cbg"] in cbgHotspotNodes:
        return_list["hotspot_origin"] = True
    
    if row["safegraph_place_id"] in idHotspotNodes:
        return_list["hotspot_destination"] = True
    
    return return_list

# Insert a value into its correct place in a sorted list
def insert(fluxList, fluxDict, key, value):
    index = -1
    for i in range(len(fluxList)):
        if value > fluxDict[fluxList[i]]["flux"]:
            index = i
            break
    
    if index != -1:
        fluxList = fluxList[:index] + [key] + fluxList[index:]
        fluxDict[key]["sortedIndex"] = index
        for j in range(index+1, len(fluxList)):
            fluxDict[fluxList[j]]["sortedIndex"] += 1
    else:
        fluxList.append(key)
        fluxDict[key]["sortedIndex"] = len(fluxList)-1
    
    return fluxList

def main():
    frames = []
    for f in geocodedPatternsDataFiles:
        if city in f:
            frames.append(pd.read_pickle(geocodedPatternsDataDir + f))
    dfComplete = pd.concat(frames, ignore_index=True)

    # Load CBG Geography mapping
    dfCensus = pd.read_csv(cbgGeographicFile)
    dfCensus = dfCensus[["census_block_group", "longitude", "latitude"]]
    dfCensus = dfCensus.rename(columns={"census_block_group": "cbg", "longitude": "cbg_longitude", "latitude": "cbg_latitude"})

    framesCBGS = []
    for frame in frames:
        dfCBGS = frame[frame.visitor_home_cbgs.notnull()]
        #dfCBGS = dfCBGS[dfCBGS.visitor_work_cbgs.notnull()]
        dfCBGS = dfCBGS[["safegraph_place_id", "visitor_home_cbgs", "latitude", "longitude"]]
        framesCBGS.append(dfCBGS)

    for i in range(len(framesCBGS)):
        homeCBGSFilter = [len(x) > 2 for x in framesCBGS[i]["visitor_home_cbgs"]]
        framesCBGS[i]["homeCBGSFilter"] = homeCBGSFilter
        framesCBGS[i] = framesCBGS[i][framesCBGS[i].homeCBGSFilter == True]

    for i in range(len(framesCBGS)):
        framesCBGS[i]["visitor_home_cbgs"] = framesCBGS[i]["visitor_home_cbgs"].apply(loadJson)

    for i in range(len(framesCBGS)):
        framesCBGS[i] = framesCBGS[i].drop(["homeCBGSFilter"], axis=1)

    framesMovement = []
    for i in range(len(framesCBGS)):
        dfMovement = framesCBGS[i].apply(mapCBGNetwork, axis=1).to_frame()
        dfMovement.columns = ["structure"]
        framesMovement.append(dfMovement)

    for i in range(len(framesMovement)):
        dataFilter = [x[0] for x in framesMovement[i]["structure"]]
        framesMovement[i] = pd.DataFrame(dataFilter, columns=["safegraph_place_id", "longitude", "latitude", "cbg", "visits"])
        framesMovement[i]["cbg"] = framesMovement[i]["cbg"].apply(int)
        
    framesMovementList = []
    for i in range(len(framesMovement)):
        framesMovementList.append(list(framesMovement[i]))

    for frame in framesMovement:
        frame["cbg_latitude"] = [0.0] * len(frame.index)
        frame["cbg_longitude"] = [0.0] * len(frame.index)
        #frame = frame.apply(mapCBGCoordinates, args=(dfCensus), axis=1)

    ### Build OD network
    # Nodes: Origin node -> CBG, Destination node -> Safegraph ID
    # Edges: Connect Origin nodes to Destination nodes, weighted based on number of visits
    # We use a bipartite graph to represent this network

    #weekDict = {0:"2020-03-01", 1:"2020-03-08", 2:"2020-03-15", 3:"2020-03-22", 4:"2020-03-29"}
    weekDict = {0:"2020-03-29"}
    for week in range(len(weekDict.keys())):
        date = weekDict[week]

        # Key: CBG Number of safe_graph_id respectively, flux: visits, sortedIndex: position in sorted flux list
        originFluxDict = {}
        destFluxDict = {}

        G = nx.Graph()
        for index, row in framesMovement[week].iterrows():
            cbg = row["cbg"]
            poi = row["safegraph_place_id"]
            visits = row["visits"]
            
            # Does this cbg have a node yet?
            if cbg in originFluxDict.keys():
                G.nodes[cbg]["flux_out"] += visits
                originFluxDict[cbg]["flux"] += visits
            else:
                G.add_node(cbg, bipartite=0, flux_out=visits)
                originFluxDict[cbg] = {"flux": visits, "sortedIndex": -1}
            
            # Does this POI have a node yet?
            if poi in destFluxDict.keys():
                G.nodes[poi]["flux_in"] += visits
                destFluxDict[poi]["flux"] += visits
            else:
                G.add_node(poi, bipartite=1, flux_in=visits)
                destFluxDict[poi] = {"flux": visits, "sortedIndex": -1}
            
            G.add_edge(cbg, poi, weight=visits)
        
        # Load hotspot data
        with open(hotspotFile) as f:
            hotspotData = json.load(f)

        # fluxList is a list of safegraph_id's sorted in ascending order based on their flux
        destFluxList = []
        for key in destFluxDict.keys():
            if destFluxList == None:
                destFluxList = [key]
                destFluxDict[key]["sortedIndex"] = 0
            else:
                destFluxList = insert(destFluxList, destFluxDict, key, destFluxDict[key]["flux"])

        # Destination Hot Spot separation point: argmin of the Solving Equation results list
        destResults = []
        if not date in hotspotData[city].keys():
            c_d = findHotSpots(destFluxList, destFluxDict, destResults)
        else:
            c_d = hotspotData[city][date]["c_d"]
            
        # Destination Hotspot
        for key in destFluxDict.keys():
            if destFluxDict[key]["sortedIndex"] == c_d:
                c_d_key = key
                c_d_flux = destFluxDict[key]["flux"]

        # Stats
        print(c_d_key)
        print(c_d_flux)

        # fluxList is a list of safegraph_id's sorted in ascending order based on their flux
        originFluxList = []
        for key in originFluxDict.keys():
            if originFluxList == None:
                originFluxList = [key]
                originFluxDict[key]["sortedIndex"] = 0
            else:
                originFluxList = insert(originFluxList, originFluxDict, key, originFluxDict[key]["flux"])

        # Origin Hot Spot separation point: argmin of the Solving Equation results list
        originResults = []
        if not date in hotspotData[city].keys():
            c_o = findHotSpots(originFluxList, originFluxDict, originResults)
        else:
            c_o = hotspotData[city][date]["c_o"]
            
        # Origin Hotspot
        for key in originFluxDict.keys():
            if originFluxDict[key]["sortedIndex"] == c_o:
                c_o_key = key
                c_o_flux = originFluxDict[key]["flux"]

        # Stats
        print(c_o_key)
        print(c_o_flux)

        # Write Hotspot data to file
        hotspotData[city][date] = {"c_o": c_o, "c_d": c_d}
        with open(hotspotFile, "w") as f:
            json.dump(hotspotData, f)

if __name__ == "__main__":
    main()
