# Liam Bessell, 6/1/20
# Create final deliverable network dataframes

# Library imports
import os
import sys
import pandas as pd
import pickle
import json

os.chdir("/home/liam/docs/c19-response/")

# Globals
networkHotspotDataDir = "./data/main-file/v2/networks/"                                                                                

cityFile = "./data/core_cities.json"
cities = {}             
weekDict = {
    0:"2019-12-30", 
    1:"2020-01-06", 2:"2020-01-13", 3:"2020-01-20", 4:"2020-01-27", 
    5:"2020-02-03", 6:"2020-02-10", 7:"2020-02-17", 8:"2020-02-24",  
    9:"2020-03-02", 10:"2020-03-09", 11:"2020-03-16", 12:"2020-03-23", 13:"2020-03-30", 
    14: "2020-04-06", 15: "2020-04-19", 16:"2020-04-20", 17:"2020-04-27",
    18: "2020-05-04", 19: "2020-05-11"
}

def main():

    # Load city list
    with open(cityFile) as f:
        cities = json.load(f)

    cityCtr = 0
    for city in cities.keys():

        ### Analyze Hotspot Network
        HHFiles = os.listdir(networkHotspotDataDir + city.lower() + "/HH/")
        HNFiles = os.listdir(networkHotspotDataDir + city.lower() + "/HN/")
        NHFiles = os.listdir(networkHotspotDataDir + city.lower() + "/NH/")
        NNFiles = os.listdir(networkHotspotDataDir + city.lower() + "/NN/")

        # edgeData = {}
        # for week in weekDict.keys():
        #     edgeData[weekDict[week]] = {"HH": 0, "HN": 0, "NH": 0, "NN": 0}
            
        HH = []
        HN = []
        NH = []
        NN = []
        for week in weekDict.keys():

            date = weekDict[week]
            HHFile = ""
            for f in HHFiles:
                if date in f:
                    HHFile = f
                    break
            
            HNFile = ""
            for f in HNFiles:
                if date in f:
                    HNFile = f
                    break
            
            NHFile = ""
            for f in NHFiles:
                if date in f:
                    NHFile = f
                    break
            
            NNFile = ""
            for f in NNFiles:
                if date in f:
                    NNFile = f
                    break


            if HHFile.endswith("pkl"):
                dfHH = pd.read_pickle(networkHotspotDataDir + city.lower() + "/HH/" + HHFile)
            
            if HNFile.endswith("pkl"):
                dfHN = pd.read_pickle(networkHotspotDataDir + city.lower() + "/HN/" + HNFile)
            
            if NHFile.endswith("pkl"):
                dfNH = pd.read_pickle(networkHotspotDataDir + city.lower() + "/NH/" + NHFile)
            
            if NNFile.endswith("pkl"):
                dfNN = pd.read_pickle(networkHotspotDataDir + city.lower() + "/NN/" + NNFile)
            
            # edgeData[date]["HH"] += dfHH["visits"].sum()
            # edgeData[date]["HN"] += dfHN["visits"].sum()
            # edgeData[date]["NH"] += dfNH["visits"].sum()
            # edgeData[date]["NN"] += dfNN["visits"].sum()

            HH.append(dfHH["visits"].sum())
            HN.append(dfHN["visits"].sum())
            NH.append(dfNH["visits"].sum())
            NN.append(dfNN["visits"].sum())


        # Create flows
        # HH = []
        # HN = []
        # NH = []
        # NN = []
        # weeks = [week for week in edgeData.keys()]
        # for week in edgeData.keys():

        #     HH.append(edgeData[week]["HH"])
        #     HN.append(edgeData[week]["HN"])
        #     NH.append(edgeData[week]["NH"])
        #     NN.append(edgeData[week]["NN"])


        # Compute flow proportion
        allEdges = []
        HH_proportion = []
        HN_proportion = []
        NH_proportion = []
        NN_proportion = []
        for i in range(len(HH)):

            allEdges.append(HH[i] + HN[i] + NH[i] + NN[i])
            
            if allEdges[i] > 0:
                HH_proportion.append(HH[i] / allEdges[i])
                HN_proportion.append(HN[i] / allEdges[i])
                NH_proportion.append(NH[i] / allEdges[i])
                NN_proportion.append(NN[i] / allEdges[i])
            else:
                HH_proportion.append(0)
                HN_proportion.append(0)
                NH_proportion.append(0)
                NN_proportion.append(0)
        

        # Create dataframe
        df = pd.DataFrame(columns=["date", "city", 
                                    "hh_absolute", "hn_absolute", "nh_absolute", "nn_absolute", 
                                    "hh_proportion", "hn_proportion", "nh_proportion", "nn_proportion"])

        ctr = 0
        for week in weekDict.keys():

            date = weekDict[week]
            data = {"date": date, "city": city.lower(), 
                        "hh_absolute": HH[ctr], "hn_absolute": HN[ctr], "nh_absolute": NH[ctr], "nn_absolute": NN[ctr],
                        "hh_proportion": HH_proportion[ctr], "hn_proportion": HN_proportion[ctr], "nh_proportion": NH_proportion[ctr], "nn_proportion": NN_proportion[ctr]}
            df = df.append(data, ignore_index=True)

            ctr += 1

        df.to_pickle(networkHotspotDataDir + "complete/" + city.lower() + "_network_analysis.pkl")


        cityCtr += 1
        print("{0}/{1}".format(cityCtr, len(cities.keys())))


if __name__ == "__main__":
    main()