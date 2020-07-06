# Liam Bessell, 3/31/20

import pandas as pd
import numpy as np

csvFilePath = "./v1/main-file/2020-03-01-weekly-patterns.csv"

def main():
    print("Reading {0}...".format(csvFilePath))
    df = pd.read_csv(csvFilePath, nrows=50, engine="python")
    print("Successfully read {0}".format(csvFilePath))
    df.info()

if __name__ == "__main__":
    main()