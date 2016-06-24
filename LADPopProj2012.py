__author__ = 'G'

import sys
sys.path.append('../harvesterlib')

import pandas as pd
import argparse
import json

import now


def download(inPath, outPath, col, keyCol, digitCheckCol, noDigitRemoveFields):
    dName = outPath

    genderArray = ["female", "male"]

    listinPath = inPath[0].split('/')
    iYear = listinPath[len(listinPath) - 1].split('-')[0]

    iPopID = "ONS-" + iYear + "-based-LAD-Subnational-Population-Projections"
    #iPopType = "Base"
    iPopdescription = "ONS projections (http://www.ons.gov.uk/peoplepopulationandcommunity/populationandmigration/populationprojections/datasets/localauthoritiesinenglandtable2)"

    # operate this file
    raw_data = {}
    for j in col:
        raw_data[j] = []

    for i in range(2):
        inFile = inPath[i]

        # load files
        logfile.write(str(now.now()) + ' ' + inFile + ' file loading\n')
        print(inFile + ' file loading------')

        df = pd.read_csv(inFile, dtype='unicode')
        csvcol = df.columns.tolist()
        yearcol = csvcol[3:]
        lenyearcol = len(yearcol)

        for j in range(lenyearcol):
            raw_data[col[2]] = raw_data[col[2]] + df.ix[:, 2].tolist()
            raw_data[col[4]] = raw_data[col[4]] + [yearcol[j]] * df.shape[0]
            raw_data[col[5]] = raw_data[col[5]] + df.ix[:, 0].tolist()
            raw_data[col[6]] = raw_data[col[6]] + df.ix[:, 1].tolist()
            raw_data[col[7]] = raw_data[col[7]] + df.ix[:, j + 3].tolist()
            raw_data[col[8]] = raw_data[col[8]] + [int(float(x))/int(float(df.ix[df.shape[0]-1, j + 3])) for x in df.ix[:, j + 3].tolist()]

        raw_data[col[0]] = raw_data[col[0]] + [iPopID] * df.shape[0] * lenyearcol
        #raw_data[col[1]] = raw_data[col[1]] + [iPopType] * df.shape[0] * lenyearcol
        raw_data[col[1]] = raw_data[col[1]] + [iPopdescription] * df.shape[0] * lenyearcol
        raw_data[col[3]] = raw_data[col[3]] + [genderArray[i]] * df.shape[0] * lenyearcol


    raw_data[col[7]] = [int(float(i) * 1000) for i in raw_data[col[7]]]
    raw_data[col[2]] = [i.replace("All ages", "All Ages") for i in raw_data[col[2]]]

    df1 = pd.DataFrame(raw_data)
    strings = df1.to_json(orient="records")

    jsonString = '[{"jsondata":' + strings + '}]'

    myJson = pd.read_json(jsonString)
    myJson.index = ['mydata']

    # save to file
    myJson.to_json(path_or_buf=dName, orient="index")
    logfile.write(str(now.now()) + ' has been extracted and saved as ' + str(dName) + '\n')
    print('Requested data has been extracted and saved as ' + dName)
    logfile.write(str(now.now()) + ' finished\n')
    print("finished")


parser = argparse.ArgumentParser(
    description='Get the age group date from 2012-based_Subnational_Population_Projections as .json files.')
parser.add_argument("--generateConfig", "-g", help="generate a config file called config_tempLADPopProj2012MF.json",
                    action="store_true")
parser.add_argument("--configFile", "-c", help="path for config file")
args = parser.parse_args()

if args.generateConfig:
    obj = {
        "inPath": ["./data/2012-based_Subnational_Population_Projections_female.csv", "./data/2012-based_Subnational_Population_Projections_male.csv"],
        "outPath": "2012-based_Subnational_Population_Projections-mf.json",
        "colFields": ['popId', 'popId_description', "age_band", "gender", "year", "area_id", "area_name", "persons"],
        "primaryKeyCol": [],
        "digitCheckCol": [],
        "noDigitRemoveFields": []
    }

    logfile = open("log_tempLADPopProj2012MF.log", "w")
    logfile.write(str(now.now()) + ' start\n')

    errfile = open("err_tempLADPopProj2012MF.err", "w")

    with open("config_tempLADPopProj2012MF.json", "w") as outfile:
        json.dump(obj, outfile, indent=4)
        logfile.write(str(now.now()) + ' config file generated and end\n')
        sys.exit("config file generated")

if args.configFile == None:
    args.configFile = "config_tempLADPopProj2012MF.json"

with open(args.configFile) as json_file:
    oConfig = json.load(json_file)

    logfile = open('log_' + oConfig["outPath"].split('.')[0] + '.log', "w")
    logfile.write(str(now.now()) + ' start\n')

    errfile = open('err_' + oConfig["outPath"].split('.')[0] + '.err', "w")

    logfile.write(str(now.now()) + ' read config file\n')
    print("read config file")

download(oConfig["inPath"], oConfig["outPath"], oConfig["colFields"], oConfig["primaryKeyCol"], oConfig["digitCheckCol"], oConfig["noDigitRemoveFields"])
