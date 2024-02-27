import datetime
import os
import re
import signal
import socket
import sys
import time

import pymu.tools as tools
from pymu.client import Client
from pymu.pmuConfigFrame import import_config
from pymu.pmuDataFrame import DataFrame
from pymu.pmuLib import *
from pymu.server import Server

CSV_DIR = "./data"

RUNNING = True


def csvPrint(dFrame, csv_handle):

    strOut = ""
    for i in range(0, len(dFrame.pmus)):
        strOut += dFrame.soc.formatted + ","
        for j in range(0, len(dFrame.pmus[i].phasors)):
            strOut += str(dFrame.pmus[i].phasors[j].deg) + ","
        strOut += str(dFrame.pmus[i].freq) + ","
        strOut += str(dFrame.pmus[i].dfreq)
        if i != (len(dFrame.pmus) - 1):
            strOut += ","
    strOut += "\n"

    csv_handle.write(strOut)


def getNextIndex(originalPath):
    splitArr1 = originalPath.split("_")
    nextIndex = -1
    if len(splitArr1) == 2:
        nextIndex = 1
    elif len(splitArr1) > 2:
        splitArr2 = splitArr1[-1].split(".")
        nextIndex = int(splitArr2[0]) + 1

    if nextIndex <= 0:
        print("# Error creating next csv file from '{}'".format(originalPath))
        sys.exit()

    return nextIndex


def createCsvDir():
    global CSV_DIR

    if not os.path.isdir(CSV_DIR):
        os.mkdir(CSV_DIR)


def createCsvFile(confFrame):

    createCsvDir()

    stationName = confFrame.stations[0].stn
    prettyDate = time.strftime("%Y%m%d_%H%M%S", time.localtime())
    csvFileName = "{}_{}.csv".format(prettyDate, stationName.rstrip())
    csv_path = "{}/{}".format(CSV_DIR, csvFileName)

    if os.path.isfile(csv_path):
        nextIndex = getNextIndex(csv_path)
        csvFileName = "{}_{}.csv".format(prettyDate, nextIndex)
        csv_path = "{}/{}".format(CSV_DIR, csvFileName)

    csv_handle = open(csv_path, "w")
    csv_handle.write("Timestamp")
    for ch in confFrame.stations[0].channels:
        csv_handle.write(",{}".format(ch.rstrip())) if ch.rstrip() != "" else None
    csv_handle.write(",Freq")
    csv_handle.write(",ROCOF")
    csv_handle.write("\n")

    return csv_handle


if __name__ == "__main__":
    config_mapping = {
        1: "id01_neighborhood.pmuconfig",
        2: "id02_neighborhood.pmuconfig",
    }

    dataRcvr = Server(4713, "UDP", True)
    dataRcvr.setTimeout(10)

    p = 0
    milliStart = int(round(time.time() * 1000))
    timenow = milliStart
    run_for = 20000  # milliseconds
    while timenow - milliStart < run_for:
        try:

            # Extract idcode from dataframe ------------

            # Method 1: read in first 6 bytes, then the rest of the dataframe
            # d1 = tools.getDataSampleHex(dataRcvr, 6)
            # if d1 == "":
            #     break
            # idcode = int(d1[-4:], 16)
            # confFrame = import_config(config_mapping[idcode])
            # d2 = tools.getDataSampleHex(dataRcvr, confFrame.dataframe_bytes - 6)
            # d = d1 + d2

            # Method 2: read in max bytes, then parse out idcode & grab config
            d = tools.getDataSampleHex(dataRcvr, 65507)
            if d == "":
                break
            idcode = int(d[8:12])
            confFrame = import_config(config_mapping[idcode])

            # -------------------------------------------

            dFrame = DataFrame(d, confFrame)  # Create dataFrame
            if p == 0:
                csv_handle = createCsvFile(confFrame)
                print("Data Collection Started...")
            csvPrint(dFrame, csv_handle)
            p += 1
        except KeyboardInterrupt:
            break
            RUNNING = False
        except socket.timeout:
            print("## Data not available right now...Exiting")
            break
        except Exception as e:
            print(f"## Exception {e}")
            break
        timenow = int(round(time.time() * 1000))

    # Print statistics about processing speed
    milliEnd = int(round(time.time() * 1000))

    print("")
    print("##### ##### #####")
    print("Python Stats")
    print("----- ----- -----")
    print("Duration:  ", (milliEnd - milliStart) / 1000, "s")
    print("Total Pkts:", p)
    print("Pkts/Sec:  ", p / ((milliEnd - milliStart) / 1000))
    print("##### ##### #####")
    dataRcvr.stop()
    csv_handle.close()
