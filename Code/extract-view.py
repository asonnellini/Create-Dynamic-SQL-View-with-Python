## TO DO

# EXIT FROM PROGRAM IN CASE CANNOT CONNECT TO DB

# EXIT IF YOU CANNOT SAVE THE CSV

# IN CASE YOU CANNOT SAVE PKL JUST STATE A WARNING AND SUPPRESS "SUCCESSFUL MESSAGES"

# IN CASE YOU CANNOT READ THE PKL JUST STATE A WARNING AND SUPPRESS "SUCCESSFUL MESSAGES"


# manage the logging of events
import logging

import argparse

#https://docs.python.org/3/howto/argparse.html
#parser = argparse.ArgumentParser()
#parser.add_argument("square", help="display a square of a given number",
#                    type=int)
#args = parser.parse_args()
#print(args.square**2)

# pandas for Dataframes
import pandas as pd

import funcTools as ft


# Format properly the logging messages
logging.basicConfig(format='%(levelname)s:%(message)s', level= getattr(logging, "INFO")  )


######### CONNECTION DETAILS
server = 'DESKTOP-DNQ8B1C' 
database = 'Survey_Sample_A18' 
username = 'sa' 
password = 'sonne4ever' 

connDetails = 'DRIVER={ODBC Driver 17 for SQL Server};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password

######### CSV DIRECTORY LOCATION FOR SurveyStructure

DirStore = "C:\\Users\\Andrea\\Desktop\\StoreCsvDir"
SurveyStructurePastFile = "SurveyStructure.pkl"
viewCsv = "SurveyOutcome.csv"


########### DOWNLOAD SurveyStructure

logging.info("Downloading snapshot of SurveyStructure")

SurveyStructureCurrent = ft.SQLTableToDf(connDetails, "SurveyStructure", ["SurveyId", "QuestionId", "OrdinalValue" ])

########### CHECK WHETHER THE VIEW IS UP-TO-DATE

if ft.isViewFresh(DirStore, SurveyStructurePastFile, SurveyStructureCurrent) == False:

    logging.info("View not fresh... Refreshing the view")

    #Save the latest SurveyStructure on a pikle file
    ft.saveDfOnPkl(DirStore, SurveyStructurePastFile, SurveyStructureCurrent)

    # Create the query for the view
    queryView = ft.createDynamicQueryForView(connDetails)
      
    # create/refresh the view
    ft.createOrAlterView(queryView, connDetails)

# Extract the data from the view
viewDf = ft.SQLTableToDf(connDetails, "vw_AllSurveyData", ["SurveyId"])

ft.saveDfOnCsv(DirStore, viewCsv, viewDf)

logging.info(f"The up-to-date pivoted outcome of Surveys has been saved to {viewCsv}")










############

#Check if package is installed, if not install it

#import importlib.util
#import sys

## For illustrative purposes.
#name = 'itertools'

#if name in sys.modules:
#    print(f"{name!r} already in sys.modules")
#elif (spec := importlib.util.find_spec(name)) is not None:
#    # If you choose to perform the actual import ...
#    module = importlib.util.module_from_spec(spec)
#    sys.modules[name] = module
#    spec.loader.exec_module(module)
#    print(f"{name!r} has been imported")
#else:
#    print(f"can't find the {name!r} module")