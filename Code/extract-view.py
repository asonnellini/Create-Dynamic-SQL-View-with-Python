# Test scenario:
# - cannot open the connection with DB ==> raise critical message + exit script
# - cannot save the view in the csv ==> raise critical error + exit
# - cannot load the pkl file ==> raise a warning messag and continue refreshing the view
# - cannot save the pkl file ==> raise a warning messag and continue 
# - change the SurveyStructure Table with:
#   - INSERT INTO SurveyStructure (SurveyId, QuestionId, OrdinalValue) VALUES	(3, 2, 1) ==> upon running the script, the column ANS_Q2 of the view changed from NULL to -1 = View was updated = OK
#   - DELETE FROM SurveyStructure WHERE SurveyId = 3 AND QuestionId = 2 AND OrdinalValue = 1 ==> upon running the script, the column ANS_Q2 of the view changed from -1 to NULL = View was updated = OK
#   - UPDATE SurveyStructure SET QuestionId = 2 WHERE SurveyId = 3 AND QuestionId = 1 AND OrdinalValue = 1 ==> upon running the script, ANS_Q1 was 10 and ANS_Q2 was NULL, now they are NULL and -1 OK!


# manage the logging of events
import logging

import argparse

##https://docs.python.org/3/howto/argparse.html
#parser = argparse.ArgumentParser(description = "This script dumps on a csv file the up-to-date snapshot of the view vw_AllSurveyData")

#parser.add_argument("--server", help="Server that hosts the DB",
#                    type=str, default = 'DESKTOP-DNQ8B1C')

#parser.add_argument("--DB", help="Name of the Database to connect to",
#                    type=str, default = 'Survey_Sample_A18')

#parser.add_argument("--user", help="User to connect with to the Database",
#                    type=str)

#parser.add_argument("--pwd", help="Password to connect with to the Database",
#                    type=str )

#parser.add_argument("--DirStore", help="Directory on the local machine that hosts the pkl file with previous snapshots of the SurveryStructure table and where the snapshot of the view vw_AllSurveyData will be dumped",
#                    type=str, default = "C:\\Users\\Andrea\\Desktop\\StoreCsvDir")

#parser.add_argument("--oldSnap", help="The name of the pkl file with previous snapshots of the SurveryStructure table",
#                    type=str, default = "SurveyStructure.pkl")

#parser.add_argument("--csv", help="The name of the csv file that has a snapshot of the vw_AllSurveyData view",
#                    type=str, default = "SurveyOutcome.csv")


#args = parser.parse_args()
##print(args.square**2)




# pandas for Dataframes
import pandas as pd

import funcTools as ft

import ContentObfuscation as co

# Format properly the logging messages
logging.basicConfig(format='%(levelname)s:%(message)s', level= getattr(logging, "INFO")  )


######### CONNECTION DETAILS
server = 'DESKTOP-DNQ8B1C' 
database = 'Survey_Sample_A18' 
username = 'sa' 
password = 'sonne4ever' 

connDetails = co.obfuscate('DRIVER={ODBC Driver 17 for SQL Server};SERVER=' + server + ';DATABASE=' + database + ';UID=' + username + ';PWD=' + password)


######### CSV DIRECTORY LOCATION FOR SurveyStructure

DirStore = "C:\\Users\\Andrea\\Desktop\\StoreCsvDir"
SurveyStructurePastFile = "SurveyStructure.pkl"
viewCsv = "SurveyOutcome.csv"


########### DOWNLOAD SurveyStructure

logging.info("Downloading snapshot of SurveyStructure")

SurveyStructureCurrent = ft.SQLTableToDf(connDetails, "SurveyStructure", ["SurveyId", "QuestionId", "OrdinalValue" ])

########### CHECK WHETHER THE VIEW IS UP-TO-DATE - if not, refresh the view

if ft.isViewFresh(DirStore, SurveyStructurePastFile, SurveyStructureCurrent) == False:

    
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