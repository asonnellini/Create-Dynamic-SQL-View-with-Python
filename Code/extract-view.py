

######################### REQUIRED MODULES ########################

requirements = ["logging","argparse", "pandas", "pyodbc", "os", "pickle", "cryptography", "base64"]


############################# INSTALL MISSING MODULES #############################

import sys
import subprocess
#import pkg_resources

def install(package):
    subprocess.check_call([sys.executable, "-m", "pip", "install", package])

for module in requirements:
    try:
        __import__(module)
    except:
        print(f"installing the package {module}")
        install(module)
        

############################# LOAD MODULES #############################



# manage the logging of events
import logging

# To pass arguments from command line
import argparse

# pandas for Dataframes
import pandas as pd

# import tools from the folder scriptTools
import scriptTools as st

# Format properly the logging messages
logging.basicConfig(format='%(levelname)s:%(message)s', level= getattr(logging, "INFO")  )


############################# ARGUMENTS PASSED TO THE SCRIPT #############################

parser = argparse.ArgumentParser(description = "This script dumps on a csv file the up-to-date snapshot of the view vw_AllSurveyData")

parser.add_argument("user", help="User to connect with to the Database",
                    type=str)

parser.add_argument("pwd", help="Password to connect with to the Database",
                    type=str )

parser.add_argument("--server", help="Server that hosts the DB - default: localhost",
                    type=str, default = 'localhost')

parser.add_argument("--DB", help="Name of the Database to connect to - default: Survey_Sample_A18",
                    type=str, default = 'Survey_Sample_A18')

parser.add_argument("--DirStore", help="Directory on the local machine that hosts the pkl file with previous snapshots of the SurveryStructure table and where the snapshot of the view vw_AllSurveyData will be dumped - default: C:\\Users\\Andrea\\Desktop\\StoreCsvDir",
                    type=str, default = "C:\\Users\\Andrea\\Desktop\\StoreCsvDir")

parser.add_argument("--oldSnap", help="The name of the pkl file with previous snapshots of the SurveryStructure table - default: SurveyStructure.pkl",
                    type=str, default = "SurveyStructure.pkl")

parser.add_argument("--csv", help="The name of the csv file that has a snapshot of the vw_AllSurveyData view - default: SurveyOutcome.csv",
                    type=str, default = "SurveyOutcome.csv")


args = parser.parse_args()


############################# CONNECTION DETAILS ################################

connDetails = st.obfuscate('DRIVER={ODBC Driver 17 for SQL Server};SERVER=' + args.server + ';DATABASE=' + args.DB + ';UID=' + args.user + ';PWD=' + args.pwd)


################### CSV DIRECTORY LOCATION FOR SurveyStructure ###################

DirStore = args.DirStore
SurveyStructurePastFile = args.oldSnap
viewCsv = args.csv


########################### DOWNLOAD SurveyStructure #############################

SurveyStructureCurrent = st.SQLTableToDf(connDetails, "SurveyStructure", ["SurveyId", "QuestionId", "OrdinalValue" ])


########### CHECK WHETHER THE VIEW IS UP-TO-DATE - if not, refresh the view #############################

if st.isViewFresh(DirStore, SurveyStructurePastFile, SurveyStructureCurrent) == False:

    
    #Save the latest SurveyStructure on a pikle file
    st.saveDfOnPkl(DirStore, SurveyStructurePastFile, SurveyStructureCurrent)

    # Create the query for the view
    queryView = st.createDynamicQueryForView(connDetails)
      
    # create/refresh the view
    st.createOrAlterView(queryView, connDetails)

############################# Extract the data from the view #############################
viewDf = st.SQLTableToDf(connDetails, "vw_AllSurveyData", ["SurveyId"])

st.saveDfOnCsv(DirStore, viewCsv, viewDf)

logging.info(f"The up-to-date snapshot of vw_AllSurveyData has been saved to {viewCsv}")

############################# END SCRIPT #############################

