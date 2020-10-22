#To connect and interact with the DB
import pyodbc 

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

import os


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

######### OPEN CONNECTION TO DB

cnxn = ft.openDb(connDetails)

logging.info("Connection with {0} open".format(database))

# Cursor to DB
cursor = cnxn.cursor()

########### DOWNLOAD SurveyStructure

logging.info("Downloading Table SurveyStructure")

SurveyStructureCurrent = ft.SQLTableToDf(database, cnxn, "SurveyStructure", ["SurveyId", "QuestionId", "OrdinalValue" ])

#close connection
cnxn.close()

#print(SurveyStructureDf.head(3))

#print(SurveyStructureDf.shape)

#Check if you have a file  in the current directory named SurveyStructure.pkl

#print(ft.isCsvHere(DirStore, CSVname))

###############################################################################################
#Alternative approach

if ft.isViewFresh(DirStore, SurveyStructurePastFile, SurveyStructureCurrent) == False:

    logging.info("View not fresh")

    pass
    # Create the query for the view
    #ft.createDynamicQueryForView(viewName)


    # create/refresh the view

else:

    print("View not fresh")


# Run the query (which is always SELECT * FROM <view>) to extract the view 


# dump content of the csv on a file named <survey>_<todayDate>.csv









###############################################################################################

# If a previous SurveyStructureFile is available
if ft.isFileHere(DirStore, SurveyStructureFile):
    try:
        with open(os.path.join(DirStore, SurveyStructureFile)) as f:
            SurveyStructurePast = f.read()
    except Exception as ex:
        logging.critical(f"Cannot open file {SurveyStructureFile}")
    
    # If current SurveyStructure image different from past SurveyStructure image
    if SurveyStructureDf.equals(SurveyStructurePast) == False:
        logging.info("No changes in SurveyStructure - retrieving data from the view")

        #Rebuild query to create the view


        # refresh the view


        # extract the content of the view

    # if the current SurveyStructure image = past SurveyStructure image 
        
        logging.info("No changes in SurveyStructure - retrieving data from the view")


        # ==> extract the view


# if a previous image does not exist

    # save in pkl the current image 


    # rebuild the query to create the view


    # extract the view


#2.b) If you have a backup of SurveyStructure, compare the backup and the current snapshot of SurveyStructure; to compare you can first compare the number of rows + check this function https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.equals.html
#   2.b.1 if the backup SurveyStructure and the current SurveyStructure are aligned ==> no problem, do not change the view and just extract in csv the view as it is
#   2.b.2 if the backup SurveyStructure and the current SurveyStructure are different, then: 
#       - rebuild the dynamic SQL query based on the new SurveyStructure 
#       - alter the view
#       - extract a new csv of the view 


#2.a) If you do not have locally a backup of SurveyStructure:
# - save locally SurveyStructure
# - Answer
# - User  
# - Build the SQL code for the view - you have to dynamically create it, as it may change if SurveyStructure changes; once you have created the SQL Query, launch via python the SQL query to create the view
# - run the SQL code to extract the pivoted table in csv format and store it





#3) 




# CLose the connection to the Database



# if previous data are available (in pkl file) ==> do a comparison



# if previos data are not available save them in pkl






    #SELECT       UserId       , 1 as SurveyId       ,      COALESCE(      (       SELECT a.Answer_Value       FROM Answer as a       WHERE        a.UserId = u.UserId        AND a.SurveyId = 1        AND a.QuestionId = 1      ), -1) AS ANS_Q1  ,      COALESCE(      (       SELECT a.Answer_Value       FROM Answer as a       WHERE        a.UserId = u.UserId        AND a.SurveyId = 1        AND a.QuestionId = 2      ), -1) AS ANS_Q2  ,  NULL AS ANS_Q3      FROM      [User] as u     WHERE EXISTS     (       SELECT *       FROM Answer as a       WHERE u.UserId = a.UserId       AND a.SurveyId = 1     )  UNION     SELECT       UserId       , 2 as SurveyId       ,  NULL AS ANS_Q1  ,      COALESCE(      (       SELECT a.Answer_Value       FROM Answer as a       WHERE        a.UserId = u.UserId        AND a.SurveyId = 2        AND a.QuestionId = 2      ), -1) AS ANS_Q2  ,      COALESCE(      (       SELECT a.Answer_Value       FROM Answer as a       WHERE        a.UserId = u.UserId        AND a.SurveyId = 2        AND a.QuestionId = 3      ), -1) AS ANS_Q3      FROM      [User] as u     WHERE EXISTS     (       SELECT *       FROM Answer as a       WHERE u.UserId = a.UserId       AND a.SurveyId = 2     )  UNION     SELECT       UserId       , 3 as SurveyId       ,      COALESCE(      (       SELECT a.Answer_Value       FROM Answer as a       WHERE        a.UserId = u.UserId        AND a.SurveyId = 3        AND a.QuestionId = 1      ), -1) AS ANS_Q1  ,  NULL AS ANS_Q2  ,  NULL AS ANS_Q3      FROM      [User] as u     WHERE EXISTS     (       SELECT *       FROM Answer as a       WHERE u.UserId = a.UserId       AND a.SurveyId = 3     ) 





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