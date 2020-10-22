# Functions

#Modules

import pyodbc 

import pandas as pd

import logging

import os

def openDb(connDetails:str):
    """This function opens the connection to the Database
    
    Input:
        - connDetails: connection string for pyodbc in a safe way, i.e. with try except mechanism
    
    Output:
        - cnxn: output of pyodbc.connect

    """

    try:
        # open connection with the database
        cnxn = pyodbc.connect(connDetails)
    
    except Exception as ex:

        if type(ex).__name__ == "OperationalError":

            logging.critical("Cannot connect to the Database {0} - please check the odb connection string and/or DB availability.".format(database))
        
        else:

            logging.critical("Cannot connect to the Database {0}.".format(database))

    return cnxn


def queryDB(connDetails:str, queryString:str) -> pd.DataFrame:
    """
    This function returns in a dataframe the output of a query.
    
    Input:
        - connDetails: connection string required by pyodbc.connect, e.g.: connDetails = 'DRIVER={ODBC Driver 17 for SQL Server};SERVER='+ <serverName> +';DATABASE='+ <databaseName> + ';UID='+ <username> + ';PWD=' + <password>
        - queryString: SQL Query

    Output:
        - result: a dataframe with the output of the query

    """

    cnxn = openDb(connDetails)

    result = pd.read_sql_query(queryString, cnxn)

    cnxn.close()

    return result



def SQLTableToDf(DBName:str, connDetails:str, TableName:str, orderBy:str = None) -> pd.DataFrame:
    """
    This function returns a dataframe having the same content as the SQL Table TableName belonging to the Database pointed
    by DBCursor.
    Input:
        - DBName: name of the SQL Database
        - connDetails: Connection details to connect to the SQL Database via pyodbc
        - TableName: name of the original SQL Table
        - orderBy: columns based on which to order by
        
    Output:
        - SQLdf = dataframe having the same content as the original SQL Table
    
    """
    # Open connection
    cnxn = openDb(connDetails)
    
    #Initialize SQLdf
    SQLdf = None

    # Build the query to extract data from TableName 
    queryString = "SELECT * FROM [" + DBName + "].[dbo].[" + TableName + "]"
    
    # if orderBy is different from None, then add the ORDER BY clause to queryString
    if orderBy != None:

        queryString += " ORDER BY """ + ",".join(orderBy) 
    
    try:
        SQLdf = pd.read_sql_query(queryString, cnxn)
    except Exception as ex:
        logging.critical("Cannot Download Data from the table {0} in the Database {1} with the following query: \n{2}".format( TableName, DBName, queryString ))
    
    cnxn.close()

    return SQLdf


def isFileHere(DirStore:str, fileName:str) -> bool:
    """
    Returns True if the file fileName is in the folder pointed by DirStore, False otherwise

    Input:
        - DirStore: path of the directory that should have the pkl file
        - fileName: name of the pkl file that saves the SurveyStructure from previous runs
    """
    # list of files in DirStore
    fileList = os.listdir(DirStore)

    return fileName in fileList

def safeOpenFile(DirStore:str, fileName:str):
    """ 
    This function safely opens fileName in the folder DirStore, and returns its content
    Input:
        - DirStore: path of the directory that should have the pkl file
        - fileName: name of the pkl file that saves the SurveyStructure from previous runs 
    Output:
        - contentFile: content of the file

    """
    try:
        with open(os.path.join(DirStore, fileName)) as f:
            contentFile = f.read()

    except Exception as ex:     
        logging.critical(f"Cannot open file {SurveyStructureFile}")

    return contentFile


def isViewFresh(DirStore:str, fileName:str, SurveyStructureDf: pd.DataFrame) -> bool:
    """
    Returns 
        - True if the file SurveyStructurePast in the directory DirStore exists and hosts a snapshot of SurveyStructure which is identical to SurveyStructureCurrent
        - False otherwise

    Input:
        - DirStore: path of the directory that should have the pkl file
        - fileName: name of the pkl file that saves the SurveyStructure from previous runs
        - SurveyStructureCurrent: dataframe 
    
    Output:
        - True means that the View is fresh and there is no need to create or alter it
        - False means that the View needs to be refreshed
    """

    result = False

    #if the pkl file named fileName with the snapshot of SurveyStructure from a previous run exists in DirStore
    if isFileHere(DirStore, fileName):
        
        # open the file
        SurveyStructurePast = safeOpenFile(DirStore, fileName)

        # and if the snapshot of SurveyStructure from a previous run is identical to the current snapshot, then result = True 
        result = SurveyStructureDf.equals(SurveyStructurePast)

    return result


def createDynamicQueryForView(viewName:str) -> str:
    """
    Create a dynamic query for the creation of the view
    
    """