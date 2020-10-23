# Functions

#Modules

import pyodbc 

import pandas as pd

import logging

import os

import pickle

import sys

import ContentObfuscation as co

def openDb(connDetails:str):
    """This function opens the connection to the Database
    
    Input:
        - connDetails: connection string for pyodbc in a safe way, i.e. with try except mechanism
    
    Output:
        - cnxn: output of pyodbc.connect

    """
    #logging.info("Connecting to Database...")

    try:
        # open connection with the database
        cnxn = pyodbc.connect(co.deobfuscate(connDetails))
    
    except Exception as ex:
        
        logging.critical("Cannot connect to the Database - please check database connection details.")
        logging.critical(ex)
        sys.exit(-1)

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



def SQLTableToDf(connDetails:str, TableName:str, orderBy:str = None) -> pd.DataFrame:
    """
    This function returns a dataframe having the same content as the SQL Table TableName belonging to the Database pointed
    by DBCursor.
    Input:
        - connDetails: Connection details to connect to the SQL Database via pyodbc
        - TableName: name of the original SQL Table
        - orderBy: columns based on which to order by
        
    Output:
        - SQLdf = dataframe having the same content as the original SQL Table
    
    """
    # Open connection
    cnxn = openDb(connDetails)
    
    logging.info(f"Downloading snapshot of {TableName}")

    #Initialize SQLdf
    SQLdf = None

    # Build the query to extract data from TableName 
    
    queryString = "SELECT * FROM " + TableName 
    
    # if orderBy is different from None, then add the ORDER BY clause to queryString
    if orderBy != None:

        queryString += " ORDER BY " + ",".join(orderBy) 
    
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


def safeOpenPkl(DirStore:str, fileName:str):
    """ 
    This function safely opens fileName in the folder DirStore, and returns its content
    Input:
        - DirStore: path of the directory that should have the pkl file
        - fileName: name of the pkl file that saves the SurveyStructure from previous runs 
    Output:
        - result: content of the file

    """
    result = pd.DataFrame()

    success = True

    try:
        with open(os.path.join(DirStore,fileName),'rb') as f:
            result = pickle.load(f)
    except Exception as ex:     
        logging.warning(f"Cannot open file {fileName} - will refresh the view")
        logging.warning(ex)
        result = pd.DataFrame()
        success = False

    if success == True: logging.info(f"{fileName} Successfully loaded")

    return result


def isViewFresh(DirStore:str, fileName:str, SurveyStructureDf: pd.DataFrame) -> bool:
    """
    Returns 
        - True if the file SurveyStructurePast in the directory DirStore exists and hosts a snapshot of SurveyStructure which is identical to SurveyStructureCurrent
        - False otherwise

    Input:
        - DirStore: path of the directory that should have the pkl file
        - fileName: name of the pkl file that saves the SurveyStructure from previous runs
        - SurveyStructureCurrent: dataframe with the current snapshot of SurveyStructure
    
    Output:
        - True means that the View is fresh and there is no need to create or alter it
        - False means that the View needs to be refreshed
    """

    result = False

    #if the pkl file named fileName with the snapshot of SurveyStructure from a previous run exists in DirStore
    if isFileHere(DirStore, fileName):
        
        logging.info(f"Comparing current snapshot of SurveyStructure with the previous one saved in {fileName}")

        # open the file
        SurveyStructurePast = safeOpenPkl(DirStore, fileName)

        # and if the snapshot of SurveyStructure from a previous run is identical to the current snapshot, then result = True 
        result = SurveyStructureDf.equals(SurveyStructurePast)

        if result == True: logging.info("No changes on SurveyStructure compared to the previous snapshot - the view vw_AllSurveyData is already up-to-date") 

        else: logging.info("The previous snapshot of SurveyStructure is outdated... need to refresh the view vw_AllSurveyData")

    return result

def createDynamicQueryForView(connDetails:str) -> str:
    """
    Create the Dynamic Query for the View 

    Input:
        - connDetails: connection string for pyodbc in a safe way, i.e. with try except mechanism

    Output:
        - strFinalQuery: the final query for the view
    """
    
    #Download the content of Survey
    SurveyCurrent = SQLTableToDf(connDetails, "Survey", ["SurveyId"])

	# Outer query template
    strQueryTemplateForOuterUnionQuery = """
			SELECT
						UserId
						, <SURVEY_ID> as SurveyId
						, <DYNAMIC_QUESTION_ANSWERS>
				FROM
					[User] as u
				WHERE EXISTS
				(
						SELECT *
						FROM Answer as a
						WHERE u.UserId = a.UserId
						AND a.SurveyId = <SURVEY_ID>
				) """

	# Template for answer referring to questions which are part of the SurveyId in object
    strQueryTemplateForAnswerQuery = """
				COALESCE(
					(
						SELECT a.Answer_Value
						FROM Answer as a
						WHERE
							a.UserId = u.UserId
							AND a.SurveyId = <SURVEY_ID>
							AND a.QuestionId = <QUESTION_ID>
					), -1) AS ANS_Q<QUESTION_ID> """

	# Template for answer referring to questions which are NOT part of the SurveyId in object
    strQueryTemplateForNullColumn = ' NULL AS ANS_Q<QUESTION_ID> '

	# initialize
    strCurrentUnionQueryBlock = ''

    strFinalQuery = ''

	# Outer loop on each row of Survey table
    for indexSurveyCurrent, rowSurveyId in SurveyCurrent.iterrows():

		# Extract only the field SurveyId
        currentSurveyId = rowSurveyId["SurveyId"]

		# Query to mark with 1 Questions belonging to Survey currentSurveyId, 0 otherwise
        currentQuestionQuery =	""" SELECT *
						FROM
						(
							SELECT
								SurveyId,
								QuestionId,
								1 as InSurvey
							FROM
								SurveyStructure
							WHERE
								SurveyId = @currentSurveyId
							UNION
							SELECT 
								@currentSurveyId as SurveyId,
								Q.QuestionId,
								0 as InSurvey
							FROM
								Question as Q
							WHERE NOT EXISTS
							(
								SELECT *
								FROM SurveyStructure as S
								WHERE S.SurveyId = @currentSurveyId AND S.QuestionId = Q.QuestionId
							)
						) as t
						ORDER BY QuestionId; """

        currentQuestionQuery = currentQuestionQuery.replace("@currentSurveyId", str(currentSurveyId))

		# Run the query
        QuestionQueryTable = queryDB(connDetails, currentQuestionQuery)

        strColumnsQueryPart = ""

		# loop on the above table about questions belonging to a SurveyId or not
        for index, row in QuestionQueryTable.iterrows():

            currentSurveyIdInQuestion, currentQuestionId, currentInSurvey = row["SurveyId"], row["QuestionId"], row["InSurvey"]

			# if the question does not belong to the Survey, then the column Question<ID> = Null
            if currentInSurvey == 0:
                strColumnsQueryPart += strQueryTemplateForNullColumn.replace('<QUESTION_ID>',str(currentQuestionId))

			# if the question belongs to the Survey, then the column Question<ID> should be populated with the proper value
            else:
                strColumnsQueryPart += strQueryTemplateForAnswerQuery.replace('<QUESTION_ID>', str(currentQuestionId))

			# Add a coma only for all iterations in the loop except the last one
            if index < QuestionQueryTable.shape[0] -1:
                strColumnsQueryPart = strColumnsQueryPart + ' , ' 

	#			--Now, all the SQL for the question columns is in  @strColumnsQueryPart
	#			-- We need to build the outer SQL for the current Survey, from the template

	#			--Back in the outer loop, over the surveys

		# Replace <DYNAMIC_QUESTION_ANSWERS> in the initial query
        strCurrentUnionQueryBlock = strQueryTemplateForOuterUnionQuery.replace("<DYNAMIC_QUESTION_ANSWERS>",strColumnsQueryPart)

		# Replace <SURVEY_ID> in the initial query
        strCurrentUnionQueryBlock = strCurrentUnionQueryBlock.replace('<SURVEY_ID>', str(currentSurveyId))

		# Query for the specific SurveyId			
        strFinalQuery = strFinalQuery + strCurrentUnionQueryBlock

		# Add UNION for all the iterations of the loops over the SurveyId, except the last one
        if indexSurveyCurrent < SurveyCurrent.shape[0] - 1:
            strFinalQuery += ' UNION '

    return strFinalQuery



def createOrAlterView(viewQuery:str, connDetails:str) -> None:
    """
    This function creates a view according to the SQL query in viewQuery
    
    Input:
        - viewQuery: query to build the view
        - connDetails: connection string for pyodbc in a safe way, i.e. with try except mechanism
    """
    # Create view statement
    createViewStatement = ' CREATE OR ALTER VIEW vw_AllSurveyData AS ' + viewQuery

    # open DB connection
    cnxn = openDb(connDetails)

    # Create cursor
    cursor = cnxn.cursor()

    logging.info("Refreshing the view vw_AllSurveyData...")

    # execute statement
    try:
        cursor.execute(createViewStatement)
    except Exception as ex:
        logging.critical(ex)
        logging.critical("Cannot Create or Refresh the view vw_AllSurveyData - exiting the script")
        sys.exit(-2)


    
    # commit the changes done within the connection cnxn
    cnxn.commit()

    logging.info("View vw_AllSurveyData refreshed")

    cnxn.close()


def saveDfOnCsv(DirStore:str, fileName:str, inputDf:pd.DataFrame) -> None:
    """
    This function saves the dataframe inputDf into a csv file named filename in the folder pointed by DirStore

    Input:
        - DirStore: path of the directory that should have the pkl file
        - fileName: name of the pkl file that saves the SurveyStructure from previous runs
        - inputDf: dataframe to be saved
    """

    try:
        
        with open(os.path.join(DirStore,fileName), 'w', newline="") as f:
            inputDf.to_csv(f, index = False)
    except Exception as ex:
        logging.critical(ex)
        logging.critical(f"Cannot save the view vw_AllSurveyData in the csv file {fileName}")
        logging.critical(f"please check whether the folder {DirStore} is accessible or the file {fileName} is open - exiting from the script")
        sys.exit(-3)


def saveDfOnPkl(DirStore:str, fileName:str, inputDf:pd.DataFrame) -> None:
    """
    This function saves the dataframe inputDf into a pkl file named filename in the folder pointed by DirStore

    Input:
        - DirStore: path of the directory that should have the pkl file
        - fileName: name of the pkl file that saves the SurveyStructure from previous runs
        - inputDf: dataframe to be saved
    """
    success = True

    try:
        with open(os.path.join(DirStore,fileName), 'wb') as f:
            pickle.dump(inputDf,f)
    except Exception as ex:
        logging.warning(f"Cannot save the snapshot of SurveyStructure in the pkl file {fileName}")
        logging.warning(ex)
        success = False

    if success == True: logging.info(f"Snapshot of SurveyStructure saved on {fileName}")





#def saveView(DirStore:str, fileName:str, connDetails:str) -> str:
#    """
#    This function stores in a csv file the content of the view vw_AllSurveyData

#    Input:
#        - DirStore: path of the directory that should have the pkl file
#        - fileName: name of the pkl file that saves the SurveyStructure from previous runs
#        - connDetails: connection string for pyodbc in a safe way, i.e. with try except mechanism
#    Outpu:
#        - 
#    """

#    # open DB connection
    
#    viewDf = SQLTableToDf(connDetails, "vw_AllSurveyData", ["SurveyId"])

#    saveDfOnCsv(DirStore, fileName, viewDf)
