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

def createDynamicQueryForView(connDetails:str) -> str:
    """
    
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

