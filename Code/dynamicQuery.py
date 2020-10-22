import funcTools as ft

import pandas as pd

######### CONNECTION DETAILS
server = 'DESKTOP-DNQ8B1C' 
database = 'Survey_Sample_A18' 
username = 'sa' 
password = 'sonne4ever' 

connDetails = 'DRIVER={ODBC Driver 17 for SQL Server};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password

#def createDynamicQueryForView(connDetails:str) -> str:

#	#Download the content of Survey
#	SurveyCurrent = ft.SQLTableToDf(connDetails, "Survey", ["SurveyId"])

#	# Outer query template
#	strQueryTemplateForOuterUnionQuery = """
#			SELECT
#						UserId
#						, <SURVEY_ID> as SurveyId
#						, <DYNAMIC_QUESTION_ANSWERS>
#				FROM
#					[User] as u
#				WHERE EXISTS
#				(
#						SELECT *
#						FROM Answer as a
#						WHERE u.UserId = a.UserId
#						AND a.SurveyId = <SURVEY_ID>
#				) """

#	# Template for answer referring to questions which are part of the SurveyId in object
#	strQueryTemplateForAnswerQuery = """
#				COALESCE(
#					(
#						SELECT a.Answer_Value
#						FROM Answer as a
#						WHERE
#							a.UserId = u.UserId
#							AND a.SurveyId = <SURVEY_ID>
#							AND a.QuestionId = <QUESTION_ID>
#					), -1) AS ANS_Q<QUESTION_ID> """

#	# Template for answer referring to questions which are NOT part of the SurveyId in object
#	strQueryTemplateForNullColumn = ' NULL AS ANS_Q<QUESTION_ID> '

#	# initialize
#	strCurrentUnionQueryBlock = ''

#	strFinalQuery = ''

#	# Outer loop on each row of Survey table
#	for indexSurveyCurrent, rowSurveyId in SurveyCurrent.iterrows():

#		# Extract only the field SurveyId
#		currentSurveyId = rowSurveyId["SurveyId"]
	
#		# Query to mark with 1 Questions belonging to Survey currentSurveyId, 0 otherwise
#		currentQuestionQuery =	""" SELECT *
#						FROM
#						(
#							SELECT
#								SurveyId,
#								QuestionId,
#								1 as InSurvey
#							FROM
#								SurveyStructure
#							WHERE
#								SurveyId = @currentSurveyId
#							UNION
#							SELECT 
#								@currentSurveyId as SurveyId,
#								Q.QuestionId,
#								0 as InSurvey
#							FROM
#								Question as Q
#							WHERE NOT EXISTS
#							(
#								SELECT *
#								FROM SurveyStructure as S
#								WHERE S.SurveyId = @currentSurveyId AND S.QuestionId = Q.QuestionId
#							)
#						) as t
#						ORDER BY QuestionId; """
	
#		currentQuestionQuery = currentQuestionQuery.replace("@currentSurveyId", str(currentSurveyId))

#		# Run the query
#		QuestionQueryTable = ft.queryDB(connDetails, currentQuestionQuery)
	
#		strColumnsQueryPart = ""
	
#		# loop on the above table about questions belonging to a SurveyId or not
#		for index, row in QuestionQueryTable.iterrows():
		
#			currentSurveyIdInQuestion, currentQuestionId, currentInSurvey = row["SurveyId"], row["QuestionId"], row["InSurvey"]
		
#			# if the question does not belong to the Survey, then the column Question<ID> = Null
#			if currentInSurvey == 0:
#				strColumnsQueryPart += strQueryTemplateForNullColumn.replace('<QUESTION_ID>',str(currentQuestionId))
		
#			# if the question belongs to the Survey, then the column Question<ID> should be populated with the proper value
#			else:
#				strColumnsQueryPart += strQueryTemplateForAnswerQuery.replace('<QUESTION_ID>', str(currentQuestionId))
		
#			# Add a coma only for all iterations in the loop except the last one
#			if index < QuestionQueryTable.shape[0] -1:
#				strColumnsQueryPart = strColumnsQueryPart + ' , ' 

#	#			--Now, all the SQL for the question columns is in  @strColumnsQueryPart
#	#			-- We need to build the outer SQL for the current Survey, from the template

#	#			--Back in the outer loop, over the surveys
	
#		# Replace <DYNAMIC_QUESTION_ANSWERS> in the initial query
#		strCurrentUnionQueryBlock = strQueryTemplateForOuterUnionQuery.replace("<DYNAMIC_QUESTION_ANSWERS>",strColumnsQueryPart)
	
#		# Replace <SURVEY_ID> in the initial query
#		strCurrentUnionQueryBlock = strCurrentUnionQueryBlock.replace('<SURVEY_ID>', str(currentSurveyId))

#		# Query for the specific SurveyId			
#		strFinalQuery = strFinalQuery + strCurrentUnionQueryBlock
	
#		# Add UNION for all the iterations of the loops over the SurveyId, except the last one
#		if indexSurveyCurrent < SurveyCurrent.shape[0] - 1:
#			strFinalQuery += ' UNION '

#	return strFinalQuery

result = ft.queryDB(connDetails, ft.createDynamicQueryForView(connDetails) )

print(result.head(4))

print(result.shape)


