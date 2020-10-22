import funcTools as ft

import pandas as pd

######### CONNECTION DETAILS
server = 'DESKTOP-DNQ8B1C' 
database = 'Survey_Sample_A18' 
username = 'sa' 
password = 'sonne4ever' 

connDetails = 'DRIVER={ODBC Driver 17 for SQL Server};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password








cnxn = ft.openDb(connDetails)

cursor = cnxn.cursor()

# columns of Survey = SurveyId, SurveyDescription, Survey_UsesrAdminId    
SurveyCurrent = ft.SQLTableToDf(database, cnxn, "Survey", ["SurveyId"])

# Columns = QuestionId, SurveyId, UserId, Answer_Value
AnswerCurrent = ft.SQLTableToDf(database, cnxn, "Answer", ["SurveyId"])




#	DECLARE @strQueryTemplateForOuterUnionQuery  nvarchar(max);
#	DECLARE @strQueryTemplateForAnswerQuery  nvarchar(max);
#	DECLARE @strQueryTemplateForNullColumn  nvarchar(max);

#	DECLARE @currentSurveyId int;

#	--When writing a dynamic SQL query
#	-- Leave space between the text markers which have to replaced
#	--	e.g <SURVEY_ID>, <DYNAMIC_QUESTION_ANSWERS>
#	-- Make sure that the markers do not colide with any "real" SQL clauses, values, etc.
	
#	-- This one is the bloc around UNION for each survey
#	SET @strQueryTemplateForOuterUnionQuery = '
		#SELECT
		#			UserId
		#			, <SURVEY_ID> as SurveyId
		#			, <DYNAMIC_QUESTION_ANSWERS>
		#	FROM
		#		[User] as u
		#	WHERE EXISTS
		#	(
		#			SELECT *
		#			FROM Answer as a
		#			WHERE u.UserId = a.UserId
		#			AND a.SurveyId = <SURVEY_ID>
		#	) ' ;

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

#	-- this one will be used to iteratively replace <DYNAMIC_QUESTION_ANSWERS>

#	-- Look at the templates, they do not end with a comma
	#SET @strQueryTemplateForAnswerQuery = '
	#		COALESCE(
	#			(
	#				SELECT a.Answer_Value
	#				FROM Answer as a
	#				WHERE
	#					a.UserId = u.UserId
	#					AND a.SurveyId = <SURVEY_ID>
	#					AND a.QuestionId = <QUESTION_ID>
	#			), -1) AS ANS_Q<QUESTION_ID> ';

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


#	SET @strQueryTemplateForNullColumn = ' NULL AS ANS_Q<QUESTION_ID> ';

strQueryTemplateForNullColumn = ' NULL AS ANS_Q<QUESTION_ID> '

#	DECLARE @strCurrentUnionQueryBlock nvarchar(max);
#	SET @strCurrentUnionQueryBlock = '';

strCurrentUnionQueryBlock = ''

#	DECLARE @strFinalQuery nvarchar(max);
#	SET @strFinalQuery = '';

strFinalQuery = ''

#	--LOOP HERE -> Iterating over the surveys
#	-- Iteration over data from a table --> with a CURSOR
#	-- A CURSOR is essentially a file pointer
#	-- Iteration row after row over a resultset
#	-- A CURSOR is a variable, needed declared over a query

#	--STRONG SYNTAX PARTICULARITY --> cursor variables DO NOT bear an @ symbol
#	-- This doesn't "open" anything, the query is not run at this stage
#	DECLARE surveyCursor CURSOR FOR
#								SELECT SurveyId
#								FROM Survey
#								ORDER BY SurveyId;

#	OPEN surveyCursor; -- When open, the cursor is "before" the first row 
#						-- of the resultset
	
#	--We must iterate until "EOF", the end of the resultset
#	-- If the resultset has multiple columns, the order of columns
#	-- must be respected when feeding into local variables
#	FETCH NEXT FROM surveyCursor INTO @currentSurveyId;

# need data from current SurveyId

for indexSurveyCurrent, rowSurveyId in SurveyCurrent.iterrows():

	#currentSurveyId = SurveyId
	currentSurveyId = rowSurveyId["SurveyId"]

	print(currentSurveyId)

#	WHILE @@FETCH_STATUS = 0 
#	--If @@FETCH_STATUS is equal to 0, then a row can be read
#	BEGIN
			
#			-- Main loop, iterating over the surveys
#			-- For each survey, we have @currentSurveyId
#			-- We need to construct the column "queries"

#			-- We need to iterate of the survey questions
#			-- given by the table SurveyStructure

#			DECLARE currentQuestionCursor CURSOR FOR
			#SELECT *
			#		FROM
			#		(
			#			SELECT
			#				SurveyId,
			#				QuestionId,
			#				1 as InSurvey
			#			FROM
			#				SurveyStructure
			#			WHERE
			#				SurveyId = @currentSurveyId
			#			UNION
			#			SELECT 
			#				@currentSurveyId as SurveyId,
			#				Q.QuestionId,
			#				0 as InSurvey
			#			FROM
			#				Question as Q
			#			WHERE NOT EXISTS
			#			(
			#				SELECT *
			#				FROM SurveyStructure as S
			#				WHERE S.SurveyId = @currentSurveyId AND S.QuestionId = Q.QuestionId
			#			)
			#		) as t
			#		ORDER BY QuestionId;


			
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

	print(currentQuestionQuery)

	QuestionQueryTable = pd.read_sql_query(currentQuestionQuery, cnxn)
	
#			DECLARE @currentSurveyIdInQuestion int;
#			DECLARE @currentQuestionId int;
#			DECLARE @currentInSurvey int; 

#			OPEN currentQuestionCursor;
#			-- Remember to follow the order of the resultset heading
#			FETCH NEXT FROM currentQuestionCursor INTO	@currentSurveyIdInQuestion,
#														@currentQuestionId,
#														@currentInSurvey;
			
#			DECLARE @strColumnsQueryPart nvarchar(max);
#			--Remember that in all algorithms involving string concatenation
#			-- the inital string variable must be initialised to the empty string
#			SET @strColumnsQueryPart = '';
	strColumnsQueryPart = ""
	
	for index, row in QuestionQueryTable.iterrows():
		#print(row)
		currentSurveyIdInQuestion, currentQuestionId, currentInSurvey = row["SurveyId"], row["QuestionId"], row["InSurvey"]
		print(currentSurveyIdInQuestion)
		print(currentQuestionId)
		print(currentInSurvey)

#			WHILE @@FETCH_STATUS = 0
#			-- The "global" variable @@FETCH_STATUS is now "localised" to the last call of FETCH NEXT
#			BEGIN
#				-- This loop iterates over the resultset indicating whether a question is member of
#				-- the current survey, thanks to the bound local variable @currentInSurvey

#				IF @currentInSurvey = 0
#				-- Current Question NOT in Current Survey
#					BEGIN
#						--Use our SQL templates to proceed with text replacement
#						SET @strColumnsQueryPart = @strColumnsQueryPart 
#							+ REPLACE(@strQueryTemplateForNullColumn
#									, '<QUESTION_ID>', @currentQuestionId);
#					END
		if currentInSurvey == 0:
			strColumnsQueryPart += strQueryTemplateForNullColumn.replace('<QUESTION_ID>',str(currentQuestionId))
			#print(strColumnsQueryPart)

		else:
			strColumnsQueryPart += strQueryTemplateForAnswerQuery.replace('<QUESTION_ID>', str(currentQuestionId))
			#print(strColumnsQueryPart)
			
#				ELSE
#				-- Current Question in Current Survey
#					BEGIN
#						SET @strColumnsQueryPart = @strColumnsQueryPart
#							+ REPLACE(@strQueryTemplateForAnswerQuery
#								, '<QUESTION_ID>', @currentQuestionId);

#						-- This would be ok, but no need to replace the marker <SURVEY_ID> now
#						-- as there will be a global replacement when we have finished to build
#						-- the whole query block to be UNIONed
#							-- SET @strColumnsQueryPart = REPLACE(@strColumnsQueryPart
#							--		, '<SURVEY_ID>', @currentSurveyId);
#					END

				
#				FETCH NEXT FROM currentQuestionCursor INTO	@currentSurveyIdInQuestion,
#														@currentQuestionId,
#														@currentInSurvey;
		if index < QuestionQueryTable.shape[0] -1:
			strColumnsQueryPart = strColumnsQueryPart + ' , ' 
#				-- Column list construction
#				-- A comma must be placed after the last block, as long as
#				-- a next row is to be read by the cursor
#				-- We still have question columns to add
#				IF @@FETCH_STATUS = 0
#				BEGIN
#					-- After the FETCH NEXT, is there a row to read following? Yes, we put a comma
#					SET @strColumnsQueryPart = @strColumnsQueryPart + ' , ' ;
#				END;


#			END; -- end of while over currentQuestionCursor

#			CLOSE currentQuestionCursor;
#			DEALLOCATE currentQuestionCursor;

#			--Now, all the SQL for the question columns is in  @strColumnsQueryPart
#			-- We need to build the outer SQL for the current Survey, from the template

#			--Back in the outer loop, over the surveys

#			SET @strCurrentUnionQueryBlock = REPLACE(@strQueryTemplateForOuterUnionQuery
#											, '<DYNAMIC_QUESTION_ANSWERS>'
#											, @strColumnsQueryPart);
	strCurrentUnionQueryBlock = strQueryTemplateForOuterUnionQuery.replace("<DYNAMIC_QUESTION_ANSWERS>",strColumnsQueryPart)
#			
#			SET @strCurrentUnionQueryBlock =  REPLACE(@strCurrentUnionQueryBlock
#											  , '<SURVEY_ID>'
#											  , @currentSurveyId);
	strCurrentUnionQueryBlock = strCurrentUnionQueryBlock.replace('<SURVEY_ID>', str(currentSurveyId))
			
#			SET @strFinalQuery = @strFinalQuery +  @strCurrentUnionQueryBlock
	strFinalQuery = strFinalQuery + strCurrentUnionQueryBlock
#		FETCH NEXT FROM surveyCursor INTO @currentSurveyId;

#		IF @@FETCH_STATUS = 0
#		BEGIN
#			-- There's more surveys to read
#			SET @strFinalQuery = @strFinalQuery + ' UNION ' ;
#		END;
	if indexSurveyCurrent < SurveyCurrent.shape[0] - 1:
		strFinalQuery += ' UNION '

#	END; --end of while over surveyCursor

#	CLOSE surveyCursor;
#	DEALLOCATE surveyCursor;

print(strFinalQuery)
#	RETURN @strFinalQuery;

result = pd.read_sql_query(strFinalQuery, cnxn)

print(result.head(4))

print(result.shape)