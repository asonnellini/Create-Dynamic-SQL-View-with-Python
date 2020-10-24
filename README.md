# Create-Dynamic-SQL-View-with-Python

The target of the scripts in this repository is to dump in a csv file the content of a Database View that hosts pivoted data about the outcome of a Survey.
The Database View relies on the content of the Table SurveyStructure and, upon running the script, it is refreshed everytime SurveyStructure changed compared to the last time the script was run. This feature emulates the behavior of an SQL Trigger that is designed to be triggered upon UPDATE, DELETE, INSERT  actions on the SurveyStructure Table.

The script is meant to install automatically all needed python modules in case they are installed yet.

The script is NOT implemented via OOP.

All the scripts are in the folder Code, please see the section "How to run the script" below for instructions as to how run the script.

This work was done to complete the DSTI joint Assignment "Software Engineering part 2 & Data Wrangling".


## How to run the script
To use the script::

- ``` git clone https://github.com/asonnellini/Create-Dynamic-SQL-View-with-Python.git ```
- ``` cd Create-Dynamic-SQL-View-with-Python/Code ```
- To have an overview of all the possible flags, their meaning and their default value:
	- ``` python extract-view.py -h ```

- To run the script:
	- ``` python extract-view.py <DBuser> <DBpwd> --server <DBServer> --DB <DBName> --DirStore <DirectoryWhereStoreFiles> --oldSnap <NamePklFileWithPreviousSnapSurveyStructure> --csv <CSVNameOutcomeResults> ```

	- Note: the non-mandatory flags oldSnap and csv have default values which should be fine in most cases

Assuming default values for the flags --oldSnap and --csv are used, upon execution of the scritp, the following 2 files will be created in the directory specified in the flag --DirStore:
- SurveyStructure.pkl  --> this file will host the current snapshot of the table SurveyStructure
- SurveyOutcome.csv --> this file will host the up-to-date content of the view vw_AllSurveyData


 ## Test scenarios
 
 The robustness of the script has been tested against the below "adverse" scenarios:
 
- cannot connect to the DB ==> raise error message + exit script returning -1
- cannot execute the "CREATE OR ALTER VIEW" command ==> raise error message + exit script returning -2
- cannot save the view in the csv ==> raise error message + exit script returning -3
- cannot load the pkl file ==> raise a warning messag and continue refreshing the view
- cannot save the pkl file ==> raise a warning messag and continue 
 
 The trigger-like behavior was tested against the below scenarios:

- change the SurveyStructure Table with:
	- INSERT INTO SurveyStructure (SurveyId, QuestionId, OrdinalValue) VALUES	(3, 2, 1) ==> upon running the script, the column ANS_Q2 of the view changed from NULL to -1 (i.e. View was updated) = OK
	- DELETE FROM SurveyStructure WHERE SurveyId = 3 AND QuestionId = 2 AND OrdinalValue = 1 ==> upon running the script, the column ANS_Q2 of the view changed from -1 to NULL (i.e. View was updated) = OK
	- UPDATE SurveyStructure SET QuestionId = 2 WHERE SurveyId = 3 AND QuestionId = 1 AND OrdinalValue = 1 ==> upon running the script, ANS_Q1 was 10 and ANS_Q2 was NULL, now they are NULL and -1 OK!