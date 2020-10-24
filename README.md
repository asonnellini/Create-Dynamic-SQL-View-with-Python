# Create-Dynamic-SQL-View-with-Python


This repository hosts the python scripts that I produced for the DSTI joint Assignment "software engineering & Data Wrangling".

The target of the assignment is to dump in a csv file the content of a Database View that hosts pivoted data about the outcome of a Survey.
The Database View relies on the content of the Table SurveyStructure and, upon running the script, it is refreshed everytime SurveyStructure changed compared to the last time the script was run. This feature emulates the behavior of an SQL Trigger that is designed to be triggered upon UPDATE, DELETE, INSERT  actions on the SurveyStructure Table.

All the scripts are in the folder Code, please see the section below for instructions as to how run the script.


## How to run the script
To use the script::

- ``` git clone https://github.com/asonnellini/Create-Dynamic-SQL-View-with-Python.git ```
- ``` cd Create-Dynamic-SQL-View-with-Python/Code ```
- To have an overview of all the possible flags, their meaning and their default value:
	- ``` python extract-view.py -h ```

- To run the script:
	- ``` python extract-view.py <DBuser> <DBpwd> --server <DBServer> --DB <DBName> --DirStore <DirectoryWhereStoreFiles> --oldSnap <NamePklFileWithPreviousSnapSurveyStructure> --csv <CSVNameOutcomeResults> ```

	- Note: the non-mandatory flags oldSnap and csv have default values which should be fine in most cases



 ## Test scenarios
 
 The robustness of the script has been tested against the below adver scenarios:
 
- cannot open the connection with DB ==> raise error message + exit script
- cannot save the view in the csv ==> raise error message + exit
- cannot load the pkl file ==> raise a warning messag and continue refreshing the view
- cannot save the pkl file ==> raise a warning messag and continue 
 
 The trigger-like behavior was tested against the below scenarios:

- change the SurveyStructure Table with:
	- INSERT INTO SurveyStructure (SurveyId, QuestionId, OrdinalValue) VALUES	(3, 2, 1) ==> upon running the script, the column ANS_Q2 of the view changed from NULL to -1 = View was updated = OK
	- DELETE FROM SurveyStructure WHERE SurveyId = 3 AND QuestionId = 2 AND OrdinalValue = 1 ==> upon running the script, the column ANS_Q2 of the view changed from -1 to NULL = View was updated = OK
	- UPDATE SurveyStructure SET QuestionId = 2 WHERE SurveyId = 3 AND QuestionId = 1 AND OrdinalValue = 1 ==> upon running the script, ANS_Q1 was 10 and ANS_Q2 was NULL, now they are NULL and -1 OK!