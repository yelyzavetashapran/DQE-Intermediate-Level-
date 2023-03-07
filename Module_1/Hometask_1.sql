WITH
	json_string AS
	(
		SELECT '[{"employee_id": "5181816516151", "department_id": "1", "class": "src\bin\comp\json"}, {"employee_id": "925155", "department_id": "1", "class": "src\bin\comp\json"}, {"employee_id": "815153", "department_id": "2", "class": "src\bin\comp\json"}, {"employee_id": "967", "department_id": "", "class": "src\bin\comp\json"}]' [str]
	),
    trimed AS
    (
    SELECT TRIM('[]' FROM (SELECT [str] FROM json_string)) AS [trimed_str]
    ),
	recursive_cte AS
	(
    SELECT
		CAST(trim(' " : ' FROM right(left([trimed_str], CHARINDEX('", "', [trimed_str])), len(left([trimed_str], CHARINDEX('", "', [trimed_str]))) - CHARINDEX('": "', [trimed_str]))) AS bigint) [employee_id],
		CAST(trim(' " : ' FROM replace(right(left([trimed_str], CHARINDEX('", "class"', [trimed_str])), len(left([trimed_str], CHARINDEX('", "class"', [trimed_str]))) - CHARINDEX('"department_id": "', [trimed_str])), 'department_id": ', '')) AS int) [department_id],
		STUFF([trimed_str], 1, CHARINDEX('}', [trimed_str]), '') [string]
    FROM
        trimed

    UNION ALL

    SELECT
		CAST(trim(' " : ' FROM right(left([string], CHARINDEX('", "', [string])), len(left(string, CHARINDEX('", "', [string]))) - CHARINDEX('": "', [string]))) AS bigint) [employee_id],
		NULLIF(CAST(trim(' " : ' FROM replace(right(left([string], CHARINDEX('", "class"', [string])), len(left([string], CHARINDEX('", "class"', [string]))) - CHARINDEX('"department_id": "', [string])), 'department_id": ', '')) AS int), 0) [department_id],
		STUFF([string], 1, CHARINDEX('}', [string]), '') [string]
    FROM
        recursive_cte
    WHERE CHARINDEX('employee_id', [string]) <>  0
    )

select [employee_id], [department_id] from recursive_cte