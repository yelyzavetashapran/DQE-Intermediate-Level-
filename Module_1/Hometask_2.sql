
CREATE PROCEDURE Analyzer (@p_DatabaseName NVARCHAR(30), @p_SchemaName NVARCHAR(30), @p_TableName NVARCHAR(30))
AS
BEGIN


DECLARE @v_Query varchar(MAX);
DECLARE @result_1 TABLE (
	[Database_name] NVARCHAR(100),
	[Schema_name] NVARCHAR(100),
	[Table_name] NVARCHAR(100),
	[Column_name] NVARCHAR(100),
	[Data_type] NVARCHAR(100),
	[Data_type_raw] NVARCHAR(100)
);
DECLARE @result_2 TABLE (
	[Table_name] NVARCHAR(100),
	[Column_name] NVARCHAR(100),
	[unique_rows_cnt] INT,
	[cnt_null] INT,
	[cnt_all] INT
);
DECLARE @result_3 TABLE (
	[Table_name] NVARCHAR(100),
	[Column_name] NVARCHAR(100),
	[cnt_empty]  NVARCHAR(100),
	[only_upper] NVARCHAR(100),
	[only_lower] NVARCHAR(100),
	[min_val] NVARCHAR(100),
	[max_val] NVARCHAR(100)
);

-- cte and query for first part of data
WITH first_query AS(
	SELECT
		CASE
			WHEN  @p_TableName != '%' 
		THEN 
			'SELECT 
			a.TABLE_CATALOG as Database_name,
			a.TABLE_SCHEMA AS Schema_name,
			a.TABLE_NAME AS Table_name,
			b.COLUMN_NAME AS Column_name,
			CASE WHEN b.CHARACTER_MAXIMUM_LENGTH IS NOT NULL THEN b.DATA_TYPE + ''('' + CAST(b.CHARACTER_MAXIMUM_LENGTH AS varchar) + '')'' ELSE b.DATA_TYPE END AS Data_type,
			CASE WHEN b.CHARACTER_MAXIMUM_LENGTH IS NOT NULL THEN b.DATA_TYPE + ''('' + CAST(b.CHARACTER_MAXIMUM_LENGTH AS varchar) + '')'' ELSE b.DATA_TYPE END AS Data_type_raw
			FROM ['+@p_DatabaseName+'].[INFORMATION_SCHEMA].[TABLES] a
			JOIN ['+@p_DatabaseName+'].[INFORMATION_SCHEMA].[COLUMNS] b 
			ON a.TABLE_CATALOG = b.TABLE_CATALOG and a.TABLE_SCHEMA=b.TABLE_SCHEMA and a.TABLE_NAME = b.TABLE_NAME
			WHERE a.TABLE_SCHEMA = ''' + @p_SchemaName + '''  and a.TABLE_NAME =  ''' + @p_TableName + ''''
		ELSE 
			'SELECT 
			a.TABLE_CATALOG as Database_name,
			a.TABLE_SCHEMA AS Schema_name,
			a.TABLE_NAME AS Table_name,
			b.COLUMN_NAME AS Column_name,
			CASE WHEN b.CHARACTER_MAXIMUM_LENGTH IS NOT NULL THEN b.DATA_TYPE + ''('' + CAST(b.CHARACTER_MAXIMUM_LENGTH AS varchar) + '')'' ELSE b.DATA_TYPE END AS Data_type,
			CASE WHEN b.CHARACTER_MAXIMUM_LENGTH IS NOT NULL THEN b.DATA_TYPE + ''('' + CAST(b.CHARACTER_MAXIMUM_LENGTH AS varchar) + '')'' ELSE b.DATA_TYPE END AS Data_type_raw
			FROM ['+@p_DatabaseName+'].[INFORMATION_SCHEMA].[TABLES] a
			JOIN ['+@p_DatabaseName+'].[INFORMATION_SCHEMA].[COLUMNS] b 
			ON a.TABLE_CATALOG = b.TABLE_CATALOG and a.TABLE_SCHEMA=b.TABLE_SCHEMA and a.TABLE_NAME = b.TABLE_NAME
			WHERE a.TABLE_SCHEMA = ''' + @p_SchemaName + ''''
		END 
			[query_text]
)


-- first part data
SELECT @v_Query = [query_text] FROM first_query;
INSERT INTO @result_1([Database_name], [Schema_name], [Table_name], [Column_name], [Data_type], [Data_type_raw])
EXEC (@v_Query);







-- cte and query for second part of data
WITH cte_2 as (
SELECT
		[Database_name] + '.' + [Schema_name] + '.' + [Table_name] [path],
		[Table_name],
		[Column_name],
		[Schema_name],
		[Database_name],
		LEAD([Table_name] + '.' + [Column_name]) OVER (ORDER BY [Table_name] + '.' + [Column_name]) [lead_row]

FROM @result_1),
	secong_query AS
	(
		SELECT
			CASE
				WHEN [lead_row] IS NOT NULL THEN 
					'SELECT 
						'''+[Table_name]+''', 
						'''+[Column_name]+''', 
												
						COUNT(DISTINCT('+[Column_name]+')) [dist_rows_cnt],
						(SELECT COUNT(*) FROM '+[path]+'  WHERE '+[Column_name]+' IS NULL) [cnt_null], 
						(SELECT COUNT(*)  FROM '+[path]+'  ) [cnt_all]
					FROM '+[path]+'  UNION ALL '
				ELSE
					'SELECT 
						'''+[Table_name]+''', 
						'''+[Column_name]+''', 
						COUNT(DISTINCT('+[Column_name]+')) [dist_rows_cnt],
						(SELECT COUNT(*) FROM '+[path]+' WHERE '+[Column_name]+' IS NULL) [cnt_null], 
						(SELECT COUNT(*)  FROM '+[path]+'  ) [cnt_all]
					FROM '+[path]+'  '
				END [query_text]
		FROM cte_2
	)


-- second part data (only counts)
SELECT
	@v_Query = STRING_AGG(CAST([query_text] AS nvarchar(MAX)), '') WITHIN GROUP (ORDER BY [query_text])
FROM secong_query;
INSERT INTO @result_2([Table_name], [Column_name], [unique_rows_cnt], [cnt_null], [cnt_all])
EXEC (@v_Query);



-- cte and query for next part of date
WITH cte_3 as (
SELECT
		[Database_name] + '.' + [Schema_name] + '.' + [Table_name] [path],
		[Table_name],
		[Column_name],
		[Schema_name],
		[Database_name],
		[Data_type],
		LEAD([Table_name] + '.' + [Column_name]) OVER (ORDER BY [Table_name] + '.' + [Column_name]) [lead_row]

FROM @result_1),
	third_query AS
	(
		SELECT
			CASE
				WHEN [lead_row] IS NOT NULL AND [Data_type] LIKE 'char%' OR [Data_type] LIKE 'varchar%' OR [Data_type] LIKE 'text%' OR [Data_type] LIKE 'nchar%' OR [Data_type] LIKE 'nvarchar%' OR [Data_type] LIKE 'ntext%' OR [Data_type] LIKE 'binary%' OR [Data_type] LIKE 'varbinary%' 
				THEN 'SELECT 
						'''+[Table_name]+''', 
						'''+[Column_name]+''',
						CAST(MIN('+[Column_name]+') as varchar(100)) [min_val],
						CAST(MAX('+[Column_name]+') as varchar(100))  [max_val], 
						CAST((SELECT COUNT(*) FROM '+[path]+'  WHERE '+[Column_name]+' = '''')  AS VARCHAR(100)),
						CAST((SELECT COUNT(*) FROM '+[path]+'  WHERE  '+[Column_name]+'  LIKE ''%[A-Z]%'' AND '+[Column_name]+' = UPPER('+[Column_name]+') collate SQL_Latin1_General_CP1_CS_AS) AS VARCHAR(100)),
						CAST((SELECT COUNT(*) FROM '+[path]+'  WHERE '+[Column_name]+'  LIKE ''%[A-Z]%'' AND '+[Column_name]+' = LOWER('+[Column_name]+') collate SQL_Latin1_General_CP1_CS_AS) AS VARCHAR(100))
					FROM '+[path]+' UNION ALL '
				WHEN [lead_row] IS NOT NULL AND [Data_type] IN ('tinyint', 'smallint', 'int', 'bigint', 'decimal', 'numeric', 'smallmoney', 'money', 'float', 'real') 
				THEN 'SELECT 
						'''+[Table_name]+''', 
						'''+[Column_name]+''',
						CAST(MIN('+[Column_name]+') as varchar(100)) [min_val],
						CAST(MAX('+[Column_name]+') as varchar(100))  [max_val], 
						CAST((SELECT COUNT(*) FROM '+[path]+'  WHERE '+[Column_name]+' = 0) AS VARCHAR(100)),
						''numeric column'', 
						''numeric column''
					FROM '+[path]+' UNION ALL '
				
				
				
				
				WHEN [lead_row] IS NULL AND [Data_type] LIKE 'char%' OR [Data_type] LIKE 'varchar%' OR [Data_type] LIKE 'text%' OR [Data_type] LIKE 'nchar%' OR [Data_type] LIKE 'nvarchar%' OR [Data_type] LIKE 'ntext%' OR [Data_type] LIKE 'binary%' OR [Data_type] LIKE 'varbinary%' 
				THEN 'SELECT 
						'''+[Table_name]+''', 
						'''+[Column_name]+''',
						CAST(MIN('+[Column_name]+') as varchar(100)) [min_val],
						CAST(MAX('+[Column_name]+') as varchar(100))  [max_val], 
						CAST((SELECT COUNT(*) FROM '+[path]+'  WHERE '+[Column_name]+' = '''') AS VARCHAR(100)),
						CAST((SELECT COUNT(' + [Column_name] + ') FROM '+[path]+'  WHERE '+[Column_name]+'  LIKE ''%[A-Z]%'' AND  '+[Column_name]+' = upper('+[Column_name]+') collate SQL_Latin1_General_CP1_CS_AS) AS VARCHAR(100)),
						CAST((SELECT COUNT(*) FROM '+[path]+' WHERE '+[Column_name]+'  LIKE ''%[A-Z]%'' AND '+[Column_name]+' = LOWER('+[Column_name]+') collate SQL_Latin1_General_CP1_CS_AS) AS VARCHAR(100))
					FROM '+[path]+' '
				WHEN [lead_row] IS NULL AND [Data_type] IN ('tinyint', 'smallint', 'int', 'bigint', 'decimal', 'numeric', 'smallmoney', 'money', 'float', 'real') 
				THEN 'SELECT 
						'''+[Table_name]+''', 
						'''+[Column_name]+''',
						CAST(MIN('+[Column_name]+') as varchar(100)) [min_val],
						CAST(MAX('+[Column_name]+') as varchar(100))  [max_val], 
						CAST((SELECT COUNT(*) FROM '+[path]+'  WHERE '+[Column_name]+' = 0) AS VARCHAR(100)),
						''numeric column'', 
						''numeric column''
					FROM '+[path]+' '

				WHEN [lead_row] IS NOT NULL AND [Data_type] IN ('date') THEN 'SELECT '''+[Table_name]+''',
																					'''+[Column_name]+''', 
																					CAST(MIN('+[Column_name]+') as varchar(100)) [min_val],
																					CAST(MAX('+[Column_name]+') as varchar(100))  [max_val], 
																					CAST((SELECT COUNT(*) FROM '+[path]+'  WHERE '+[Column_name]+' = '''') AS VARCHAR(100)), 
																					''N/A'', ''N/A'' 
																					FROM '+[path]+' 
																					'
				WHEN [lead_row] IS NULL AND [Data_type] IN ('date') THEN 'SELECT '''+[Table_name]+''', 
																				'''+[Column_name]+''', 
																					CAST(MIN('+[Column_name]+') as varchar(100)) [min_val],
																					CAST(MAX('+[Column_name]+') as varchar(100))  [max_val], 
																					 CAST((SELECT COUNT(*) FROM '+[path]+'  WHERE '+[Column_name]+' = '''') AS VARCHAR(100)), 
																					 ''N/A'', ''N/A'' 
																					FROM '+[path]+' 
																					 '
				
				WHEN [lead_row] IS NOT NULL  THEN 'SELECT '''+[Table_name]+''', '''+[Column_name]+''', ''Data Type not numberic/string'', ''N/A'', ''N/A'', ''N/A'', ''N/A'' '
				WHEN [lead_row] IS NULL THEN 'SELECT '''+[Table_name]+''', '''+[Column_name]+''', ''Data Type not numberic/string'' , ''N/A'', ''N/A'', ''N/A'', ''N/A'' '
							
				END [query_text]
		FROM cte_3
	)


-- third part data
SELECT
	@v_Query = STRING_AGG(CAST([query_text] AS nvarchar(MAX)), '') WITHIN GROUP (ORDER BY [query_text])
FROM third_query;
INSERT INTO @result_3([Table_name], [Column_name], [min_val], [max_val], [cnt_empty], [only_upper], [only_lower])
EXEC (@v_Query);




-- last select
SELECT 
	a.[Database_name], 
	a.[Schema_name], 
	a.[Table_name], 
	b.[cnt_all] as 'Table total row count', 
	a.[Column_name], a.[Data_type], 
	b.[unique_rows_cnt] as 'Count of DISTINCT values', 
	b.[cnt_null] as 'Count of NULL values',
	c.[cnt_empty] as 'Count of empty/zero values',
	c.[only_upper] as 'Only UPPERCASE strings',
	c.[only_lower]  as 'Only LOWERCASE strings',
	c.[min_val]  as 'MIN value',
	c.[max_val]  as 'MAX value'
	FROM @result_1 a
JOIN @result_2 b ON a.[Table_name] = b.[Table_name] and a.[Column_name] = b.[Column_name]
JOIN @result_3 C on a.[Table_name] = c.[Table_name] and a.[Column_name] = c.[Column_name]

END

-- execution of storded procedure
--EXEC Analyzer 'TRN', 'hr', '%'
