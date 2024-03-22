import typer
from typing_extensions import Annotated
import bcpy;
import pandas as pd;
import pyodbc;
import tempfile;
from pathlib import Path;
import os
SCRIPT_DIRECTORY = os.path.dirname(os.path.abspath(__file__))


app = typer.Typer()

QUERY_CONNECTION_STRING = (
    r'DRIVER={ODBC Driver 17 for SQL Server};'
    r'SERVER=dev-sql1.gravitate.energy;'
    r'DATABASE=pe_dev;'
    r'UID=matt.rothmeyer;'
    r'PWD=6FG5HP?ENomF5&bT'
)


@app.command("query-and-write-files")
def query_and_write_files():
    temporary_directory = SCRIPT_DIRECTORY
    query_results = query_data()
    curve_points = query_results[0]
    curve_point_prices = query_results[1]
    quoted_values = query_results[2]
    formula_results = query_results[3]
    curve_points.to_csv(Path(temporary_directory) / 'curve_points.csv', index=False)
    curve_point_prices.to_csv(Path(temporary_directory) / 'curve_point_prices.csv', index=False)
    quoted_values.to_csv(Path(temporary_directory) / 'quoted_values.csv', index=False)
    formula_results.to_csv(Path(temporary_directory) / 'formula_results.csv', index=False)
    
    
@app.command("import-files")
def import_files():
    temporary_directory = SCRIPT_DIRECTORY
    copy_csv_to_sql(Path(temporary_directory) / 'curve_points.csv', 'CurvePoint')
    # copy_csv_to_sql(Path(temporary_directory) / 'formula_results.csv', 'FormulaResult')
    # copy_csv_to_sql(Path(temporary_directory) / 'curve_point_prices.csv', 'CurvePointPrice')
    # copy_csv_to_sql(Path(temporary_directory) / 'quoted_values.csv', 'QuoteValue')


@app.command("reset-and-pull")
def main():
    
    query_results = query_data()
    curve_points = query_results[0]
    curve_point_prices = query_results[1]
    quoted_values = query_results[2]
    formula_results = query_results[3]

    with tempfile.TemporaryDirectory() as temporary_directory:
        print('created temporary directory', temporary_directory)
        curve_points.to_csv(Path(temporary_directory) / 'curve_points.csv', index=False)
        curve_point_prices.to_csv(Path(temporary_directory) / 'curve_point_prices.csv', index=False)
        quoted_values.to_csv(Path(temporary_directory) / 'quoted_values.csv', index=False)
        formula_results.to_csv(Path(temporary_directory) / 'formula_results.csv', index=False)
        
        # copy_csv_to_sql(Path(temporary_directory) / 'formula_results.csv', 'FormulaResult')
        # copy_csv_to_sql(Path(temporary_directory) / 'curve_points.csv', 'CurvePoint')
        # copy_csv_to_sql(Path(temporary_directory) / 'curve_point_prices.csv', 'CurvePointPrice')
        # copy_csv_to_sql(Path(temporary_directory) / 'quoted_values.csv', 'QuoteValue')
        
        
        
 
def copy_csv_to_sql(csv_file_path, table_name):
    sql_config = {
        'server': 'localhost',
        'database': 'PE_local',
        'username': 'sa',
        'password': 'Atleast8'
    }
    
    flat_file = bcpy.FlatFile(qualifier='', path=csv_file_path)
    sql_table = bcpy.SqlTable(sql_config, table=table_name)
    flat_file.to_sql(sql_table, use_existing_sql_table=True) 

@app.command("query")        
def query_data():
    # Define your SQL query
    # Your SQL query that returns multiple result sets
    days_back = 5
    query = f"""
SET NOCOUNT ON
Declare @Cutoff DateTime =  DATEADD(DAY, DATEDIFF(day, 0, (dateadd(day, datediff(day, 0, DATEADD(DAY, -{days_back}, GETDATE())),0))),0)
    
SELECT * 
INTO #CurvePoint
FROM CurvePoint cp 
WHERE cp.EffectiveToDateTime > @Cutoff AND cp.IsActive = 1

SELECT cpp.* 
INTO #CurvePointPrice
FROM CurvePointPrice cpp
INNER JOIN #CurvePoint cp ON cp.CurvePointID = cpp.CurvePointID


SELECT * 
INTO #QuotedValue
FROM QuotedValue qv
WHERE qv.EffectiveToDateTime > @Cutoff AND qv.IsActive = 1




;WITH formula_results AS (
	SELECT DISTINCT FormulaResultId FROM #CurvePointPrice cpp WHERE cpp.FormulaResultId IS NOT NULL
	UNION
	SELECT DISTINCT FormulaResultId FROM #QuotedValue qv WHERE qv.FormulaResultId IS NOT NULL
)
SELECT fr.* 
INTO #FormulaResult
FROM FormulaResult fr
INNER JOIN formula_results ON fr.FormulaResultId = formula_results.FormulaResultId


Select * from #CurvePoint 
Select * from #CurvePointPrice 
Select * from #QuotedValue
Select * from #FormulaResult 
    """

    # Execute the query
    connection = pyodbc.connect(QUERY_CONNECTION_STRING)
    cursor = connection.cursor()
    cursor.execute(query)

    # Store each result set in a separate DataFrame
    dataframes = []
    while True:
        try:
            # Fetch all rows from the current result set
            rows = cursor.fetchall()
            if not rows:
                break
            # Get column names from the cursor description
            # Create a DataFrame from the fetched rows
            df = pd.DataFrame(
                [list(i) for i in rows], 
                columns=[column[0] for column in cursor.description]
            )
            dataframes.append(df)
        except pyodbc.ProgrammingError:
            # This exception is raised when there are no more result sets
            break
        # Move to the next result set
        cursor.nextset()


    connection.close()
    # Print the DataFrame to verify the results
    for i in range(len(dataframes)):
        print(dataframes[i])
    return dataframes



 
    
    
if __name__ == "__main__":
    app()
