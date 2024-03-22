import typer
from typing_extensions import Annotated
import bcpy;
import pandas as pd;
import pyodbc;
import tempfile;
from pathlib import Path;

app = typer.Typer()

@app.command("reset-and-pull")
def main():
    clear_table()
    df = query()
    with tempfile.TemporaryDirectory() as temporary_directory:
        print('created temporary directory', temporary_directory)
        csv_file_path = Path(temporary_directory) / 'AppliedPatches.csv'
        df.to_csv(csv_file_path, index=False)
        sql_config = {
            'server': 'localhost',
            'database': 'PE_local',
            'username': 'sa',
            'password': 'Atleast8'
        }
        sql_table_name = 'AppliedPatches'
        flat_file = bcpy.FlatFile(qualifier='', path=csv_file_path)
        sql_table = bcpy.SqlTable(sql_config, table=sql_table_name)
        flat_file.to_sql(sql_table, use_existing_sql_table=True)
   

@app.command("from-query")
def from_query():
    df = query()
    sql_config = {
        'server': 'localhost',
        'database': 'PE_local',
        'username': 'sa',
        'password': 'Atleast8'
    }
    
    sql_table_name = 'AppliedPatches'
    bdf = bcpy.DataFrame(df)
    sql_table = bcpy.SqlTable(sql_config, table=sql_table_name)
    bdf.to_sql(sql_table)

@app.command("from-csv")
def upgrade(name: Annotated[str, typer.Option()] = "undefined"):
    print(f"Upgraded {name}")
    sql_config = {
        'server': 'localhost',
        'database': 'PE_local',
        'username': 'sa',
        'password': 'Atleast8'
    }
    
    sql_table_name = 'AppliedPatches'
    csv_file_path = 'AppliedPatches.csv'
    
    flat_file = bcpy.FlatFile(qualifier='', path=csv_file_path)
    sql_table = bcpy.SqlTable(sql_config, table=sql_table_name)
    flat_file.to_sql(sql_table, use_existing_sql_table=True)
    
@app.command("query")
def query() -> pd.DataFrame:
    conn_str = (
        r'DRIVER={ODBC Driver 17 for SQL Server};'
        r'SERVER=dev-sql1.gravitate.energy;'
        r'DATABASE=pe_dev;'
        r'UID=matt.rothmeyer;'
        r'PWD=6FG5HP?ENomF5&bT'
    )

    # Create a connection to the database
    cnxn = pyodbc.connect(conn_str)

    # Define your SQL query
    sql_query = "SELECT * FROM appliedpatches"

    # Execute the query and load the results into a DataFrame
    df = pd.read_sql(sql_query, cnxn)

    # Close the connection
    cnxn.close()

    # Print the DataFrame to verify the results
    print(df)
    return df

@app.command("reset")    
def clear_table():
    conn_str = (
        r'DRIVER={ODBC Driver 17 for SQL Server};'
        r'SERVER=localhost;'
        r'DATABASE=PE_local;'
        r'UID=sa;'
        r'PWD=Atleast8'
    )
    conn = pyodbc.connect(conn_str)
    # Create a cursor object
    cursor = conn.cursor()

    # Define your SQL DELETE query
    # Replace 'product' with your actual table name and adjust the WHERE clause as needed
    cursor.execute('DELETE FROM AppliedPatches')

    # Commit the changes
    conn.commit()
    
    
if __name__ == "__main__":
    app()
