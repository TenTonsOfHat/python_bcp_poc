import typer
from typing_extensions import Annotated
import bcpy;
import pandas as pd;
import pyodbc;
import tempfile;
from pathlib import Path;
import os
from queue import Queue
import subprocess
import threading 

SCRIPT_DIRECTORY = Path(os.path.dirname(os.path.abspath(__file__)))


app = typer.Typer()

QUERY_CONNECTION_STRING = (
    r'DRIVER={ODBC Driver 17 for SQL Server};'
    r'SERVER=dev-sql1.gravitate.energy;'
    r'DATABASE=pe_dev;'
    r'UID=matt.rothmeyer;'
    r'PWD=6FG5HP?ENomF5&bT'
)


        
        


@app.command("query")        
def query_data():
    # Define your SQL query
    # Your SQL query that returns multiple result sets
    days_back = 5
    query = f"""
        SELECT cp.CurvePointId
        INTO ##CurvePointBCP 
        FROM CurvePoint cp 
        WHERE cp.EffectiveToDateTime > DATEADD(DAY, DATEDIFF(day, 0, (dateadd(day, datediff(day, 0, DATEADD(DAY, -5, GETDATE())),0))),0) AND cp.IsActive = 1
        """

    # Execute the query
    connection = pyodbc.connect(QUERY_CONNECTION_STRING)
    cursor = connection.cursor()
    cursor.execute(query) #leave this open so that our global temp tables exist
    
    cursor.execute("Select * from ##CurvePointBCP") #leave this open so that our global temp tables exist
   
    cp_query = "SELECT top 100 cp.* FROM CurvePoint cp Where Exists (Select 1 from ##CurvePointBCP bcp where bcp.CurvePointId = cp.CurvePointId)"
   
    bcp_path = SCRIPT_DIRECTORY / "CurvePoint.Native.bcp"
   
    cmd_args = [
        "bcp",
        cp_query,
        "queryout",
        str(bcp_path),
        "-n",
        "-d",
        "PE_Dev",
        "-S",
        "dev-sql1.gravitate.energy",
        "-U",
        "matt.rothmeyer",
        "-P",
        "6FG5HP?ENomF5&bT"
    ]
    
    command_string = ' '.join(['"{}"'.format(arg) if ' ' in arg else arg for arg in cmd_args])

    print(command_string)
    print()
    
    try_running_command_win(cmd_args)
    
   
    
    connection.close()
   
   
   
    




def try_running_command_win(cmd, timeout: int = None):
    try:
        subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=timeout, check=True, universal_newlines=True)
        # Print the output
    except subprocess.CalledProcessError as e:
        print("command '{}' return with error (code {}): {}".format(e.cmd, e.returncode, e.output))

 
    
    
if __name__ == "__main__":
    query_data()
