from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
import sqlite3
from sqlite3 import Error
import pandas as pd
import datetime as dt
import pathlib
import sys
import airflow
import csv
import base64

# Path to the main folder, outside of dags
root_path = str( pathlib.Path(__file__).parent.parent.resolve() ) 

def get_order_data():
    
    """
    Access the sqlite3 database and select all the data from the 'Order' table.
    If error happens, print it on the output.
    """
    
    try:
        conn = sqlite3.connect(root_path + "/dags/" + "Northwind_small.sqlite")
        
        print("Connection is established")
        
        cursorObj = conn.cursor()
        
        cursorObj.execute("""SELECT * FROM "Order" """)
        
        # get all the rows of the table 'Order' inside list.
        data = cursorObj.fetchall()
        
        # creates output_orders.csv inside /data/ folder
        with open( root_path + '/data/' + 'output_orders.csv', 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(['Id', 'CustomerId', 'EmployeeId', 'OrderDate', 'RequiredDate', 'ShippedDate',
                             'ShipVia', 'Freight', 'ShipName', 'ShipAddress', 'ShipCity', 'ShipRegion',
                             'ShipPostalCode', 'ShipCountry'])
            writer.writerows(data)
        
    
    except sqlite3.Error as error:
        print("Error is: ", error)
    
    finally:
        conn.close()

def merge_csv():
    
    """
    Merge the data/output_orders.csv file with the OrderDetail table named df2.
    After join operation, it selects the highest quantity sold for Rio de Janeiro.
    Creates count.txt with the result of the select operation.
    """
    
    try: 
        conn = sqlite3.connect(root_path + "/dags/" + "Northwind_small.sqlite")
        
        df1 = pd.read_csv(root_path + '/data/' + 'output_orders.csv')
        df2 = pd.read_sql_query("SELECT * FROM OrderDetail", conn)

        df_out = pd.merge( df1, df2, left_on='Id', right_on='OrderId', how='inner')
    
        result = df_out.query("ShipCity == 'Rio de Janeiro'")['Quantity'].sum()
    
        text_value = str(result)
        
        f = open( root_path + '/data/count.txt', 'x' )
        
        f.write(text_value)
        
        f.close()
        
    except sqlite3.Error as error:
        print("Error is: ", error)
    
    finally:
        conn.close()
    
def export_final_answer():
    
    """
        Creates final_output.txt with message being a Variable my_email and count.txt result.
        Encondes the message in ascii to bytes. 
        Change the message to a 64 base still in bytes. 
        Decode the message to ascii in a 64 base message.
        Write the message in the final_output.txt file in /data/ folder.
    """

    # Import count
    with open(root_path + '/data/count.txt') as f:
        count = f.readlines()[0]

    my_email = Variable.get("my_email")
    
    message = my_email+count
    
    message_bytes = message.encode('ascii')
    
    base64_bytes = base64.b64encode(message_bytes)
    
    base64_message = base64_bytes.decode('ascii')

    with open(root_path + "/data/final_output.txt", "w") as f:
        f.write(base64_message)
    
    return None      
    
default_args = {
        'owner': 'Laysa',    
        'start_date': airflow.utils.dates.days_ago(1),
        'depends_on_past': True,
        'retries': 1,
        'retry_delay': dt.timedelta(minutes=5),
        'email': ['laysa.bitencourt@indicium.tech'],
        'email_on_failure': True,
        'email_on_retry': True,
        }

with DAG(
    dag_id='Desafio-V',
    default_args=default_args,
    schedule_interval='@daily',
    description='It is the whole pipeline',
) as dag:
    
    task_get_order_data = PythonOperator(
        task_id='get_order_data',
        python_callable = get_order_data,
        do_xcom_push = False,
    )
    
    task_merge_csv = PythonOperator(
        task_id='do_merge_csv',
        python_callable = merge_csv,
        do_xcom_push = False,
    )
    
    export_final_output = PythonOperator(
        task_id='export_final_output',
        python_callable=export_final_answer,
        provide_context=True
    )
    
    task_get_order_data >> task_merge_csv >> export_final_output
