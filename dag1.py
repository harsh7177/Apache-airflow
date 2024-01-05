from airflow import DAG
import os
import pandas as pd
from airflow import PythonOperator
from airflow import BashOperator
import unzip
from datetime import datetime, timedelta

default_args = {
    'owner': 'harsh',
    'start_date': datetime.today(),
    'email': 'xyz@qwe',
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag=DAG('ETL_tool_data',
        schedule_interval=timedelta(days=1),
        default_args=default_args,
        description='Apache Airflow Final Assignment')

##TASK1

def unzip(tar_file_path, destination_path):
    with tarfile.open(tar_file_path, 'r:gz') as tar_ref:
        tar_ref.extractall(destination_path)
tar_file_path = r"C:\Users\gusai\OneDrive\Desktop\DE\tolldata.tgz"
destination_path = r"C:\Users\gusai\OneDrive\Desktop\DE\unzip-data"

unzip = PythonOperator(
    task_id='unzip_data',
    python_callable=unzip(tar_file_path,destination_path),
    dag=dag)



##TASK 2

csv_path=r"C:\Users\gusai\OneDrive\Desktop\DE\unzip-data"
def read_csv(csv_path):
    for file in os.listdir(csv_path):
        if file.endswith('.csv'):
            csv_df=pd.read_csv(os.path.join(csv_path,file),index_col=0)
            csv_df.to_csv(r"C:\Users\gusai\OneDrive\Desktop\DE\data",index=False)

extract_data_from_csv=PythonOperator(
    task_id='ex_csv',
    python_callable=read_csv(csv_path),
    op_args=[csv_path],
    dag=dag
)
    


#TASK 3
tsv_path=r"C:\Users\gusai\OneDrive\Desktop\DE\unzip-data"
def read_tsv(tsv_path):
    for file in os.listdir(tsv_path):
        if file.endswith('.tsv'):
            tsv_df=pd.read_csv(os.path.join(tsv_path,file),index_col=0,sep='\t')
            tsv_df.to_csv(r"C:\Users\gusai\OneDrive\Desktop\DE\data",index=False)

extract_data_from_tsv=PythonOperator(
    task_id='ex_tsv',
    python_callable=read_tsv,
    op_args=[tsv_path],
    dag=dag
)



###############
txt_path=r"C:\Users\gusai\OneDrive\Desktop\DE\unzip-data"

def read_txt(txt_path):
    for file in os.listdir(os.getcwd()):
        if file.endswith('.txt'):
            txt_df=pd.read_fwf(os.path.join(os.getcwd(),file),
                            widths=[6,25,12,4,10,4,6],
                            header=None,index_col=0,
                            names=['Row_id','Time_Stamp','Vehicle_Num','Toll_id','Toll_Code','Type_of_Payment','Vehicle_Code'])
            txt_df.to_csv(r"C:\Users\gusai\OneDrive\Desktop\DE\data",index=False)

extract_data_from_txt=PythonOperator(
    task_id='ex_tsv',
    python_callable=read_txt,
    op_args=[txt_path],
    dag=dag
)


################
consolidate_data=BashOperator(task_id='cansolidate_data',
                              bash_command="""paste C:/Users/gusai/OneDrive/Desktop/DE/data/csv_df 
                              C:/Users/gusai/OneDrive/Desktop/DE/data/tsv_df 
                              C:/Users/gusai/OneDrive/Desktop/DE/data/txt_df 
                              > C:/Users/gusai/OneDrive/Desktop/DE/extracted_data.csv""",
                              dag=dag)

####

def transform(concat_file_path):
    df=pd.read_csv(concat_file_path)
    df['vehicle_type']=df['vehicle_type'].str.upper()
    df.to_csv('transformed_data.csv',index=False)
    
concat_file_path=r"C:\Users\gusai\OneDrive\Desktop\DE\extracted_data.csv"
transfor_data=PythonOperator(
    task_id='ex_tsv',
    python_callable=read_txt,
    op_args=[concat_file_path],
    dag=dag
)

unzip>>extract_data_from_csv>>extract_data_from_tsv>>extract_data_from_txt>>consolidate_data>>transfor_data


    









              