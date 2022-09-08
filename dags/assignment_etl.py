import airflow
from datetime import timedelta, datetime
from airflow import DAG
import pandas as pd
import numpy as np
from google.cloud import bigquery


from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

from airflow.operators.dummy import DummyOperator




CONN_ID_ARC = "rds_arc"
CONN_ID_CODEMENTOR =  "rds_codementor"
CREDENTIALS_PATH = "/usr/local/airflow/dags/data-eng-interview-cc9d14354dd3.json"




def get_data_from_postgres(conn_id, start_date, end_date):
   
   try:
      hook = PostgresHook(postgres_conn_id = conn_id)
      df = hook.get_pandas_df(sql = f"""SELECT 
                                       *
                                       FROM cmts
                                       WHERE created_at BETWEEN '{start_date}' AND '{end_date}'; 
                                    """
                              )
   except:
      raise ValueError(f""" can't successfully get data from {conn_id}""") 
   
   print('===========')
   print(f"""rows count of {conn_id}:""", df.shape)
   print('===========')

   return df



def url_group_mapping(url):
    if url == 'www.codementor.io':
        return 'cm_homepage'
    elif url.startswith('www.codementor.io') and len(url.split('/')) == 2 and url.endswith('-experts'):
        return 'cm_experts'
    elif url.startswith('www.codementor.io') and len(url.split('/')) == 2:
        return 'cm_profile'
    elif url.startswith('www.codementor.io') and len(url.split('/')) == 3:
        return 'cm_community'
    elif url == 'arc.dev':
        return 'arc_homepage'
    elif url.startswith('arc.dev') and len(url.split('/')) >=3:
        return 'arc_jobs'
    else:
        return 'other'


def load_dataframe_to_gcp(dataframe, credentials_path):

    client = bigquery.Client.from_service_account_json(credentials_path)

    dataset_ref = bigquery.DatasetReference(client.project, 'analysis_jammy')
    table_ref = bigquery.TableReference(dataset_ref, 'first_login_info')
    load_job = client.load_table_from_dataframe(dataframe, table_ref)

    print('successfully load dataframe to bigquery', load_job.result())




def main_transformation(conn_id_codementor,
                        conn_id_arc,
                        credentials_path,
                        **context):
   try:
      start_date = context['dag_run'].conf['start_date']
      end_date = context['dag_run'].conf['end_date']
   except:
      raise ValueError(f""" can't fetch the values of start_date or end_date""") 

   # get data from cm
   df_cm = get_data_from_postgres(conn_id_codementor, start_date, end_date)

   # get data from arc
   df_arc = get_data_from_postgres(conn_id_arc, start_date, end_date)

   if df_cm.shape[0] != 0 and df_arc.shape[0] != 0 :
      df_merged = pd.concat([df_cm, df_arc], axis = 0)
   elif df_cm.shape[0] != 0:
      df_merged = df_cm
   elif df_arc.shape[0] != 0:
      df_merged = df_arc
   else:
      raise ValueError(""" There's no data found in the dataset.""")

   # concate two dataframes into one


   # convert to datetime type
   df_merged['first_landing_visited_at'] = pd.to_datetime(df_merged['first_landing_visited_at'])

   # keep the firstly visisting record for each user_id
   df = df_merged.sort_values('first_landing_visited_at').groupby('user_id').head(1)


   #group rules mapping
   df['first_landing_url_group'] = df['first_landing_url'].apply(lambda x : url_group_mapping(x[8:]))

   print('==================')
   print('rows count of df_merged', df_merged.shape[0])
   print('rows count of df', df.shape[0])
   print('==================')
 

   try:
      load_dataframe_to_gcp(df, credentials_path)
   except:
      raise ValueError(f""" failed to load dataframe to bigquery""") 
   



args={'owner': 'jammy.wang',
      'start_date': datetime(2021, 1, 1) #'retries' : 3
      }




with DAG(dag_id = 'codementor_arc_etl', 
         schedule_interval = None, 
         default_args = args, 
         catchup = False,
         description ='data pipeline for users tracking'
        ) as dag:




   start = DummyOperator(
      task_id = 'start',
      dag = dag)


 
   main_process = PythonOperator(
      task_id = 'main_process',
      python_callable = main_transformation,
      op_kwargs = {
                'conn_id_codementor': CONN_ID_CODEMENTOR, 
                'conn_id_arc': CONN_ID_ARC,
                # 'start_date' : {{dag_run.conf.start_date}}, # START_DATE, #
                # 'end_date' : {{dag_run.conf.end_date}}, #END_DATE, 
                'credentials_path': CREDENTIALS_PATH
               
               }    
      )


   end = DummyOperator(
      task_id = 'end',
      dag = dag)



   start >> main_process >> end




