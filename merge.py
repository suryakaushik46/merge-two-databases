import psycopg2
import numpy as np
import collections

conn_marco = psycopg2.connect("postgres://marco_admin_user:fuB4WQqK@marco-dev.cluster-ceig96elcanm.us-east-1.rds.amazonaws.com:5432/marco")

conn_pluto = psycopg2.connect("postgres://marco_pluto_admin_user:uo2LxjUY@marco-pluto-dev.cluster-ceig96elcanm.us-east-1.rds.amazonaws.com:5432/marco_pluto")

conn_marco.autocommit = True
conn_pluto.autocommit = True
cursor_marco = conn_marco.cursor()
cursor_pluto = conn_pluto.cursor()


# need to define on which column you need to merge
column_need_to_merge={
    "products":"product_code",
    "exchanges":"exchange_code"
}

def merge_data():

    for table_name in column_need_to_merge:
        try:
            #getting active data from marco
        
            sql_query_for_active_data_in_marco = f'SELECT {column_need_to_merge[table_name]} from dbo.{table_name} where active = TRUE'
            cursor_marco.execute(sql_query_for_active_data_in_marco)
            data_active_marco = cursor_marco.fetchall()
            list_data_active_marco = [item for t in data_active_marco for item in t]
            list_to_string_marco_data=str(list_data_active_marco).strip('[]')
            list_to_string_marco_data=f'({list_to_string_marco_data})'

        

            #deleting data from pluto which is not active in marco
            if (len(list_data_active_marco)>=1):
                sql_query_to_delete_extra_data_in_pluto=f'delete from dbo.{table_name} where {column_need_to_merge[table_name]} not in {list_to_string_marco_data}'
                cursor_pluto.execute(sql_query_to_delete_extra_data_in_pluto)
            else:
                print(f"no active data in {table_name}")
                continue
        
            #getting all data from pluto after delete 
            sql_query_for_data_in_pluto = f'SELECT {column_need_to_merge[table_name]} from dbo.{table_name}'
            cursor_pluto.execute(sql_query_for_data_in_pluto)
            data_active_pluto = cursor_pluto.fetchall()
            list_data_active_pluto = [item for t in data_active_pluto for item in t]
        
      
            #getting differnce of data from marco to pluto setdiff(marco - pluto)
            list_of_data_to_be_added_pluto= list(set(list_data_active_marco).difference(set(list_data_active_pluto)))
            list_to_string_data_to_be_added=str(list_of_data_to_be_added_pluto).strip('[]')
            list_to_string_data_to_be_added=f'({list_to_string_data_to_be_added})'

        
        
            #getting common columns in table
            sql_to_get_cloumn_names_marco=f"select column_name from information_schema.columns where table_schema = 'dbo' and table_name='{table_name}'"
            sql_to_get_cloumn_names_pluto=f"select column_name from information_schema.columns where table_schema = 'dbo' and table_name='{table_name}' "
            cursor_marco.execute(sql_to_get_cloumn_names_marco)
            cursor_pluto.execute(sql_to_get_cloumn_names_pluto)
            column_names_in_marco = cursor_marco.fetchall()
            column_names_in_pluto = cursor_pluto.fetchall()
            common_colums=list(set(column_names_in_marco) & set(column_names_in_pluto))
            list_column_names = [item for t in common_colums for item in t]
            add_columns_pluto = ','.join(list_column_names)


            list_of_raw_data=[]
            raw_data_string=""
            #getting common colums data from marco which are not there in pluto 
            if (len(list_column_names)>=1 and len(list_of_data_to_be_added_pluto)>=1):
                sql_qurey_to_get_data_in_marco=f'select {add_columns_pluto} from dbo.{table_name} where {column_need_to_merge[table_name]} in {list_to_string_data_to_be_added} '
                cursor_marco.execute(sql_qurey_to_get_data_in_marco)
                list_of_raw_data=cursor_marco.fetchall()
                raw_data_string=str(list_of_raw_data).strip('[]')
            else:
                if(len(list_column_names)<1):
                    print(f"no common cloums in {table_name}")
                else:
                    print(f"already synced {table_name}")

            #inserting data into pluto which are active in marco
            if(len(list_of_raw_data)>=1):
                sql_for_adding_data_in_pluto=f'insert into dbo.{table_name} ({add_columns_pluto}) values {raw_data_string} '
                cursor_pluto.execute(sql_for_adding_data_in_pluto)
            else:
                print("no extra data need to be added")
       
            cursor_marco.execute(sql_query_for_active_data_in_marco)
            cursor_pluto.execute(sql_query_for_data_in_pluto)

            list_marco=cursor_marco.fetchall()
            list_pluto=cursor_pluto.fetchall()

            if(collections.Counter(list_marco) == collections.Counter(list_pluto)):
                print(f"sucessfully merged {table_name}")
            print("-------------------------------------------------------")
        except Exception as error:
            print(f"error occured {error}")
            print(f"merge failed for {table_name}")
        
  
merge_data()
    
