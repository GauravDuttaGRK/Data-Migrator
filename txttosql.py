import os
import mysql.connector
import pandas as pd
from datetime import datetime

def migrate_txt_to_mysql():
    # **1 Connect to MySQL**
    conn = mysql.connector.connect(
        host="localhost",  
        user="root",  
        password="gaurav2004",  
    )
    cur = conn.cursor()

    db_name = input("Enter the SQL database name: ")

    # ** Get all table names from MySQL**
    all_datas_query = "SHOW DATABASES"
    cur.execute(all_datas_query)
    all_database = cur.fetchall()
    cur.close()
    conn.close()

    # Correct way to store all database names
    databases = [db[0] for db in all_database] 

    if db_name in databases:
        print(f"Database '{db_name}' exists.")
        
        conn = mysql.connector.connect(
            host="localhost",  
            user="root",  
            password="gaurav2004",  
            database=db_name
        )
        cur = conn.cursor()

        all_tables_query = "SHOW TABLES"
        cur.execute(all_tables_query)
        all_tables = cur.fetchall()
        tables = [tb[0] for tb in all_tables] 
        print(f"Tables for `{db_name}` database: ", tables)

        cur.close()
        conn.close()

        insertion = input(f"Do you want to migrate all txt files into SQL `{db_name}` database? (Y/N) ")

        if insertion == 'Y' or insertion == 'y':
            excel_file = r"G:\DATA_MIGRATOR_TOOL\status_csv_mysql\status_txt_mysql.xlsx"  

            expected_columns = ["Table Name", "txt_insertion_mysql", "Insertion Timestamp"]

            if os.path.exists(excel_file):
                df_status = pd.read_excel(excel_file)
                for col in expected_columns:
                    if col not in df_status.columns:
                        df_status[col] = None  
            else:
                df_status = pd.DataFrame(columns=expected_columns)
                df_status.to_excel(excel_file, index=False)

            conn = mysql.connector.connect(
                host="localhost",  
                user="root",  
                password="gaurav2004",  
                database=db_name
            )
            cur = conn.cursor()

            folder_path = r"G:\DATA_MIGRATOR_TOOL\txt_files"

            for file in os.listdir(folder_path):
                if file.endswith(".txt"): 
                    txt_path = os.path.join(folder_path, file)
                    table_name = os.path.splitext(file)[0]

                    if table_name not in df_status["Table Name"].values:
                        new_row = {"Table Name": table_name, "txt_insertion_mysql": None}
                        df_status = pd.concat([df_status, pd.DataFrame([new_row])], ignore_index=True)

                    df = pd.read_csv(txt_path, sep=r'\s+', engine='python')
                    df = df.astype(str).where(pd.notna(df), None)

                    columns = ", ".join([f" {col} TEXT" if col.lower() != "price" else "price FLOAT" for col in df.columns])
                    create_table_query = f"CREATE TABLE IF NOT EXISTS {table_name} ({columns});"
                    cur.execute(create_table_query)
                    print(f" Table `{table_name}` created or already exists.")

                    placeholders = ", ".join(["%s"] * len(df.columns))
                    insert_query = f"INSERT INTO {table_name} VALUES ({placeholders})"

                    data_tuples = list(df.itertuples(index=False, name=None))

                    if data_tuples:
                        cur.executemany(insert_query, data_tuples)
                        conn.commit()
                        print(f"✅ {cur.rowcount} rows inserted into `{table_name}`!")

                        df_status.loc[df_status["Table Name"] == table_name, "txt_insertion_mysql"] = "OK"
                        df_status.loc[df_status["Table Name"] == table_name, "Insertion Timestamp"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                        df_status.to_excel(excel_file, index=False, engine="openpyxl")

            cur.close()
            conn.close()        
        else:
            print("No data file is fetched from txt folder")
    else:
        print(f"`{db_name}` Database does not exist in SQL")

# Call the function
# migrate_txt_to_mysql()
