import mysql.connector
import pymongo
import pandas as pd
import os
from datetime import datetime

def migrate_mysql_to_mongo():
    # DATABASE CHECKING AND TABLE DISPLAY OF THE CURRENT DATABASE IN MYSQL
    # **1️ Connect to MySQL**
    conn = mysql.connector.connect(
        host="localhost",  
        user="root",  
        password="gaurav2004",  
    )
    cur = conn.cursor()

    db_name = input("Enter the SQL database name: ")

    # **3️⃣ Get all table names from MySQL**
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

        insertionsql = input(f"Do You Want To Migrate All Your Table From SQL `{db_name}` Database To MongoDB? (Y/N)")
        mngdb_name = input("Enter the MongoDB database name: ")
        if (insertionsql == "Y" or insertionsql == "y"):
            excel_file = r"G:\DATA_MIGRATOR_TOOL\status_csv_mysql\status_mysql_mongodb.xlsx"  

            # Ensure the Excel file has the required columns
            expected_columns = ["Table Name", "mysql_insertion_mongodb", "Insertion Timestamp"]

            if os.path.exists(excel_file):
                df_status = pd.read_excel(excel_file)
                for col in expected_columns:
                    if col not in df_status.columns:
                        df_status[col] = None  
            else:
                df_status = pd.DataFrame(columns=expected_columns)  
                df_status.to_excel(excel_file, index=False)  

            # **1️⃣ Connect to MySQL**
            conn = mysql.connector.connect(
                host="localhost",  
                user="root",  
                password="gaurav2004",  
                database=db_name
            )
            cur = conn.cursor()

            # **3️⃣ Get all table names from MySQL**
            all_datas = "SHOW TABLES"
            cur.execute(all_datas)
            tables = cur.fetchall()

            # **4️⃣ Loop through each table & transfer data**
            for table in tables:
                table_name = table[0]

                if table_name not in df_status["Table Name"].values:
                    new_row = {"Table Name": table_name, "mysql_insertion_mongodb": None}
                    df_status = pd.concat([df_status, pd.DataFrame([new_row])], ignore_index=True)

                df = pd.read_sql(f"SELECT * FROM {table_name}", conn)

                data_dict = df.to_dict(orient="records")

                
                mongo_client = pymongo.MongoClient("mongodb://localhost:27017/")
                mngdatabases = mongo_client.list_database_names()

                if mngdb_name in mngdatabases:
                    print(f"Database '{mngdb_name}' exists.")

                    mongo_db = mongo_client[mngdb_name]
                    collections = mongo_db.list_collection_names()

                    print(f"Collections in `{mngdb_name}`:", collections)

                    insertionmongo = input(f"Do You Want To Migrate All The Data of `{table_name}` Table In Mongo `{mngdb_name}` Database? (Y/N)")

                    if (insertionmongo == "Y" or insertionmongo == "y"):
                        if data_dict:
                            mongo_db[table_name].insert_many(data_dict)
                            print(f"✅ Table `{table_name}` transferred to MongoDB.")
                            mongo_client.close()

                            df_status.loc[df_status["Table Name"] == table_name, "mysql_insertion_mongodb"] = "OK"
                            df_status.loc[df_status["Table Name"] == table_name, "Insertion Timestamp"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                            df_status.to_excel(excel_file, index=False, engine="openpyxl")
                    else:
                        print("Migration to MongoDB did not take place.")
                else:
                    print(f"Database `{mngdb_name}` in MongoDB does not exist.")

            cur.close()
            conn.close()

            print("✅ MySQL to MongoDB transfer complete!")
        else:
            print("Data is not fetched from MySQL database.")

    else:
        print(f"Database '{db_name}' does not exist.")

# migrate_mysql_to_mongo()
