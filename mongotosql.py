import os
import pymongo
import pandas as pd
import mysql.connector
from datetime import datetime

def migrate_mongo_to_mysql():
    # Connect to MongoDB
    client = pymongo.MongoClient("mongodb://localhost:27017/")  

    # Get all database names
    databases = client.list_database_names()

    # User input for database name
    mngdb_name = input("Enter the Mongo database name: ")

    if mngdb_name in databases:
        print(f"Database '{mngdb_name}' exists.")

        # Get all collections of the given database
        db = client[mngdb_name]
        collections = db.list_collection_names()
        client.close()
        print(f"Collections in `{mngdb_name}`:", collections)

        insertionmongo = input(f"Do you want to migrate all your collections from MongoDB `{mngdb_name}` database? (Y/N) ")

        if (insertionmongo == "Y" or insertionmongo == "y"):
            excel_file = r"G:\DATA_MIGRATOR_TOOL\status_csv_mysql\status_mongo_mysql.xlsx"  

            # Ensure the Excel file has the required columns
            expected_columns = ["Table Name", "mongo_insertion_mysql", "Insertion Timestamp"]

            if os.path.exists(excel_file):
                df_status = pd.read_excel(excel_file)

                for col in expected_columns:
                    if col not in df_status.columns:
                        df_status[col] = None  
            else:
                df_status = pd.DataFrame(columns=expected_columns)  
                df_status.to_excel(excel_file, index=False)  

            # **Connect to MongoDB**
            mongo_client = pymongo.MongoClient("mongodb://localhost:27017/")
            mongo_db = mongo_client[mngdb_name]

            db_name = input("Enter the SQL database name: ")

            # DATABASE CHECKING AND TABLE DISPLAY OF THE CURRENT DATABASE IN MYSQL
            # **1️ Connect to MySQL**
            conn = mysql.connector.connect(
                host="localhost",  
                user="root",  
                password="gaurav2004",  
            )
            cur = conn.cursor()

            # **Get all database names from MySQL**
            all_datas_query = "SHOW DATABASES"
            cur.execute(all_datas_query)
            all_database = cur.fetchall()
            cur.close()
            conn.close()

            # Store database names
            databases = [db[0] for db in all_database] 

            if db_name in databases:
                print(f"Database '{db_name}' exists.")

                # Connect to MySQL
                conn = mysql.connector.connect(
                    host="localhost",  
                    user="root",  
                    password="gaurav2004",  
                    database=db_name
                )
                cur = conn.cursor()

                # List all collections in MongoDB database
                collections = mongo_db.list_collection_names()

                for collection in collections:
                    documents = mongo_db[collection].find({}, {'_id': 0})

                    df = pd.DataFrame(documents)
                    df = df.fillna("")
                    # Fix FLOAT issue in price column
                    if "price" in df.columns:
                        df["price"] = df["price"].apply(lambda x: 0.0 if pd.isna(x) or x == "" else x) 

                    # Create Table Dynamically Based on CSV Columns
                    columns = ", ".join([f" {col} VARCHAR(255) NOT NULL" if col.lower() != "price" else "`price` FLOAT" for col in df.columns])

                    create_table_query = f"CREATE TABLE IF NOT EXISTS {collection} ({columns});"
                    cur.execute(create_table_query)

                    # Insert Data into the Table
                    placeholders = ", ".join(["%s"] * len(df.columns))
                    insert_query = f"INSERT INTO {collection} VALUES ({placeholders})"
                    data_tuples = list(df.itertuples(index=False, name=None))

                    # Check if table already exists in the Excel file, otherwise add it
                    if collection not in df_status["Table Name"].values:
                        new_row = {"Table Name": collection, "mongo_insertion_mysql": None}
                        df_status = pd.concat([df_status, pd.DataFrame([new_row])], ignore_index=True)

                    insertionsql = input(f"Do You Want To Migrate All The Data Of `{collection}` Collection In SQL `{db_name}` Database? (Y/N)")

                    if (insertionsql == "Y" or insertionsql == "y"):
                        if data_tuples:
                            cur.executemany(insert_query, data_tuples)
                            conn.commit()
                            print(f"✅ {cur.rowcount} rows inserted into `{collection}`!")

                            # Update Excel file with "OK" status
                            df_status.loc[df_status["Table Name"] == collection, "mongo_insertion_mysql"] = "OK"
                            df_status.loc[df_status["Table Name"] == collection, "Insertion Timestamp"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                            df_status.to_excel(excel_file, index=False, engine="openpyxl")

                    else:
                        print("Migration to SQL did not take place.")

                # Close Database Connection
                cur.close()
                conn.close() 
                mongo_client.close()
                print("✅ Database connection closed.")

        else:
            print("Data is not fetched from MongoDB database")
    else:
        print(f"Database '{mngdb_name}' does not exist.")



# migrate_mongo_to_mysql()
