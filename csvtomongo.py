import os
import pandas as pd  # type: ignore
import pymongo
from datetime import datetime

def migrate_csv_to_mongo():
    # DATABASE CHECKING AND COLLECTION DISPLAY OF THE CURRENT DATABASE IN MONGODB
    client = pymongo.MongoClient("mongodb://localhost:27017/")  # Change URL if needed

    # Get all database names
    databases = client.list_database_names()

    # User input for database name
    db_name = input("Enter the mongo database name: ")

    if db_name in databases:
        print(f"Database '{db_name}' exists.")

        # Get all collections of the given database
        db = client[db_name]
        collections = db.list_collection_names()

        print(f"Collections in `{db_name}`:", collections)

        insertion = input(f"Do you want to migrate all csv files into `{db_name}` database ? (Y/N) ")

        if (insertion == 'Y' or insertion == 'y') :  
            folder_path = r"G:\DATA_MIGRATOR_TOOL\csv_files"  # Change this to your actual folder path
            excel_file = r"G:\DATA_MIGRATOR_TOOL\status_csv_mysql\status_csv_mongo.xlsx"  

            # Ensure the Excel file has the required columns
            expected_columns = ["Table Name", "csv_insertion_mongodb","Insertion Timestamp"]

            if os.path.exists(excel_file):
                df_status = pd.read_excel(excel_file)
                for col in expected_columns:
                    if col not in df_status.columns:
                        df_status[col] = None  
            else:
                df_status = pd.DataFrame(columns=expected_columns)  
                df_status.to_excel(excel_file, index=False)  

            # ** Connect to MongoDB**
            mongo_client = pymongo.MongoClient("mongodb://localhost:27017/")
            mongo_db = mongo_client[db_name]

            # Loop Through All CSV Files in the Folder
            for file in os.listdir(folder_path):
                if file.endswith(".csv"):    
                    file_path = os.path.join(folder_path, file)

                    # Read CSV into DataFrame
                    df = pd.read_csv(file_path,encoding="latin1") 

                    # Convert DataFrame to dictionary
                    data_dict = df.to_dict(orient="records")  

                    # Generate Table Name from File Name (without .csv)
                    table_name = os.path.splitext(file)[0]  

                    # Check if table already exists in the Excel file, otherwise add it
                    if table_name not in df_status["Table Name"].values:
                        new_row = {"Table Name": table_name, "csv_insertion_mongodb": None}
                        df_status = pd.concat([df_status, pd.DataFrame([new_row])], ignore_index=True)

                    # Insert into MongoDB (table name → collection name)
                    if data_dict:
                        mongo_db[table_name].insert_many(data_dict)
                        print(f"✅ Table `{table_name}` transferred to MongoDB.")

                        # Update Excel file with "OK" status
                        df_status.loc[df_status["Table Name"] == table_name, "csv_insertion_mongodb"] = "OK"
                        df_status.loc[df_status["Table Name"] == table_name, "Insertion Timestamp"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                        df_status.to_excel(excel_file, index=False, engine="openpyxl")

            # Close Database Connection
            mongo_client.close() 
            print("✅ Database connection closed.")

        else:
            print("No Migration Executed!")

    else:
        print(f"Database '{db_name}' does not exist.")


# migrate_csv_to_mongo()