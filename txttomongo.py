import os
import pymongo
import pandas as pd
from datetime import datetime

def migrate_txt_to_mongodb():
    # Connect to MongoDB
    client = pymongo.MongoClient("mongodb://localhost:27017/")

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

        insertion = input(f"Do you want to migrate all txt files into mongo `{db_name}` database ? (Y/N) ")

        if insertion == 'Y' or insertion == 'y':
            excel_file = r"G:\DATA_MIGRATOR_TOOL\status_csv_mysql\status_txt_mongodb.xlsx"

            # Ensure the Excel file has the required columns
            expected_columns = ["Table Name", "txt_insertion_mongodb", "Insertion Timestamp"]

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

            # Folder containing text files
            folder_path = r"G:\DATA_MIGRATOR_TOOL\txt_files"

            # Loop Through All TXT Files in the Folder
            for file in os.listdir(folder_path):
                if file.endswith(".txt"):
                    txt_path = os.path.join(folder_path, file)
                    table_name = os.path.splitext(file)[0]

                    if table_name not in df_status["Table Name"].values:
                        new_row = {"Table Name": table_name, "txt_insertion_mongodb": None}
                        df_status = pd.concat([df_status, pd.DataFrame([new_row])], ignore_index=True)

                    # Read text file into DataFrame using flexible whitespace handling
                    df = pd.read_csv(txt_path, sep=r'\s+', engine='python')
                    df = df.astype(str).where(pd.notna(df), None)

                    # Convert DataFrame to dictionary
                    data_dict = df.to_dict(orient="records")

                    # Insert into MongoDB
                    if data_dict:
                        mongo_db[table_name].insert_many(data_dict)
                        print(f"✅ Table `{table_name}` transferred to MongoDB.")

                        df_status.loc[df_status["Table Name"] == table_name, "txt_insertion_mongodb"] = "OK"
                        df_status.loc[df_status["Table Name"] == table_name, "Insertion Timestamp"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                        df_status.to_excel(excel_file, index=False, engine="openpyxl")

            mongo_client.close()
        else:
            print("No data file is fetched from txt folder")
    else:
        print(f"Database '{db_name}' does not exist.")

# Call the function
# migrate_txt_to_mongodb()
