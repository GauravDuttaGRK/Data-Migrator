import os
import pandas as pd  # type: ignore
import mysql.connector  # type: ignore
import pymongo
from datetime import datetime

import csvtosql
import csvtomongo
import sqltomongo
import mongotosql
import txttosql
import txttomongo


choice = input("""Enter 1 to load CSV into MySQL
Enter 2 to load CSV into MongoDB
Enter 3 to migrate MySQL to MongoDB 
Enter 4 to migrate MongoDB into MySQL
Enter 5 to load TXT into MySQL
Enter 6 to load CSV into MySQL
Enter which process you want to perform: """)

if choice == "1":
    print("Process For Loading CSV files into MySQL...")
    csvtosql.migrate_csv_to_mysql()

elif choice == "2":
    print("Process For Loading CSV files into MongoDB...")
    csvtomongo.migrate_csv_to_mongo()

elif choice == "3":
    print("Process For Loading MySQL Tables into MongoDB...")
    sqltomongo.migrate_mysql_to_mongo()

elif choice == "4":
    print("Process For Loading MongoDB Collections into MySQL...")
    mongotosql.migrate_mongo_to_mysql()

elif choice == "5":
    print("Process For Loading TXT Files into MySQL...")
    txttosql.migrate_txt_to_mysql()


elif choice == "6":
    print("Process For Loading TXT Files into MongoDB...")
    txttomongo.migrate_txt_to_mongodb()


else:
    print("""Invalid Choice ❌.
Please Enter The Correct Process Number ❗
Try Again ⚠""")
