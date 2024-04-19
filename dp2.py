from pymongo import MongoClient, errors
from bson.json_util import dumps
import os
import json

MONGOPASS = os.getenv('MONGOPASS')
uri = "mongodb+srv://cluster0.pnxzwgz.mongodb.net/"
client = MongoClient(uri, username='nmagee', password=MONGOPASS, connectTimeoutMS=200, retryWrites=True)
# specify a database
db = client["mgv8dh"]
# specify a collection
collection = db["dp2"]

# Directory containing JSON files
path = "data"
imported_count = 0
orphaned_count = 0
corrupted_count = 0
total_count = 0

# Process each file in the directory
for (root, dirs, files) in os.walk(path):
    for file in files:
        data_path = path + "/" + file
        try:
            with open(data_path) as f:
                file_data = json.load(f)
                entry_count = len(file_data) if isinstance(file_data, list) else 1
                total_count += entry_count
                if isinstance(file_data, list):
                    added_values = collection.insert_many(file_data)  
                    imported_count += entry_count
                else:
                    added_values = collection.insert_one(file_data)
                    imported_count += entry_count
        except json.JSONDecodeError as e:
            print("Expecting property name enclosed in double quotes")
            corrupted_count += 1
            orphaned_count = entry_count - 1
        

print(f"Total records: {total_count}, Imported: {imported_count}, Corrupted: {corrupted_count}, Orphaned: {orphaned_count}")

