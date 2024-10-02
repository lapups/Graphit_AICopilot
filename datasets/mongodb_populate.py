from pymongo import MongoClient
from pymongo.errors import ConnectionFailure
import json

# Use your new password in the connection URI
uri = "mongodb+srv://mykolakyrychenko:O8pi5hW5bKeIKvYz@cluster0.22wex.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"


client = MongoClient(uri)

# Test the connection
try:
    # The ismaster command is cheap and does not require auth
    client.admin.command('ismaster')
    print("MongoDB connection successful!")
except ConnectionFailure as e:
    print(f"MongoDB connection failed: {e}")



db = client['AICopilot']        # Replace with your database name
collection = db['Tracking_Info']  # Replace with your collection name

# File path to your NDJSON file
file_path = 'datasets/output_data/modified_id_synthetic_data_nosql.ndjson'  # Update with your NDJSON file path

# Function to read NDJSON file and insert into MongoDB
def populate_mongodb_from_ndjson(file_path, collection):
    with open(file_path, 'r') as f:
        for line in f:
            if line.strip():  # Skip empty lines
                data = json.loads(line)  # Parse each line as a JSON object
                collection.insert_one(data)  # Insert each document into MongoDB
    print("Data successfully populated into MongoDB.")

# Call the function to populate MongoDB
populate_mongodb_from_ndjson(file_path, collection)
