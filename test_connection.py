import pymongo
import certifi

username = "oloansmarvels"
password = "marvelmandaadiba"
uri = f"mongodb+srv://{username}:{password}@cluster0.1ae0dxs.mongodb.net/?retryWrites=true&w=majority"

try:
    client = pymongo.MongoClient(uri, tls=True, tlsCAFile=certifi.where())
    db = client.test
    print("✅ Success! Collections in 'test':", db.list_collection_names())
except Exception as e:
    print("❌ Connection failed:")
    print(e)


import pandas as pd
import pymongo
import certifi

client = pymongo.MongoClient(uri, tlsCAFile=certifi.where())
db = client["movielens"]
collection = db["movies"]

# Baca CSV
df = pd.read_csv("movies.csv")

# Preprocessing genre: split string genre jadi list
df['genres'] = df['genres'].apply(lambda x: x.split('|') if isinstance(x, str) else [])

# Convert DataFrame ke list of dicts untuk upload ke MongoDB
movies = df.to_dict(orient='records')

# Insert ke MongoDB
result = collection.insert_many(movies)
print(f"Inserted {len(result.inserted_ids)} documents")
