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
