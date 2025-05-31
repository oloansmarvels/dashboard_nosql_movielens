import pymongo
import certifi

uri = "mongodb+srv://<username>:<password>@cluster0.xxxxxx.mongodb.net/?retryWrites=true&w=majority"

try:
    client = pymongo.MongoClient(uri, tls=True, tlsCAFile=certifi.where())
    db = client.test
    print("✅ Success! Collections in 'test':", db.list_collection_names())
except Exception as e:
    print("❌ Connection failed:")
    print(e)
