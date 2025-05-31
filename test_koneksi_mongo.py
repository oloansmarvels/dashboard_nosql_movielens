import pymongo
import certifi

username = "oloansmarvels"
password = "marvelmandaadiba"
uri = f"mongodb+srv://{username}:{password}@cluster0.1ae0dxs.mongodb.net/?retryWrites=true&w=majority"

client = pymongo.MongoClient(uri, tls=True, tlsCAFile=certifi.where(), serverSelectionTimeoutMS=5000)

try:
    db = client.movielens
    count = db.movies.count_documents({})
    print("Jumlah dokumen:", count)
except Exception as e:
    print("Error:", e)
