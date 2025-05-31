import pandas as pd
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from cassandra.query import BatchStatement, ConsistencyLevel
import time

# Koneksi ke Astra Cassandra
cloud_config = {
    'secure_connect_bundle': 'secure-connect-movielens.zip'
}

auth_provider = PlainTextAuthProvider(
    'DGZxvpXmKIsklDlsSgfBZrHx',
    'ZWSlg8qT.ItZFxdh+kp4PX8Wqk.8NsCws_BNxfbGNsIsZKrjlmU6Y-0OSXCzKwoLb0UyEi4gkDDYa-kZnCtN+XeHcTQiDqZJAiMcTKFtT9HZMil4bhmqi+wcBaYGJS.J'
)
cluster = Cluster(cloud=cloud_config, auth_provider=auth_provider)
session = cluster.connect()

# Set keyspace
session.set_keyspace('movielens')

# Create table jika belum ada
session.execute("""
CREATE TABLE IF NOT EXISTS ratings (
    userId int,
    movieId int,
    rating float,
    timestamp bigint,
    PRIMARY KEY (userId, movieId)
);
""")

# Prepare insert statement
insert = session.prepare("""
    INSERT INTO ratings (userId, movieId, rating, timestamp)
    VALUES (?, ?, ?, ?)
""")

# Baca file CSV
df = pd.read_csv('ratings.csv')

batch_size = 1000
count = 0
total_rows = len(df)

for start in range(0, total_rows, batch_size):
    batch = BatchStatement(consistency_level=ConsistencyLevel.LOCAL_QUORUM)
    end = min(start + batch_size, total_rows)
    for i in range(start, end):
        row = df.iloc[i]
        batch.add(insert, (int(row['userId']), int(row['movieId']), float(row['rating']), int(row['timestamp'])))
    try:
        session.execute(batch)
        count += (end - start)
        print(f"{count} baris dimasukkan (batch {start} sampai {end - 1})...")
        time.sleep(1)  # delay optional tiap batch
    except Exception as e:
        print(f"Gagal insert batch baris {start} sampai {end - 1}: {e}")
