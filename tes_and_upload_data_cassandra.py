import pandas as pd
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from cassandra.query import BatchStatement, ConsistencyLevel

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

# Baca file CSV
df = pd.read_csv('ratings.csv')

# Prepare insert query
insert_query = session.prepare("""
INSERT INTO ratings (userId, movieId, rating, timestamp) VALUES (?, ?, ?, ?)
""")

batch_size = 10000
total_rows = len(df)
print(f"Total rows to insert: {total_rows}")

for start in range(0, total_rows, batch_size):
    end = min(start + batch_size, total_rows)
    batch = BatchStatement(consistency_level=ConsistencyLevel.LOCAL_QUORUM)

    for i in range(start, end):
        row = df.iloc[i]
        batch.add(insert_query, (int(row['userId']), int(row['movieId']), float(row['rating']), int(row['timestamp'])))

    session.execute(batch)
    print(f"Inserted rows {start} to {end-1}")

print("Batch insert selesai!")
