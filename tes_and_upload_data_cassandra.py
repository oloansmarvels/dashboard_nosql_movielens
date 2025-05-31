import pandas as pd
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider

# Koneksi ke Astra Cassandra
cloud_config = {
    'secure_connect_bundle': 'secure-connect-movielens.zip'
}

auth_provider = PlainTextAuthProvider('DGZxvpXmKIsklDlsSgfBZrHx', 'ZWSlg8qT.ItZFxdh+kp4PX8Wqk.8NsCws_BNxfbGNsIsZKrjlmU6Y-0OSXCzKwoLb0UyEi4gkDDYa-kZnCtN+XeHcTQiDqZJAiMcTKFtT9HZMil4bhmqi+wcBaYGJS.J')
cluster = Cluster(cloud=cloud_config, auth_provider=auth_provider)
session = cluster.connect()

# Pilih keyspace
session.set_keyspace('movielens')

# Buat table ratings (jika belum ada)
session.execute("""
CREATE TABLE IF NOT EXISTS ratings (
    userId int,
    movieId int,
    rating float,
    timestamp bigint,
    PRIMARY KEY (userId, movieId)
);
""")

# Baca file CSV ratings
df = pd.read_csv('ratings.csv')

# Prepare statement insert (untuk performa lebih baik)
insert_query = session.prepare("""
INSERT INTO ratings (userId, movieId, rating, timestamp) VALUES (?, ?, ?, ?)
""")

# Insert data satu per satu (bisa dioptimasi lagi dengan batch)
for idx, row in df.iterrows():
    session.execute(insert_query, (int(row['userId']), int(row['movieId']), float(row['rating']), int(row['timestamp'])))

print(f"Inserted {len(df)} ratings to Cassandra.")
