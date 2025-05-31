import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt
import time
from pymongo import MongoClient
from cassandra.cluster import Cluster
import altair as alt
import json
from cassandra.auth import PlainTextAuthProvider
import os

# ------------------------- Konfigurasi Halaman -------------------------
st.set_page_config(page_title="Dashboard MongoDB & Cassandra", layout="wide")

# ------------------------- Koneksi DB ------------------------

mongo_client = pymongo.MongoClient(
    "mongodb+srv://<username>:<password>@ac-xhczaoj.mongodb.net/?retryWrites=true&w=majority&tls=true"
)
mongo_db = mongo_client["movielens"]
mongo_collection = mongo_db["movies"]
# ASTRA DB connection 
cloud_config = {
    'secure_connect_bundle': 'secure-connect-movielens.zip'
}

auth_provider = PlainTextAuthProvider('DGZxvpXmKIsklDlsSgfBZrHx', 'ZWSlg8qT.ItZFxdh+kp4PX8Wqk.8NsCws_BNxfbGNsIsZKrjlmU6Y-0OSXCzKwoLb0UyEi4gkDDYa-kZnCtN+XeHcTQiDqZJAiMcTKFtT9HZMil4bhmqi+wcBaYGJS.J')
cluster = Cluster(cloud=cloud_config, auth_provider=auth_provider)
cassandra_session = cluster.connect('movielens')

# ------------------------- Fungsi Utilitas -------------------------
def get_all_genres():
    genres = mongo_collection.distinct("genres")
    return sorted(filter(None, genres))

def get_mongo_data(limit=10, genre_filter=None):
    query = {}
    if genre_filter:
        query["genres"] = genre_filter
    data = list(mongo_collection.find(query).limit(limit))
    for doc in data:
        doc.pop('_id', None)
    return pd.DataFrame(data)

def get_cassandra_data(limit=10, min_rating=None):
    base_query = "SELECT * FROM ratings"
    if min_rating:
        base_query += f" WHERE rating >= {min_rating} ALLOW FILTERING"
    else:
        base_query += " LIMIT {}".format(limit)
    rows = cassandra_session.execute(base_query)
    return pd.DataFrame(rows)

def drop_all_mongo_indexes(except_id=True):
    for name in mongo_collection.index_information():
        if except_id and name == '_id_':
            continue
        mongo_collection.drop_index(name)

def create_mongo_compound_index():
    mongo_collection.create_index([("genres", 1), ("title", 1)])

def create_cassandra_index():
    cassandra_session.execute("CREATE INDEX IF NOT EXISTS idx_movieId ON ratings (movieId)")

def drop_cassandra_index():
    cassandra_session.execute("DROP INDEX IF EXISTS idx_movieId")

def mongo_query(indexed=False):
    drop_all_mongo_indexes()
    if indexed:
        create_mongo_compound_index()
    query_filter = {"genres": "Comedy"}
    start = time.time()
    results = list(mongo_collection.find(query_filter).limit(100))
    end = time.time()
    for doc in results:
        doc.pop('_id', None)
    return results, round(end - start, 4)

def cassandra_query(indexed=False):
    if indexed:
        create_cassandra_index()
    else:
        drop_cassandra_index()
    query = "SELECT * FROM ratings WHERE rating = 5.0 ALLOW FILTERING"
    start = time.time()
    rows = cassandra_session.execute(query)
    end = time.time()
    return list(rows), query, round(end - start, 4)

def combined_query(indexed_mongo=False, indexed_cassandra=False):
    drop_all_mongo_indexes()
    if indexed_mongo:
        mongo_collection.create_index("movieId")
    if indexed_cassandra:
        create_cassandra_index()
    else:
        drop_cassandra_index()
    start_time = time.time()
    q1 = "SELECT userId, count(*) as cnt FROM ratings GROUP BY userId ALLOW FILTERING"
    users = cassandra_session.execute(q1)
    top_user = max(users, key=lambda x: x.cnt, default=None)
    if not top_user:
        return None, None, None, q1, {}, round(time.time() - start_time, 4)
    q2 = f"SELECT movieId, count(*) as cnt FROM ratings WHERE userId = {top_user.userid} GROUP BY movieId ALLOW FILTERING"
    movies = cassandra_session.execute(q2)
    top_movie = max(movies, key=lambda x: x.cnt, default=None)
    if not top_movie:
        return top_user.userid, None, None, q2, {}, round(time.time() - start_time, 4)
    detail = list(mongo_collection.find({"movieId": top_movie.movieid}).limit(1))
    if detail:
        detail[0].pop('_id', None)
    return top_user.userid, top_movie.movieid, detail[0] if detail else {}, q2, {"movieId": top_movie.movieid}, round(time.time() - start_time, 4)

def plot_genre_distribution():
    pipeline = [
        {"$unwind": "$genres"},
        {"$group": {"_id": "$genres", "count": {"$sum": 1}}},
        {"$sort": {"count": -1}},
        {"$limit": 10}
    ]
    result = list(mongo_collection.aggregate(pipeline))
    df = pd.DataFrame(result).rename(columns={"_id": "Genre", "count": "Jumlah"})
    st.bar_chart(df.set_index("Genre"))

def plot_rating_distribution():
    query = "SELECT rating FROM ratings ALLOW FILTERING"
    rows = cassandra_session.execute(query)
    ratings = [row.rating for row in rows]
    df = pd.DataFrame(ratings, columns=["rating"])
    hist = alt.Chart(df).mark_bar().encode(
        alt.X("rating:Q", bin=alt.Bin(maxbins=5), title="Rating"),
        y='count()',
        tooltip=['count()']
    ).properties(width=600, height=300)
    st.altair_chart(hist, use_container_width=True)

# ------------------------- Sidebar Navigasi -------------------------
st.sidebar.title("Navigasi")
page = st.sidebar.radio("Pilih Halaman", [
    "Ringkasan",
    "Sample Data",
    "CRUD MongoDB",
    "CRUD Cassandra",
    "MongoDB Query",
    "Cassandra Query",
    "Gabungan",
    "Ringkasan Waktu",
    "Input Query",
    "Input Query Gabungan"
])

# ------------------------- Konten Halaman -------------------------
if page == "Ringkasan":
    st.title("Dashboard Analisis MongoDB & Cassandra")
    col1, col2 = st.columns(2)
    with col1:
        count_mongo = mongo_collection.count_documents({})
        st.metric("Jumlah Film (MongoDB)", f"{count_mongo:,}")
    with col2:
        cassandra_count = cassandra_session.execute("SELECT count(*) FROM ratings").one().count
        st.metric("Jumlah Rating (Cassandra)", f"{cassandra_count:,}")
    st.subheader("Distribusi Genre Terbanyak (MongoDB)")
    plot_genre_distribution()
    st.subheader("Distribusi Rating (Cassandra)")
    plot_rating_distribution()
elif page == "Sample Data":
    st.title("Sample Data dari MongoDB dan Cassandra")

    # Tombol Refresh
    if st.button("🔄 Refresh Data"):
        st.rerun()

    # MongoDB Section
    st.subheader("Filter MongoDB:")
    genres = get_all_genres()
    selected_genre = st.selectbox("Pilih Genre", [""] + genres)
    mongo_df = get_mongo_data(limit=100, genre_filter=selected_genre if selected_genre else None)
    st.dataframe(mongo_df, use_container_width=True)

    # Cassandra Section
    st.subheader("Filter Cassandra:")
    min_rating = st.slider("Minimum Rating", min_value=0.0, max_value=5.0, step=0.5, value=0.0)
    cass_df = get_cassandra_data(limit=100, min_rating=min_rating)
    st.dataframe(cass_df, use_container_width=True)

elif page == "CRUD MongoDB":
    st.title("CRUD MongoDB - Kelola Koleksi Movies")

    # Ambil genre unik untuk multiselect
    all_genres = get_all_genres()

    # Create
    st.subheader("Tambah Data Film Baru")
    with st.form("add_movie_form"):
        movieId = st.text_input("Movie ID")
        title = st.text_input("Judul")
        genres = st.multiselect("Genres", options=all_genres)
        submit_add = st.form_submit_button("Tambah Film")
        if submit_add:
            if not movieId or not title:
                st.warning("Movie ID dan Judul wajib diisi.")
            else:
                # Cek unik movieId
                if mongo_collection.find_one({"movieId": movieId}):
                    st.error(f"Movie ID '{movieId}' sudah ada.")
                else:
                    new_doc = {"movieId": movieId, "title": title, "genres": genres}
                    try:
                        mongo_collection.insert_one(new_doc)
                        st.success(f"Film '{title}' berhasil ditambahkan.")
                    except Exception as e:
                        st.error(f"Gagal menambahkan film: {e}")

    st.markdown("---")

    # Read & Update & Delete
    st.subheader("Cari dan Kelola Data Film")
    search_movieId = st.text_input("Cari berdasarkan Movie ID")
    if search_movieId:
        doc = mongo_collection.find_one({"movieId": search_movieId})
        if doc:
            st.write("Data ditemukan:")
            st.json({k: v for k, v in doc.items() if k != '_id'})

            # Pisah form update dan delete supaya gak bingung
            with st.form("update_movie_form"):
                new_title = st.text_input("Update Judul", value=doc.get("title", ""))
                new_genres = st.multiselect("Update Genres", options=all_genres, default=doc.get("genres", []))
                update_btn = st.form_submit_button("Update Data")
                if update_btn:
                    try:
                        mongo_collection.update_one(
                            {"movieId": search_movieId},
                            {"$set": {"title": new_title, "genres": new_genres}}
                        )
                        st.success("Data berhasil diupdate.")
                    except Exception as e:
                        st.error(f"Gagal update data: {e}")

            with st.form("delete_movie_form"):
                delete_btn = st.form_submit_button("Hapus Data")
                if delete_btn:
                    try:
                        mongo_collection.delete_one({"movieId": search_movieId})
                        st.success("Data berhasil dihapus.")
                    except Exception as e:
                        st.error(f"Gagal hapus data: {e}")
        else:
            st.warning("Data film dengan Movie ID tersebut tidak ditemukan.")

elif page == "CRUD Cassandra":
    st.title("CRUD Cassandra - Kelola Tabel Ratings")

    # Create (Insert)
    st.subheader("Tambah Data Rating Baru")
    with st.form("add_rating_form"):
        userId = st.number_input("User ID", min_value=1, step=1)
        movieId = st.number_input("Movie ID", min_value=1, step=1)
        rating = st.slider("Rating", min_value=0.0, max_value=5.0, step=0.5, value=3.0)
        timestamp = st.number_input("Timestamp (unix epoch)", min_value=0, step=1)
        submit_add = st.form_submit_button("Tambah Rating")
        if submit_add:
            try:
                # Optional: cek apakah userId dan movieId ada
                cql = """
                    INSERT INTO ratings (userid, movieid, rating, timestamp) VALUES (%s, %s, %s, %s)
                """
                cassandra_session.execute(cql, (userId, movieId, rating, timestamp))
                st.success(f"Rating untuk movieId {movieId} oleh userId {userId} berhasil ditambahkan.")
            except Exception as e:
                st.error(f"Gagal menambahkan rating: {e}")

    st.markdown("---")

    # Read
    st.subheader("Cari Data Rating")
    search_userId = st.number_input("Cari berdasarkan User ID", min_value=1, step=1, key="search_user")
    search_movieId = st.number_input("Cari berdasarkan Movie ID", min_value=1, step=1, key="search_movie")

    if st.button("Cari Rating"):
        try:
            cql_search = f"""
                SELECT * FROM ratings WHERE userid = {search_userId} AND movieid = {search_movieId} ALLOW FILTERING
            """
            rows = cassandra_session.execute(cql_search)
            df = pd.DataFrame(rows)
            if df.empty:
                st.warning("Data rating tidak ditemukan.")
            else:
                st.dataframe(df)

                # Update Form
                with st.form("update_rating_form"):
                    new_rating = st.slider("Update Rating", min_value=0.0, max_value=5.0, step=0.5, value=float(df.iloc[0]['rating']))
                    new_timestamp = st.number_input("Update Timestamp", min_value=0, step=1, value=int(df.iloc[0]['timestamp']))
                    update_btn = st.form_submit_button("Update Rating")
                    if update_btn:
                        try:
                            cql_update = """
                                UPDATE ratings SET rating=%s, timestamp=%s WHERE userid=%s AND movieid=%s
                            """
                            cassandra_session.execute(cql_update, (new_rating, new_timestamp, search_userId, search_movieId))
                            st.success("Rating berhasil diupdate.")
                        except Exception as e:
                            st.error(f"Gagal update rating: {e}")

                # Delete Form
                with st.form("delete_rating_form"):
                    delete_btn = st.form_submit_button("Hapus Rating")
                    if delete_btn:
                        try:
                            cql_delete = """
                                DELETE FROM ratings WHERE userid=%s AND movieid=%s
                            """
                            cassandra_session.execute(cql_delete, (search_userId, search_movieId))
                            st.success("Rating berhasil dihapus.")
                        except Exception as e:
                            st.error(f"Gagal hapus rating: {e}")

        except Exception as e:
            st.error(f"Gagal mencari data rating: {e}")


elif page == "MongoDB Query":
    st.title("MongoDB Query dan Indexing")
    st.code("db.movies.find({ genres: { $in: [...] } }).sort({title: 1}).limit(500)", language="javascript")
    col1, col2 = st.columns(2)
    with col1:
        st.subheader("Tanpa Index")
        data, t1 = mongo_query(indexed=False)
        st.dataframe(pd.DataFrame(data), use_container_width=True)
        st.success(f"Waktu: {t1}s")
    with col2:
        st.subheader("Dengan Index")
        data, t2 = mongo_query(indexed=True)
        st.dataframe(pd.DataFrame(data), use_container_width=True)
        st.success(f"Waktu: {t2}s")

elif page == "Cassandra Query":
    st.title("Cassandra Query dan Indexing")
    st.code("SELECT * FROM ratings WHERE rating = 4.0 ALLOW FILTERING", language="sql")
    col1, col2 = st.columns(2)
    with col1:
        st.subheader("Tanpa Index")
        rows, q1, t1 = cassandra_query(indexed=False)
        st.code(q1)
        st.dataframe(pd.DataFrame(rows), use_container_width=True)
        st.success(f"Waktu: {t1}s")
    with col2:
        st.subheader("Dengan Index")
        rows, q2, t2 = cassandra_query(indexed=True)
        st.code(q2)
        st.dataframe(pd.DataFrame(rows), use_container_width=True)
        st.success(f"Waktu: {t2}s")

elif page == "Gabungan":
    st.title("Query Gabungan MongoDB + Cassandra")
    col1, col2 = st.columns(2)
    with col1:
        st.subheader("Tanpa Index")
        uid, mid, detail, q2, q3, t = combined_query(False, False)
        st.code(q2)
        st.code(f"db.movies.find({q3})", language="javascript")
        st.json(detail)
        st.success(f"Waktu: {t}s")
    with col2:
        st.subheader("Dengan Index")
        uid, mid, detail, q2, q3, t = combined_query(True, True)
        st.code(q2)
        st.code(f"db.movies.find({q3})", language="javascript")
        st.json(detail)
        st.success(f"Waktu: {t}s")

elif page == "Ringkasan Waktu":
    st.title("Ringkasan Waktu Eksekusi Query")
    _, t_mongo_no = mongo_query(False)
    _, t_mongo_yes = mongo_query(True)
    _, _, t_cass_no = cassandra_query(False)
    _, _, t_cass_yes = cassandra_query(True)
    _, _, _, _, _, t_comb_no = combined_query(False, False)
    _, _, _, _, _, t_comb_yes = combined_query(True, True)

    data = {
        "Database": ["MongoDB", "MongoDB", "Cassandra", "Cassandra", "Gabungan", "Gabungan"],
        "Indexing": ["Tanpa Index", "Dengan Index"] * 3,
        "Waktu (detik)": [t_mongo_no, t_mongo_yes, t_cass_no, t_cass_yes, t_comb_no, t_comb_yes]
    }
    df_time = pd.DataFrame(data)
    st.markdown("### Ringkasan Waktu Eksekusi per Database")
    for db in df_time['Database'].unique():
        st.subheader(f"{db}")
        df_sub = df_time[df_time['Database'] == db].copy()
        df_sub = df_sub.set_index('Indexing')[['Waktu (detik)']]
        st.table(df_sub.style.format("{:.4f}"))

elif page == "Input Query":
    st.title("Input Query")

    tab1, tab2 = st.tabs(["MongoDB", "Cassandra"])

    with tab1:
        st.subheader("MongoDB")
        query_str = st.text_area("Masukkan MongoDB Query (JSON):", value='{"genres": "Comedy"}')
        try:
            mongo_query_dict = eval(query_str)
            with st.spinner("Menjalankan query..."):
                results = list(mongo_collection.find(mongo_query_dict).limit(100))
                for r in results:
                    r.pop('_id', None)
                df = pd.DataFrame(results)
                st.dataframe(df, use_container_width=True)
        except Exception as e:
            st.error(f"Error saat menjalankan query MongoDB: {e}")

    with tab2:
        st.subheader("Cassandra")
        cql_query = st.text_area("Masukkan CQL Query:", value="SELECT * FROM ratings LIMIT 10;")
        try:
            with st.spinner("Menjalankan query..."):
                rows = cassandra_session.execute(cql_query)
                df = pd.DataFrame(rows)
                st.dataframe(df, use_container_width=True)
        except Exception as e:
            st.error(f"Error saat menjalankan query Cassandra: {e}")
            
elif page == "Input Query Gabungan":
    st.title("Input Query Gabungan MongoDB + Cassandra (Agregasi)")

    st.markdown("Masukkan query agregasi MongoDB (format pipeline JSON) dan query agregasi Cassandra (CQL).")

    # Input MongoDB aggregation pipeline as JSON list string
    mongo_agg_str = st.text_area(
        "MongoDB Aggregation Pipeline (JSON list):",
        value="""[
  {"$match": {"genres": "Comedy"}},
  {"$project": {"_id": 0, "movieId": 1, "title": 1, "genres": 1}}
]""",
        height=200
    )

    # Input Cassandra CQL aggregation query
    cassandra_agg_query = st.text_area(
        "Cassandra CQL Aggregation Query:",
        value="SELECT movieId, rating FROM ratings WHERE rating = 5.0 ALLOW FILTERING;",
        height=150
    )

    if st.button("Jalankan Query Gabungan"):

        # MongoDB aggregation
        try:
            import json
            pipeline = json.loads(mongo_agg_str)
            start_mongo = time.time()
            mongo_results = list(mongo_collection.aggregate(pipeline))
            end_mongo = time.time()

            # Hapus _id jika masih ada
            for doc in mongo_results:
                doc.pop('_id', None)

            df_mongo = pd.DataFrame(mongo_results)

            st.subheader("Hasil MongoDB Aggregation")
            st.dataframe(df_mongo, use_container_width=True)
            st.success(f"MongoDB berhasil dijalankan dalam {round(end_mongo - start_mongo, 4)} detik")

        except Exception as e:
            st.error(f"Error saat menjalankan MongoDB aggregation: {e}")
            df_mongo = pd.DataFrame()

        # Cassandra aggregation
        try:
            start_cass = time.time()
            rows = cassandra_session.execute(cassandra_agg_query)
            end_cass = time.time()
            df_cass = pd.DataFrame(rows)

            st.subheader("Hasil Cassandra Aggregation")
            st.dataframe(df_cass, use_container_width=True)
            st.success(f"Cassandra berhasil dijalankan dalam {round(end_cass - start_cass, 4)} detik")
        except Exception as e:
            st.error(f"Error saat menjalankan Cassandra aggregation: {e}")
            df_cass = pd.DataFrame()

        # Gabungkan data jika memungkinkan
        try:
            # Normalisasi nama kolom
            df_cass.columns = [col.strip() for col in df_cass.columns]
            df_mongo.columns = [col.strip() for col in df_mongo.columns]

            # Rename jika perlu agar konsisten
            if 'movieid' in df_cass.columns:
                df_cass.rename(columns={'movieid': 'movieId'}, inplace=True)

            # Debug: tampilkan kolom
            st.write("Kolom dari Cassandra:", df_cass.columns.tolist())
            st.write("Kolom dari MongoDB:", df_mongo.columns.tolist())

            # Gabungkan jika kolom movieId tersedia di keduanya
            if 'movieId' in df_cass.columns and 'movieId' in df_mongo.columns:
                merged_df = pd.merge(df_cass, df_mongo, on="movieId", how="inner")
                st.subheader("Hasil Gabungan berdasarkan movieId")
                st.dataframe(merged_df, use_container_width=True)
            else:
                st.warning("Kolom 'movieId' tidak ditemukan di salah satu hasil query.")
        except Exception as e:
            st.error(f"Error saat menggabungkan data: {e}")

# ------------------------- Footer -------------------------
st.markdown("---")
st.caption("© 2025 Kelompok Marvel | Amanda | Adiba | DS-46-02 ROBD")
