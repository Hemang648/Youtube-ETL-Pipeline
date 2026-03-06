import logging
import pandas as pd
import psycopg2
import logging

# df = pd.read_csv("data/processed/youtube_clean.csv")    # local path for testing 

logging.basicConfig(level=logging.INFO)
df = pd.read_csv("/opt/airflow/data/processed/youtube_clean.csv")

conn = psycopg2.connect(
    dbname="youtube_db",
    user="airflow",
    password="airflow",
    host="postgres",
    port="5432"
)

cur = conn.cursor()

cur.execute("""
CREATE TABLE IF NOT EXISTS youtube_videos (
    video_id TEXT PRIMARY KEY,
    title TEXT,
    channel_name TEXT,
    published_at TIMESTAMP,
    views BIGINT,
    likes BIGINT,
    comments BIGINT
);
""")

for _, row in df.iterrows():
    cur.execute("""
        INSERT INTO youtube_videos
        (video_id, title, channel_name, published_at, views, likes, comments)
        VALUES (%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT (video_id)
        DO UPDATE SET
            title = EXCLUDED.title,
            channel_name = EXCLUDED.channel_name,
            published_at = EXCLUDED.published_at,
            views = EXCLUDED.views,
            likes = EXCLUDED.likes,
            comments = EXCLUDED.comments
        """,
        (
            row.video_id,
            row.title,
            row.channel_name,
            row.published_at,
            row.views,
            row.likes,
            row.comments
        ))

conn.commit()
cur.close()
conn.close()
logging.info(f"Loaded {len(df)} records")

