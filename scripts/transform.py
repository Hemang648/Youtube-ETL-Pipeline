import json
import glob
import os
import pandas as pd
import logging

logging.basicConfig(level=logging.INFO)

def transform():

    files = glob.glob("/opt/airflow/data/raw/*.json")
    if not files:
        raise Exception("No raw files found")

    latest_file = max(files, key=os.path.getctime)

    with open(latest_file, "r") as f:
        data = json.load(f)

    search_items = data["search_results"].get("items", [])
    stats_items = data["statistics"].get("items", [])

    # Create statistics lookup dictionary
    stats_dict = {
        item["id"]: item["statistics"]
        for item in stats_items
    }

    records = []

    for item in search_items:

        video_id = item["id"].get("videoId")
        stats = stats_dict.get(video_id, {})

        records.append({
            "video_id": video_id,
            "title": item["snippet"].get("title"),
            "channel_name": item["snippet"].get("channelTitle"),
            "published_at": item["snippet"].get("publishedAt"),
            "views": int(stats.get("viewCount",0)),
            "likes": int(stats.get("likeCount",0)),                         
            "comments": int(stats.get("commentCount",0))
        })

    df = pd.DataFrame(records)

    # os.makedirs("data/processed", exist_ok=True)
    # df.to_csv("data/processed/youtube_clean.csv", index=False)   ## local path for testing
    
    os.makedirs("/opt/airflow/data/processed", exist_ok=True)
    output_path = "/opt/airflow/data/processed/youtube_clean.csv"
    df.to_csv(output_path, index=False)

    logging.info(f"Transformed {len(df)} records and saved to {output_path}")


if __name__ == "__main__":
    transform()