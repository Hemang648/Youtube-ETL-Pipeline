import os
import json
import logging
from datetime import datetime
from googleapiclient.discovery import build
from dotenv import load_dotenv

load_dotenv()
API_KEY = os.getenv("YOUTUBE_API_KEY")

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def extract_data(query="data engineering"):
    try:
        youtube = build("youtube", "v3", developerKey=API_KEY)
        logging.info(f"Fetching data for query: {query}")

        # 1️⃣ Search videos
        request = youtube.search().list(
            part="snippet",
            q=query,
            maxResults=50,
            type="video"
        )
        response = request.execute()

        # 2️ Collect video IDs
        video_ids = [
            item["id"]["videoId"]
            for item in response.get("items", [])
        ]

        # 3️ Fetch statistics using video IDs
        stats_request = youtube.videos().list(
            part="statistics",
            id=",".join(video_ids)
        )
        stats_response = stats_request.execute()

        # 4️ Combine both responses
        data = {
            "search_results": response,
            "statistics": stats_response
        }

        # 5️⃣ Save raw JSON
        
        # path = "data/raw"     ## local path for testing
        path = "/opt/airflow/data/raw"
        os.makedirs(path, exist_ok=True)
        filename = f"{path}/youtube_{datetime.now().strftime('%Y%m%d')}.json"

        with open(filename, "w") as f:
            json.dump(data, f)

        logging.info(f"Saved {len(video_ids)} videos with statistics to {filename}")

    except Exception as e:
        logging.error(f"Extraction failed: {e}")
        raise


if __name__ == "__main__":
    extract_data()