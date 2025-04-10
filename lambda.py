import os
import json
import boto3
import pymysql    # Pure Python MySQL driver
from collections import Counter
from datetime import datetime

def store_in_rds(processed_data):
    """
    Connects to the MySQL DB using pymysql, inserts user, daily listening,
    genre distribution, and top tracks. This function runs after building processed_data.
    """
    # 1) Connect to MySQL using environment variables
    #    e.g. DB_HOST, DB_USER, DB_PASS, DB_NAME, DB_PORT
    conn = pymysql.connect(
        host=os.environ["dk.ct8kksg2ijto.us-east-2.rds.amazonaws.com"],
        user=os.environ["dk"],
        password=os.environ["Hello123."],
        database=os.environ["dk"],
        port=int(os.environ.get("DB_PORT", 3306)),
        connect_timeout=5
    )

    try:
        with conn.cursor() as cur:
            # 2) Insert/Update user row
            user_id = processed_data.get("user_id", "unknown_user")
            display_name = processed_data.get("display_name", "Unknown Name")
            cur.execute("""
                INSERT INTO users (user_id, display_name)
                VALUES (%s, %s)
                ON DUPLICATE KEY UPDATE display_name = VALUES(display_name)
            """, (user_id, display_name))

            # We'll use today's date as the record_date for top tracks & genre
            record_date = datetime.utcnow().date()

            # 3) Insert daily listening
            listening_time = processed_data.get("listening_time", {})
            daily_labels = listening_time.get("daily_listening_labels", [])
            daily_values = listening_time.get("daily_listening_values", [])
            for date_str, minutes_listened in zip(daily_labels, daily_values):
                cur.execute("""
                    INSERT INTO user_daily_listening (user_id, listen_date, minutes_listened)
                    VALUES (%s, %s, %s)
                    ON DUPLICATE KEY UPDATE minutes_listened = VALUES(minutes_listened)
                """, (user_id, date_str, minutes_listened))

            # 4) Insert genre distribution
            genres = processed_data.get("genres", {})
            genre_labels = genres.get("labels", [])
            genre_sizes = genres.get("sizes", [])
            for g_label, g_count in zip(genre_labels, genre_sizes):
                cur.execute("""
                    INSERT INTO user_genres (user_id, genre, play_count, record_date)
                    VALUES (%s, %s, %s, %s)
                    ON DUPLICATE KEY UPDATE play_count = VALUES(play_count)
                """, (user_id, g_label, g_count, record_date))

            # 5) Insert top tracks
            top_tracks = processed_data.get("top_tracks", [])
            for track in top_tracks:
                track_id = track.get("track_id", "")
                track_name = track.get("track_name", "")
                artist_name = track.get("artist_name", "")
                popularity = track.get("popularity", 0)
                track_rank = track.get("rank", 0)  # stored as track_rank in the table

                cur.execute("""
                    INSERT INTO user_top_tracks
                      (user_id, track_id, track_name, artist_name, popularity, track_rank, record_date)
                    VALUES
                      (%s, %s, %s, %s, %s, %s, %s)
                    ON DUPLICATE KEY UPDATE
                      track_name = VALUES(track_name),
                      artist_name = VALUES(artist_name),
                      popularity = VALUES(popularity),
                      track_rank = VALUES(track_rank)
                """, (user_id, track_id, track_name, artist_name, popularity, track_rank, record_date))

        conn.commit()
    except Exception as e:
        conn.rollback()
        print(f"Error inserting into RDS: {e}")
        raise
    finally:
        conn.close()

def lambda_handler(event, context):
    print("Lambda function started")
    s3_client = boto3.client('s3')
    
    # Retrieve raw file details from the event
    raw_bucket = event['Records'][0]['s3']['bucket']['name']
    raw_key = event['Records'][0]['s3']['object']['key']
    print(f"Triggered by bucket: {raw_bucket}, key: {raw_key}")

    # Read raw JSON from S3
    try:
        response = s3_client.get_object(Bucket=raw_bucket, Key=raw_key)
        raw_data = json.loads(response['Body'].read())
        print("Successfully read raw data")
    except Exception as e:
        print(f"Error reading raw file: {e}")
        return {'statusCode': 500, 'body': str(e)}

    # Extract user_id and display_name (passed from authenticate_and_extract)
    user_id = raw_data.get("user_id", "unknown_user")
    display_name = raw_data.get("display_name", "Unknown")

    # -------------- Build processed_data (unchanged from your logic) ---------------
    # ... For brevity, we assume your existing code that calculates:
    #     genre_part, top_10_artists, top_10_tracks, mainstream_score, daily listening, etc.
    #     Once done, you produce 'processed_data' as below:

    processed_data = {
        "user_id": user_id,
        "display_name": display_name,
        # "genres": genre_part,
        # "top_artists": top_10_artists,
        # "top_tracks": top_10_tracks,
        # "listening_time": {...},
        # "mainstream_score": mainstream_score,
        # "day_vs_night": {...}
    }

    # (A) Insert data into MySQL RDS
    store_in_rds(processed_data)

    # (B) Upload final processed JSON to S3
    processed_bucket = 'spotify-processed-data-dk'
    processed_key = raw_key.replace("raw/", "processed/").replace(".json", ".processed.json")
    try:
        s3_client.put_object(
            Bucket=processed_bucket,
            Key=processed_key,
            Body=json.dumps(processed_data),
            ContentType='application/json'
        )
        print("Successfully uploaded processed data")
    except Exception as e:
        print(f"Error uploading processed data: {e}")
        return {'statusCode': 500, 'body': str(e)}

    return {'statusCode': 200, 'body': "Transformation complete"}
