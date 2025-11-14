import snowflake.connector
from datetime import datetime
from src.config import Config

class SnowflakeLoader:
    def __init__(self):
        print("🔌 Connecting to Snowflake...")
        self.conn = snowflake.connector.connect(
            user=Config.SNOWFLAKE_USER,
            password=Config.SNOWFLAKE_PASSWORD,
            account=Config.SNOWFLAKE_ACCOUNT,
            warehouse=Config.SNOWFLAKE_WAREHOUSE,
            database=Config.SNOWFLAKE_DATABASE
        )
        self.cursor = self.conn.cursor()
        self.cursor.execute(f"USE DATABASE {Config.SNOWFLAKE_DATABASE}")
        self.cursor.execute(f"USE SCHEMA {Config.SNOWFLAKE_SCHEMA}")
        print("Connected to Snowflake\n")

    def load_todays_data(self):
        today = datetime.now()
        date_path = f"raw/{today.year}/{today.month:02d}/{today.day:02d}"
        print(f"Loading data from: {date_path}\n")
        try:
            print("Step 1: Loading videos to staging...")
            self.load_videos_to_staging(date_path)
            print("Step 2: Loading channels...")
            self.load_channels(date_path)
            print("Step 3: Loading video facts...")
            self.load_video_facts()
            print("Step 4: Refreshing aggregations...")
            self.refresh_aggregations()
            print("Step 5: Cleaning up staging...")
            self.cleanup_staging()
            print("\nSnowflake loading completed successfully!\n")
            self.print_summary()
            return True
        except Exception as e:
            print(f"\n Error loading to Snowflake: {e}\n")
            return False

    def load_videos_to_staging(self, date_path):
        # 1. Create the staging table if not exists
        self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS RAW.STG_VIDEOS (
                raw_json VARIANT,
                load_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
                file_name VARCHAR(500)
            )
        """)
        # 2. Create a temp table using FLATTEN to unpack the JSON array
        self.cursor.execute(f"""
            CREATE OR REPLACE TEMPORARY TABLE TEMP_VIDEOS AS
            SELECT 
                value AS raw_json,
                METADATA$FILENAME AS file_name
            FROM @YOUTUBE_ANALYTICS.RAW.AZURE_STAGE/{date_path}/ (FILE_FORMAT => 'MY_JSON_FORMAT'),
                LATERAL FLATTEN(input => $1)
            WHERE METADATA$FILENAME LIKE '%videos_%'
        """)
        # 3. Insert unpacked records into staging table
        self.cursor.execute("""
            INSERT INTO RAW.STG_VIDEOS (raw_json, file_name)
            SELECT raw_json, file_name FROM TEMP_VIDEOS
        """)
        print("Videos loaded to staging.")


    def load_channels(self, date_path):
        self.cursor.execute(f"""
        CREATE OR REPLACE TEMPORARY TABLE TEMP_CHANNELS AS
        SELECT
            value:channel_id::VARCHAR as channel_id,
            value:channel_title::VARCHAR as channel_title,
            value:channel_country::VARCHAR as channel_country,
            value:subscriber_count::NUMBER as subscriber_count,
            value:video_count::NUMBER as video_count
        FROM @YOUTUBE_ANALYTICS.RAW.AZURE_STAGE/{date_path}/ (FILE_FORMAT => 'MY_JSON_FORMAT'),
            LATERAL FLATTEN(input => $1) t
        WHERE METADATA$FILENAME LIKE '%channels_%'
        AND value:channel_id IS NOT NULL
        QUALIFY ROW_NUMBER() OVER (PARTITION BY value:channel_id ORDER BY METADATA$FILENAME DESC) = 1
    """)


        self.cursor.execute("""
        MERGE INTO YOUTUBE_ANALYTICS.CORE.DIM_CHANNELS tgt
        USING TEMP_CHANNELS src
        ON tgt.channel_id = src.channel_id
        WHEN MATCHED THEN UPDATE SET
            tgt.channel_title = src.channel_title,
            tgt.channel_country = src.channel_country,
            tgt.subscriber_count = src.subscriber_count,
            tgt.video_count = src.video_count,
            tgt.last_updated = CURRENT_TIMESTAMP()
        WHEN NOT MATCHED THEN INSERT (
            channel_id, channel_title, channel_country,
            subscriber_count, video_count, first_seen_date
        )
        VALUES (
            src.channel_id, src.channel_title, src.channel_country,
            src.subscriber_count, src.video_count, CURRENT_DATE()
        )
        """)
        print("Channel merge completed.")

    def load_video_facts(self):
        self.cursor.execute("""
        INSERT INTO YOUTUBE_ANALYTICS.CORE.FACT_VIDEOS
        SELECT 
            raw_json:video_id::VARCHAR,
            raw_json:channel_id::VARCHAR,
            raw_json:category_id::INT,
            raw_json:title::VARCHAR,
            raw_json:description::TEXT,
            raw_json:tags::ARRAY,
            raw_json:final_sentiment::VARCHAR,
            raw_json:classification_method::VARCHAR,
            raw_json:positive_keyword_count::INT,
            raw_json:negative_keyword_count::INT,
            raw_json:view_count::NUMBER,
            raw_json:like_count::NUMBER,
            raw_json:comment_count::NUMBER,
            raw_json:engagement_rate::FLOAT,
            raw_json:published_at::TIMESTAMP_NTZ,
            raw_json:collected_at::TIMESTAMP_NTZ,
            DATE(raw_json:collected_at::TIMESTAMP_NTZ),
            raw_json:search_keyword::VARCHAR,
            raw_json:search_region::VARCHAR
        FROM YOUTUBE_ANALYTICS.RAW.STG_VIDEOS
        WHERE raw_json:video_id IS NOT NULL
        """)
        print("Videos loaded to fact table.")

    def refresh_aggregations(self):
        self.cursor.execute("""
        DELETE FROM YOUTUBE_ANALYTICS.ANALYTICS.AGG_DAILY_BY_REGION 
        WHERE analysis_date = CURRENT_DATE()
        """)
        self.cursor.execute("""
        INSERT INTO YOUTUBE_ANALYTICS.ANALYTICS.AGG_DAILY_BY_REGION
        SELECT 
            CURRENT_DATE() as analysis_date,
            ch.channel_country,
            v.final_sentiment,
            COUNT(*) as video_count,
            SUM(v.view_count) as total_views,
            SUM(v.like_count) as total_likes,
            SUM(v.comment_count) as total_comments,
            AVG(v.engagement_rate) as avg_engagement_rate
        FROM YOUTUBE_ANALYTICS.CORE.FACT_VIDEOS v
        JOIN YOUTUBE_ANALYTICS.CORE.DIM_CHANNELS ch ON v.channel_id = ch.channel_id
        WHERE v.collection_date = CURRENT_DATE()
        GROUP BY ch.channel_country, v.final_sentiment
        """)
        print("Aggregations refreshed.")

    def cleanup_staging(self):
        self.cursor.execute("TRUNCATE TABLE YOUTUBE_ANALYTICS.RAW.STG_VIDEOS")
        print(" Staging table cleaned.")

    def print_summary(self):
        print(f"{'='*60}")
        print("SNOWFLAKE DATA SUMMARY")
        print(f"{'='*60}\n")
        self.cursor.execute("""
        SELECT 
            (SELECT COUNT(*) FROM YOUTUBE_ANALYTICS.CORE.DIM_CHANNELS) as channels,
            (SELECT COUNT(*) FROM YOUTUBE_ANALYTICS.CORE.FACT_VIDEOS) as videos,
            (SELECT COUNT(*) FROM YOUTUBE_ANALYTICS.ANALYTICS.AGG_DAILY_BY_REGION) as aggregations
        """)
        result = self.cursor.fetchone()
        print(f"Total Records:")
        print(f"   Channels:     {result[0]:,}")
        print(f"   Videos:       {result[1]:,}")
        print(f"   Aggregations: {result[2]:,}")
        self.cursor.execute("""
        SELECT 
            channel_country,
            final_sentiment,
            video_count
        FROM YOUTUBE_ANALYTICS.ANALYTICS.AGG_DAILY_BY_REGION
        WHERE analysis_date = CURRENT_DATE()
        ORDER BY channel_country, video_count DESC
        """)
        print(f"\n Today's Sentiment by Region:")
        current_region = None
        for row in self.cursor:
            if row[0] != current_region:
                current_region = row[0]
                print(f"\n   {row[0]}:")
            print(f"      {row[1]:12} {row[2]:4} videos")
        print(f"\n{'='*60}\n")

    def close(self):
        self.cursor.close()
        self.conn.close()

def main():
    try:
        Config.validate()
        loader = SnowflakeLoader()
        success = loader.load_todays_data()
        loader.close()
        return 0 if success else 1
    except Exception as e:
        print(f"\n Error: {e}\n")
        return 1

if __name__ == "__main__":
    exit(main())
