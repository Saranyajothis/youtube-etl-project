import azure.functions as func
import logging
import json
import os
from datetime import datetime
from googleapiclient.discovery import build
from azure.storage.blob import BlobServiceClient
import snowflake.connector

# Configuration class
class AzureFunctionConfig:
    def __init__(self):
        # YouTube API
        self.YOUTUBE_API_KEY = os.environ.get('YOUTUBE_API_KEY')
        
        # Azure Storage
        self.AZURE_CONNECTION_STRING = os.environ.get('AZURE_CONNECTION_STRING')
        self.AZURE_CONTAINER_NAME = os.environ.get('AZURE_CONTAINER_NAME', 'youtube-data')
        
        # Snowflake
        self.SNOWFLAKE_USER = os.environ.get('SNOWFLAKE_USER')
        self.SNOWFLAKE_PASSWORD = os.environ.get('SNOWFLAKE_PASSWORD')
        self.SNOWFLAKE_ACCOUNT = os.environ.get('SNOWFLAKE_ACCOUNT')
        self.SNOWFLAKE_WAREHOUSE = os.environ.get('SNOWFLAKE_WAREHOUSE', 'COMPUTE_WH')
        self.SNOWFLAKE_DATABASE = os.environ.get('SNOWFLAKE_DATABASE', 'YOUTUBE_ANALYTICS')
        self.SNOWFLAKE_SCHEMA = os.environ.get('SNOWFLAKE_SCHEMA', 'CORE')
        
        # Search Configuration
        self.REGIONS = ['US', 'GB', 'CA', 'AU']
        self.SEARCH_KEYWORDS = ['technology', 'AI', 'machine learning', 'data science']
        self.VIDEOS_PER_KEYWORD = 5
        
        # Sentiment Classification
        self.POSITIVE_KEYWORDS = ['amazing', 'great', 'excellent', 'best', 'awesome']
        self.NEGATIVE_KEYWORDS = ['terrible', 'worst', 'bad', 'awful', 'horrible']
        self.POSITIVE_CATEGORIES = [28]  # Science & Technology
        self.NEGATIVE_CATEGORIES = [25]  # News & Politics
        self.MIXED_CATEGORIES = [22, 23, 24]  # People & Blogs, Comedy, Entertainment
    
    def validate(self):
        """Validate required configuration"""
        required = [
            'YOUTUBE_API_KEY', 'AZURE_CONNECTION_STRING', 
            'SNOWFLAKE_USER', 'SNOWFLAKE_PASSWORD', 'SNOWFLAKE_ACCOUNT',
            'SNOWFLAKE_WAREHOUSE', 'SNOWFLAKE_DATABASE'
        ]
        missing = [key for key in required if not getattr(self, key)]
        if missing:
            raise ValueError(f"Missing required configuration: {missing}")

# YouTube Collector Service
class YouTubeCollectorService:
    def __init__(self, config):
        self.config = config
        self.youtube = build('youtube', 'v3', developerKey=config.YOUTUBE_API_KEY)
        self.blob_service = BlobServiceClient.from_connection_string(config.AZURE_CONNECTION_STRING)
        self.container_client = self.blob_service.get_container_client(config.AZURE_CONTAINER_NAME)
        self.logger = logging.getLogger('YouTubeCollector')
    
    def search_videos(self, keyword, region_code, max_results=10):
        """Search for videos by keyword and region"""
        try:
            request = self.youtube.search().list(
                part='id,snippet',
                q=keyword,
                type='video',
                regionCode=region_code,
                maxResults=max_results,
                relevanceLanguage='en',
                order='relevance',
                publishedAfter=(datetime.now().replace(hour=0, minute=0, second=0, microsecond=0).isoformat() + 'Z')
            )
            response = request.execute()
            video_ids = []
            for item in response.get('items', []):
                if item['id']['kind'] == 'youtube#video':
                    video_ids.append(item['id']['videoId'])
            return video_ids

        except Exception as e:
            self.logger.error(f"Error searching videos for {keyword} in {region_code}: {e}")
            return []

    def get_video_details(self, video_ids):
        """Get detailed information for videos"""
        try:
            all_videos = []
            for i in range(0, len(video_ids), 50):
                batch = video_ids[i:i+50]
                request = self.youtube.videos().list(
                    part='snippet,statistics,contentDetails',
                    id=','.join(batch)
                )
                response = request.execute()
                all_videos.extend(response.get('items', []))
            return all_videos
        except Exception as e:
            self.logger.error(f"Error getting video details: {e}")
            return []

    def get_channel_details(self, channel_ids):
        """Get channel information"""
        try:
            all_channels = []
            for i in range(0, len(channel_ids), 50):
                batch = channel_ids[i:i+50]
                request = self.youtube.channels().list(
                    part='snippet,statistics',
                    id=','.join(batch)
                )
                response = request.execute()
                all_channels.extend(response.get('items', []))
            return all_channels
        except Exception as e:
            self.logger.error(f"Error getting channel details: {e}")
            return []

    def classify_video(self, video_data):
        """Classify video sentiment"""
        category_id = int(video_data['snippet']['categoryId'])
        title = video_data['snippet']['title']
        description = video_data['snippet'].get('description', '')
        tags = video_data['snippet'].get('tags', [])
        
        # Combine text
        combined_text = f"{title} {description} {' '.join(tags)}".lower()
        
        # Count keywords
        pos_count = sum(1 for word in self.config.POSITIVE_KEYWORDS if word in combined_text)
        neg_count = sum(1 for word in self.config.NEGATIVE_KEYWORDS if word in combined_text)
        
        # Classification logic
        if category_id in self.config.POSITIVE_CATEGORIES:
            sentiment = 'POSITIVE'
            method = 'CATEGORY_BASED'
        elif category_id in self.config.NEGATIVE_CATEGORIES:
            sentiment = 'NEGATIVE'
            method = 'CATEGORY_BASED'
        elif category_id in self.config.MIXED_CATEGORIES:
            if pos_count > neg_count:
                sentiment = 'POSITIVE'
            elif neg_count > pos_count:
                sentiment = 'NEGATIVE'
            else:
                sentiment = 'NEUTRAL'
            method = 'KEYWORD_BASED'
        else:
            sentiment = 'UNKNOWN'
            method = 'UNCATEGORIZED'
            
        return {
            'final_sentiment': sentiment,
            'classification_method': method,
            'positive_keyword_count': pos_count,
            'negative_keyword_count': neg_count
        }

    def calculate_engagement(self, statistics):
        """Calculate engagement rate"""
        views = int(statistics.get('viewCount', 0))
        likes = int(statistics.get('likeCount', 0))
        comments = int(statistics.get('commentCount', 0))
        
        if views == 0:
            return 0.0
            
        engagement_rate = ((likes + comments) / views) * 100
        return round(engagement_rate, 4)

    def collect_data(self):
        """Main collection function"""
        self.logger.info("Starting YouTube data collection")
        all_videos = []
        all_channels = {}
        
        for region in self.config.REGIONS:
            self.logger.info(f"Processing region: {region}")
            for keyword in self.config.SEARCH_KEYWORDS:
                self.logger.info(f"Searching for: {keyword} in {region}")
                video_ids = self.search_videos(keyword, region, self.config.VIDEOS_PER_KEYWORD)
                
                if not video_ids:
                    continue
                
                videos = self.get_video_details(video_ids)
                
                for video in videos:
                    classification = self.classify_video(video)
                    engagement = self.calculate_engagement(video.get('statistics', {}))
                    
                    video_record = {
                        'video_id': video['id'],
                        'channel_id': video['snippet']['channelId'],
                        'category_id': int(video['snippet']['categoryId']),
                        'title': video['snippet']['title'],
                        'description': video['snippet'].get('description', ''),
                        'tags': video['snippet'].get('tags', []),
                        'published_at': video['snippet']['publishedAt'],
                        'view_count': int(video['statistics'].get('viewCount', 0)),
                        'like_count': int(video['statistics'].get('likeCount', 0)),
                        'comment_count': int(video['statistics'].get('commentCount', 0)),
                        'engagement_rate': engagement,
                        'search_keyword': keyword,
                        'search_region': region,
                        'collected_at': datetime.now().isoformat(),
                        **classification
                    }
                    all_videos.append(video_record)
                    all_channels[video['snippet']['channelId']] = None

        self.logger.info(f"Collected {len(all_videos)} videos from {len(all_channels)} channels")
        
        # Get channel details
        channel_ids = list(all_channels.keys())
        channels = self.get_channel_details(channel_ids)
        
        channel_records = []
        for channel in channels:
            channel_records.append({
                'channel_id': channel['id'],
                'channel_title': channel['snippet']['title'],
                'channel_country': channel['snippet'].get('country', 'UNKNOWN'),
                'subscriber_count': int(channel['statistics'].get('subscriberCount', 0)),
                'video_count': int(channel['statistics'].get('videoCount', 0))
            })
            
        return all_videos, channel_records

    def upload_to_azure(self, videos, channels):
        """Upload data to Azure Blob Storage"""
        try:
            now = datetime.now()
            date_path = f"raw/{now.year}/{now.month:02d}/{now.day:02d}"
            timestamp = now.strftime("%Y%m%d_%H%M%S")
            
            # Upload videos
            videos_blob_name = f"{date_path}/videos_{timestamp}.json"
            videos_blob = self.container_client.get_blob_client(videos_blob_name)
            videos_blob.upload_blob(json.dumps(videos, indent=2), overwrite=True)
            
            # Upload channels
            channels_blob_name = f"{date_path}/channels_{timestamp}.json"
            channels_blob = self.container_client.get_blob_client(channels_blob_name)
            channels_blob.upload_blob(json.dumps(channels, indent=2), overwrite=True)
            
            # Create metadata
            metadata = {
                'collection_date': now.strftime('%Y-%m-%d'),
                'collection_time': now.strftime('%H:%M:%S'),
                'total_videos': len(videos),
                'total_channels': len(channels),
                'regions': self.config.REGIONS,
                'keywords': self.config.SEARCH_KEYWORDS,
                'function_execution': True
            }
            
            metadata_blob_name = f"{date_path}/metadata_{timestamp}.json"
            metadata_blob = self.container_client.get_blob_client(metadata_blob_name)
            metadata_blob.upload_blob(json.dumps(metadata, indent=2), overwrite=True)
            
            self.logger.info(f"Successfully uploaded to Azure: {len(videos)} videos, {len(channels)} channels")
            return True
            
        except Exception as e:
            self.logger.error(f"Error uploading to Azure: {e}")
            return False

# # Complete Snowflake Loader Service
# class SnowflakeLoaderService:
#     def __init__(self, config):
#         self.config = config
#         self.logger = logging.getLogger('SnowflakeLoader')
#         self.conn = None
#         self.cursor = None

#     def connect(self):
#         """Connect to Snowflake"""
#         try:
#             self.conn = snowflake.connector.connect(
#                 user=self.config.SNOWFLAKE_USER,
#                 password=self.config.SNOWFLAKE_PASSWORD,
#                 account=self.config.SNOWFLAKE_ACCOUNT,
#                 warehouse=self.config.SNOWFLAKE_WAREHOUSE,
#                 database=self.config.SNOWFLAKE_DATABASE
#             )
#             self.cursor = self.conn.cursor()
#             self.cursor.execute(f"USE DATABASE {self.config.SNOWFLAKE_DATABASE}")
#             self.cursor.execute(f"USE SCHEMA {self.config.SNOWFLAKE_SCHEMA}")
#             self.logger.info("Connected to Snowflake")
#             return True
#         except Exception as e:
#             self.logger.error(f"Failed to connect to Snowflake: {e}")
#             return False

#     def load_todays_data(self):
#         """Load today's data from Azure to Snowflake"""
#         if not self.connect():
#             return False
        
#         try:
#             today = datetime.now()
#             date_path = f"raw/{today.year}/{today.month:02d}/{today.day:02d}"
#             self.logger.info(f"Loading data from: {date_path}")
            
#             # Step 1: Load videos to staging
#             self.logger.info("Loading videos to staging...")
#             self._load_videos_to_staging(date_path)
            
#             # Step 2: Load channels
#             self.logger.info("Loading channels...")
#             self._load_channels(date_path)
            
#             # Step 3: Load video facts
#             self.logger.info("Loading video facts...")
#             self._load_video_facts()
            
#             # Step 4: Refresh aggregations
#             self.logger.info("Refreshing aggregations...")
#             self._refresh_aggregations()
            
#             # Step 5: Cleanup staging
#             self.logger.info("Cleaning up staging...")
#             self._cleanup_staging()
            
#             self.logger.info("Snowflake loading completed successfully")
#             return True
            
#         except Exception as e:
#             self.logger.error(f"Error loading to Snowflake: {e}")
#             return False
#         finally:
#             self.close()

#     def _load_videos_to_staging(self, date_path):
#         """Load videos to staging table"""
#         # Create staging table
#         self.cursor.execute("""
#             CREATE TABLE IF NOT EXISTS RAW.STG_VIDEOS (
#                 raw_json VARIANT,
#                 load_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
#                 file_name VARCHAR(500)
#             )
#         """)
        
#         # Create temp table using FLATTEN to unpack JSON array
#         self.cursor.execute(f"""
#             CREATE OR REPLACE TEMPORARY TABLE TEMP_VIDEOS AS
#             SELECT 
#                 value AS raw_json,
#                 METADATA$FILENAME AS file_name
#             FROM @YOUTUBE_ANALYTICS.RAW.YOUTUBE_STAGE/{date_path}/ (FILE_FORMAT => 'MY_JSON_FORMAT'),
#                 LATERAL FLATTEN(input => $1)
#             WHERE METADATA$FILENAME LIKE '%videos_%'
#         """)
        
#         # Insert unpacked records
#         self.cursor.execute("""
#             INSERT INTO RAW.STG_VIDEOS (raw_json, file_name)
#             SELECT raw_json, file_name FROM TEMP_VIDEOS
#         """)

#     def _load_channels(self, date_path):
#         """Load channel data"""
#         self.cursor.execute(f"""
#             CREATE OR REPLACE TEMPORARY TABLE TEMP_CHANNELS AS
#             SELECT
#                 value:channel_id::VARCHAR as channel_id,
#                 value:channel_title::VARCHAR as channel_title,
#                 value:channel_country::VARCHAR as channel_country,
#                 value:subscriber_count::NUMBER as subscriber_count,
#                 value:video_count::NUMBER as video_count
#             FROM @YOUTUBE_ANALYTICS.RAW.YOUTUBE_STAGE/{date_path}/ (FILE_FORMAT => 'MY_JSON_FORMAT'),
#                 LATERAL FLATTEN(input => $1) t
#             WHERE METADATA$FILENAME LIKE '%channels_%'
#             AND value:channel_id IS NOT NULL
#             QUALIFY ROW_NUMBER() OVER (PARTITION BY value:channel_id ORDER BY METADATA$FILENAME DESC) = 1
#         """)

#         # MERGE into DIM_CHANNELS
#         self.cursor.execute("""
#             MERGE INTO YOUTUBE_ANALYTICS.CORE.DIM_CHANNELS tgt
#             USING TEMP_CHANNELS src
#             ON tgt.channel_id = src.channel_id
#             WHEN MATCHED THEN UPDATE SET
#                 tgt.channel_title = src.channel_title,
#                 tgt.channel_country = src.channel_country,
#                 tgt.subscriber_count = src.subscriber_count,
#                 tgt.video_count = src.video_count,
#                 tgt.last_updated = CURRENT_TIMESTAMP()
#             WHEN NOT MATCHED THEN INSERT (
#                 channel_id, channel_title, channel_country,
#                 subscriber_count, video_count, first_seen_date
#             )
#             VALUES (
#                 src.channel_id, src.channel_title, src.channel_country,
#                 src.subscriber_count, src.video_count, CURRENT_DATE()
#             )
#         """)

#     def _load_video_facts(self):
#         """Load video facts from staging"""
#         self.cursor.execute("""
#             INSERT INTO YOUTUBE_ANALYTICS.CORE.FACT_VIDEOS
#             SELECT 
#                 raw_json:video_id::VARCHAR,
#                 raw_json:channel_id::VARCHAR,
#                 raw_json:category_id::INT,
#                 raw_json:title::VARCHAR,
#                 raw_json:description::TEXT,
#                 raw_json:tags::ARRAY,
#                 raw_json:final_sentiment::VARCHAR,
#                 raw_json:classification_method::VARCHAR,
#                 raw_json:positive_keyword_count::INT,
#                 raw_json:negative_keyword_count::INT,
#                 raw_json:view_count::NUMBER,
#                 raw_json:like_count::NUMBER,
#                 raw_json:comment_count::NUMBER,
#                 raw_json:engagement_rate::FLOAT,
#                 raw_json:published_at::TIMESTAMP_NTZ,
#                 raw_json:collected_at::TIMESTAMP_NTZ,
#                 DATE(raw_json:collected_at::TIMESTAMP_NTZ),
#                 raw_json:search_keyword::VARCHAR,
#                 raw_json:search_region::VARCHAR
#             FROM YOUTUBE_ANALYTICS.RAW.STG_VIDEOS
#             WHERE raw_json:video_id IS NOT NULL
#         """)

#     def _refresh_aggregations(self):
#         """Refresh daily aggregations"""
#         # Delete today's data first
#         self.cursor.execute("""
#             DELETE FROM YOUTUBE_ANALYTICS.ANALYTICS.AGG_DAILY_BY_REGION 
#             WHERE analysis_date = CURRENT_DATE()
#         """)
        
#         # Insert fresh aggregations
#         self.cursor.execute("""
#             INSERT INTO YOUTUBE_ANALYTICS.ANALYTICS.AGG_DAILY_BY_REGION
#             SELECT 
#                 CURRENT_DATE() as analysis_date,
#                 ch.channel_country,
#                 v.final_sentiment,
#                 COUNT(*) as video_count,
#                 SUM(v.view_count) as total_views,
#                 SUM(v.like_count) as total_likes,
#                 SUM(v.comment_count) as total_comments,
#                 AVG(v.engagement_rate) as avg_engagement_rate
#             FROM YOUTUBE_ANALYTICS.CORE.FACT_VIDEOS v
#             JOIN YOUTUBE_ANALYTICS.CORE.DIM_CHANNELS ch ON v.channel_id = ch.channel_id
#             WHERE v.collection_date = CURRENT_DATE()
#             GROUP BY ch.channel_country, v.final_sentiment
#         """)

#     def _cleanup_staging(self):
#         """Clean up staging table"""
#         self.cursor.execute("TRUNCATE TABLE YOUTUBE_ANALYTICS.RAW.STG_VIDEOS")

#     def close(self):
#         """Close connection"""
#         if self.cursor:
#             self.cursor.close()
#         if self.conn:
#             self.conn.close()

class SnowflakeLoaderService:
    def __init__(self, config):
        self.config = config
        self.logger = logging.getLogger('SnowflakeLoader')
        self.conn = None
        self.cursor = None

    def connect(self):
        """Connect to Snowflake"""
        try:
            self.conn = snowflake.connector.connect(
                user=self.config.SNOWFLAKE_USER,
                password=self.config.SNOWFLAKE_PASSWORD,
                account=self.config.SNOWFLAKE_ACCOUNT,
                warehouse=self.config.SNOWFLAKE_WAREHOUSE,
                database=self.config.SNOWFLAKE_DATABASE
            )
            self.cursor = self.conn.cursor()
            self.cursor.execute(f"USE DATABASE {self.config.SNOWFLAKE_DATABASE}")
            self.cursor.execute(f"USE SCHEMA {self.config.SNOWFLAKE_SCHEMA}")
            self.logger.info("Connected to Snowflake")
            return True
        except Exception as e:
            self.logger.error(f"Failed to connect to Snowflake: {e}")
            return False

    def load_todays_data(self):
        """Load today's data from Azure to Snowflake"""
        if not self.connect():
            return False
        
        try:
            today = datetime.now()
            date_path = f"raw/{today.year}/{today.month:02d}/{today.day:02d}"
            self.logger.info(f"Loading data from: {date_path}")
            
            # Step 1: Load videos to staging with error handling
            try:
                self.logger.info("Loading videos to staging...")
                self._load_videos_to_staging(date_path)
                self.conn.commit()  # COMMIT AFTER STAGING
                self.logger.info("✅ Videos loaded to staging successfully")
            except Exception as e:
                self.logger.error(f"❌ FAILED loading videos to staging: {str(e)}")
                self.conn.rollback()  # Rollback on error
                return False
            
            # Step 2: Load channels with error handling
            try:
                self.logger.info("Loading channels...")
                self._load_channels(date_path)
                self.conn.commit()  # COMMIT AFTER CHANNELS
                self.logger.info("✅ Channels loaded successfully")
            except Exception as e:
                self.logger.error(f"❌ FAILED loading channels: {str(e)}")
                self.conn.rollback()  # Rollback on error
                return False
            
            # Step 3: Load video facts with error handling
            try:
                self.logger.info("Loading video facts...")
                self._load_video_facts()
                self.conn.commit()  # COMMIT AFTER FACTS
                self.logger.info("✅ Video facts loaded successfully")
            except Exception as e:
                self.logger.error(f"❌ FAILED loading video facts: {str(e)}")
                self.conn.rollback()  # Rollback on error
                return False
            
            # Step 4: Refresh aggregations (optional - don't fail if this errors)
            try:
                self.logger.info("Refreshing aggregations...")
                self._refresh_aggregations()
                self.conn.commit()  # COMMIT AFTER AGGREGATIONS
                self.logger.info("✅ Aggregations refreshed successfully")
            except Exception as e:
                self.logger.error(f"⚠️  Aggregations failed (non-critical): {str(e)}")
                self.conn.rollback()
            
            # Step 5: Cleanup staging (optional - don't fail if this errors)
            try:
                self.logger.info("Cleaning up staging...")
                self._cleanup_staging()
                self.conn.commit()  # COMMIT AFTER CLEANUP
                self.logger.info("✅ Staging cleaned up successfully")
            except Exception as e:
                self.logger.error(f"⚠️  Staging cleanup failed (non-critical): {str(e)}")
                self.conn.rollback()
            
            self.logger.info("Snowflake loading completed successfully")
            return True
            
        except Exception as e:
            self.logger.error(f"Error loading to Snowflake: {e}")
            self.conn.rollback()  # Rollback any uncommitted changes
            return False
        finally:
            self.close()

    def _load_videos_to_staging(self, date_path):
        """Load videos to staging table"""
        # Create staging table with FULL DATABASE.SCHEMA path
        self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS YOUTUBE_ANALYTICS.RAW.STG_VIDEOS (
                raw_json VARIANT,
                load_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
                file_name VARCHAR(500)
            )
        """)
        
        # FIXED: Use fully qualified file format name
        self.cursor.execute(f"""
            CREATE OR REPLACE TEMPORARY TABLE TEMP_VIDEOS AS
            SELECT 
                value AS raw_json,
                METADATA$FILENAME AS file_name
            FROM @YOUTUBE_ANALYTICS.RAW.YOUTUBE_STAGE/{date_path}/ 
                (FILE_FORMAT => YOUTUBE_ANALYTICS.RAW.MY_JSON_FORMAT),
                LATERAL FLATTEN(input => $1)
            WHERE METADATA$FILENAME LIKE '%videos_%'
        """)
        
        # Insert unpacked records with FULL DATABASE.SCHEMA path
        self.cursor.execute("""
            INSERT INTO YOUTUBE_ANALYTICS.RAW.STG_VIDEOS (raw_json, file_name)
            SELECT raw_json, file_name FROM TEMP_VIDEOS
        """)

    def _load_channels(self, date_path):
        """Load channel data"""
        # FIXED: Use fully qualified file format name
        self.cursor.execute(f"""
            CREATE OR REPLACE TEMPORARY TABLE TEMP_CHANNELS AS
            SELECT
                value:channel_id::VARCHAR as channel_id,
                value:channel_title::VARCHAR as channel_title,
                value:channel_country::VARCHAR as channel_country,
                value:subscriber_count::NUMBER as subscriber_count,
                value:video_count::NUMBER as video_count
            FROM @YOUTUBE_ANALYTICS.RAW.YOUTUBE_STAGE/{date_path}/ 
                (FILE_FORMAT => YOUTUBE_ANALYTICS.RAW.MY_JSON_FORMAT),
                LATERAL FLATTEN(input => $1) t
            WHERE METADATA$FILENAME LIKE '%channels_%'
            AND value:channel_id IS NOT NULL
            QUALIFY ROW_NUMBER() OVER (PARTITION BY value:channel_id ORDER BY METADATA$FILENAME DESC) = 1
        """)

        # MERGE into DIM_CHANNELS with FULL DATABASE.SCHEMA path
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

    def _load_video_facts(self):
        """Load video facts from staging using MERGE to handle duplicates"""
        # FIXED: Use MERGE instead of INSERT to handle duplicate keys
        self.cursor.execute("""
            MERGE INTO YOUTUBE_ANALYTICS.CORE.FACT_VIDEOS tgt
            USING (
                SELECT DISTINCT
                    raw_json:video_id::VARCHAR as video_id,
                    raw_json:channel_id::VARCHAR as channel_id,
                    raw_json:category_id::INT as category_id,
                    raw_json:title::VARCHAR as title,
                    raw_json:description::TEXT as description,
                    raw_json:tags::ARRAY as tags,
                    raw_json:final_sentiment::VARCHAR as final_sentiment,
                    raw_json:classification_method::VARCHAR as classification_method,
                    raw_json:positive_keyword_count::INT as positive_keyword_count,
                    raw_json:negative_keyword_count::INT as negative_keyword_count,
                    raw_json:view_count::NUMBER as view_count,
                    raw_json:like_count::NUMBER as like_count,
                    raw_json:comment_count::NUMBER as comment_count,
                    raw_json:engagement_rate::FLOAT as engagement_rate,
                    raw_json:published_at::TIMESTAMP_NTZ as published_at,
                    raw_json:collected_at::TIMESTAMP_NTZ as collected_at,
                    DATE(raw_json:collected_at::TIMESTAMP_NTZ) as collection_date,
                    raw_json:search_keyword::VARCHAR as search_keyword,
                    raw_json:search_region::VARCHAR as search_region
                FROM YOUTUBE_ANALYTICS.RAW.STG_VIDEOS
                WHERE raw_json:video_id IS NOT NULL
            ) src
            ON tgt.video_id = src.video_id
            WHEN NOT MATCHED THEN INSERT (
                video_id, channel_id, category_id, title, description, tags,
                final_sentiment, classification_method, positive_keyword_count,
                negative_keyword_count, view_count, like_count, comment_count,
                engagement_rate, published_at, collected_at, collection_date,
                search_keyword, search_region
            )
            VALUES (
                src.video_id, src.channel_id, src.category_id, src.title, 
                src.description, src.tags, src.final_sentiment, 
                src.classification_method, src.positive_keyword_count,
                src.negative_keyword_count, src.view_count, src.like_count, 
                src.comment_count, src.engagement_rate, src.published_at, 
                src.collected_at, src.collection_date, src.search_keyword, 
                src.search_region
            )
        """)

    def _refresh_aggregations(self):
        """Refresh daily aggregations"""
        # Create schema and table if they don't exist
        self.cursor.execute("CREATE SCHEMA IF NOT EXISTS YOUTUBE_ANALYTICS.ANALYTICS")
        
        self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS YOUTUBE_ANALYTICS.ANALYTICS.AGG_DAILY_BY_REGION (
                analysis_date DATE,
                channel_country VARCHAR(10),
                final_sentiment VARCHAR(20),
                video_count NUMBER,
                total_views NUMBER,
                total_likes NUMBER,
                total_comments NUMBER,
                avg_engagement_rate FLOAT
            )
        """)
        
        # Delete today's data first
        self.cursor.execute("""
            DELETE FROM YOUTUBE_ANALYTICS.ANALYTICS.AGG_DAILY_BY_REGION 
            WHERE analysis_date = CURRENT_DATE()
        """)
        
        # Insert fresh aggregations
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

    def _cleanup_staging(self):
        """Clean up staging table"""
        self.cursor.execute("TRUNCATE TABLE YOUTUBE_ANALYTICS.RAW.STG_VIDEOS")

    def close(self):
        """Close connection"""
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()

# Create the Azure Function App
app = func.FunctionApp()

@app.timer_trigger(schedule="0 0 6 * * *", arg_name="myTimer", run_on_startup=False,
              use_monitor=False) 
def YouTubeCollectorFunction(myTimer: func.TimerRequest) -> None:
    """Main Azure Function entry point"""
    # Setup logging
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger('YouTubeETLFunction')

    if myTimer.past_due:
        logger.info('The timer is past due!')

    logger.info('YouTube ETL Function started')

    try:
        # Initialize configuration
        config = AzureFunctionConfig()
        config.validate()

        # Step 1: Collect YouTube data
        logger.info("Starting YouTube data collection...")
        collector = YouTubeCollectorService(config)
        videos, channels = collector.collect_data()

        if not videos:
            logger.warning("No videos collected, ending execution")
            return

        # Step 2: Upload to Azure
        logger.info("Uploading to Azure Blob Storage...")
        upload_success = collector.upload_to_azure(videos, channels)

        if not upload_success:
            logger.error("Failed to upload to Azure")
            return

        # Step 3: Load to Snowflake (with complete implementation)
        logger.info("Loading to Snowflake...")
        snowflake_loader = SnowflakeLoaderService(config)
        load_success = snowflake_loader.load_todays_data()

        if load_success:
            logger.info(f"YouTube ETL completed successfully! Processed {len(videos)} videos from {len(channels)} channels")
        else:
            logger.error("Snowflake loading failed")

    except Exception as e:
        logger.error(f"YouTube ETL failed: {e}")
        raise