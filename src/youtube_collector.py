import json
from datetime import datetime
from googleapiclient.discovery import build
from azure.storage.blob import BlobServiceClient
from src.config import Config

class YouTubeCollector:
    def __init__(self):
        self.youtube = build('youtube', 'v3', developerKey=Config.YOUTUBE_API_KEY)
        self.blob_service = BlobServiceClient.from_connection_string(Config.AZURE_CONNECTION_STRING)
        self.container_client = self.blob_service.get_container_client(Config.AZURE_CONTAINER_NAME)
    
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
                order='relevance'
            )
            response = request.execute()
            
            video_ids = []
            for item in response.get('items', []):
                if item['id']['kind'] == 'youtube#video':
                    video_ids.append(item['id']['videoId'])
            
            return video_ids
        
        except Exception as e:
            print(f"Error searching videos: {e}")
            return []
    
    def get_video_details(self, video_ids):
        """Get detailed information for videos"""
        try:
            request = self.youtube.videos().list(
                part='snippet,statistics,contentDetails',
                id=','.join(video_ids)
            )
            response = request.execute()
            return response.get('items', [])
        
        except Exception as e:
            print(f"Error getting video details: {e}")
            return []
    
    def get_channel_details(self, channel_ids):
        """Get channel information"""
        try:
            request = self.youtube.channels().list(
                part='snippet,statistics',
                id=','.join(channel_ids)
            )
            response = request.execute()
            return response.get('items', [])
        
        except Exception as e:
            print(f"Error getting channel details: {e}")
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
        pos_count = sum(1 for word in Config.POSITIVE_KEYWORDS if word in combined_text)
        neg_count = sum(1 for word in Config.NEGATIVE_KEYWORDS if word in combined_text)
        
        # Classification logic
        if category_id in Config.POSITIVE_CATEGORIES:
            sentiment = 'POSITIVE'
            method = 'CATEGORY_BASED'
        elif category_id in Config.NEGATIVE_CATEGORIES:
            sentiment = 'NEGATIVE'
            method = 'CATEGORY_BASED'
        elif category_id in Config.MIXED_CATEGORIES:
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
        print(f"\n{'='*60}")
        print(f"Starting YouTube Data Collection")
        print(f"Time: {datetime.now()}")
        print(f"{'='*60}\n")
        
        if Config.DRY_RUN:
            print("⚠️  DRY RUN MODE - Collecting limited data for testing\n")
        
        all_videos = []
        all_channels = {}
        
        # Collect videos
        for region in Config.REGIONS:
            print(f"📍 Region: {region}")
            
            for keyword in Config.SEARCH_KEYWORDS:
                print(f"   🔍 Searching: '{keyword}'", end='')
                
                video_ids = self.search_videos(keyword, region, Config.VIDEOS_PER_KEYWORD)
                print(f" → Found {len(video_ids)} videos")
                
                if not video_ids:
                    continue
                
                # Get video details
                videos = self.get_video_details(video_ids)
                
                # Process each video
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
            
            print()
        
        print(f"Collected {len(all_videos)} videos from {len(all_channels)} channels\n")
        
        # Get channel details
        print("📺 Fetching channel details...")
        channel_ids = list(all_channels.keys())
        
        for i in range(0, len(channel_ids), 50):
            batch = channel_ids[i:i+50]
            channels = self.get_channel_details(batch)
            
            for channel in channels:
                all_channels[channel['id']] = {
                    'channel_id': channel['id'],
                    'channel_title': channel['snippet']['title'],
                    'channel_country': channel['snippet'].get('country', 'UNKNOWN'),
                    'subscriber_count': int(channel['statistics'].get('subscriberCount', 0)),
                    'video_count': int(channel['statistics'].get('videoCount', 0))
                }
        
        print(f"Fetched details for {len(all_channels)} channels\n")
        
        return all_videos, list(all_channels.values())
    
    def upload_to_azure(self, videos, channels):
        """Upload data to Azure Blob Storage"""
        print("☁️  Uploading to Azure Blob Storage...")
        
        try:
            # Create date-based path
            now = datetime.now()
            date_path = f"raw/{now.year}/{now.month:02d}/{now.day:02d}"
            timestamp = now.strftime("%Y%m%d_%H%M%S")
            
            # Upload videos
            videos_blob_name = f"{date_path}/videos_{timestamp}.json"
            videos_blob = self.container_client.get_blob_client(videos_blob_name)
            videos_blob.upload_blob(json.dumps(videos, indent=2), overwrite=True)
            print(f"Videos: {videos_blob_name}")
            
            # Upload channels
            channels_blob_name = f"{date_path}/channels_{timestamp}.json"
            channels_blob = self.container_client.get_blob_client(channels_blob_name)
            channels_blob.upload_blob(json.dumps(channels, indent=2), overwrite=True)
            print(f"Channels: {channels_blob_name}")
            
            # Create metadata
            metadata = {
                'collection_date': now.strftime('%Y-%m-%d'),
                'collection_time': now.strftime('%H:%M:%S'),
                'total_videos': len(videos),
                'total_channels': len(channels),
                'regions': Config.REGIONS,
                'keywords': Config.SEARCH_KEYWORDS
            }
            
            metadata_blob_name = f"{date_path}/metadata_{timestamp}.json"
            metadata_blob = self.container_client.get_blob_client(metadata_blob_name)
            metadata_blob.upload_blob(json.dumps(metadata, indent=2), overwrite=True)
            print(f" Metadata: {metadata_blob_name}")
            
            return True
        
        except Exception as e:
            print(f"Error uploading to Azure: {e}")
            return False
    
    def print_summary(self, videos):
        """Print collection summary"""
        print(f"\n{'='*60}")
        print("COLLECTION SUMMARY")
        print(f"{'='*60}")
        
        # Sentiment distribution
        sentiment_counts = {}
        for video in videos:
            sentiment = video['final_sentiment']
            sentiment_counts[sentiment] = sentiment_counts.get(sentiment, 0) + 1
        
        print("\n Sentiment Distribution:")
        for sentiment, count in sorted(sentiment_counts.items()):
            percentage = (count / len(videos)) * 100
            print(f"   {sentiment:12} {count:4} ({percentage:5.1f}%)")
        
        # Region distribution
        region_counts = {}
        for video in videos:
            region = video['search_region']
            region_counts[region] = region_counts.get(region, 0) + 1
        
        print("\n Region Distribution:")
        for region, count in sorted(region_counts.items()):
            print(f"{region:4} {count:4} videos")
        
        print(f"\n{'='*60}\n")

def main():
    """Main execution"""
    try:
        # Validate configuration
        Config.validate()
        
        # Create collector
        collector = YouTubeCollector()
        
        # Collect data
        videos, channels = collector.collect_data()
        
        # Upload to Azure
        success = collector.upload_to_azure(videos, channels)
        
        if success:
            collector.print_summary(videos)
            print("Data collection completed successfully!\n")
            return 0
        else:
            print(" Data collection failed\n")
            return 1
    
    except Exception as e:
        print(f"\nError: {e}\n")
        return 1

if __name__ == "__main__":
    exit(main())