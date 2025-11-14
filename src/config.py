import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv('config/.env')

class Config:
    """Configuration for YouTube ETL Pipeline"""
    
    # API Keys
    YOUTUBE_API_KEY = os.getenv('YOUTUBE_API_KEY')
    AZURE_CONNECTION_STRING = os.getenv('AZURE_STORAGE_CONNECTION_STRING')
    AZURE_CONTAINER_NAME = os.getenv('AZURE_CONTAINER_NAME', 'youtube-raw-data')
    
    # Snowflake
    SNOWFLAKE_USER = os.getenv('SNOWFLAKE_USER')
    SNOWFLAKE_PASSWORD = os.getenv('SNOWFLAKE_PASSWORD')
    SNOWFLAKE_ACCOUNT = os.getenv('SNOWFLAKE_ACCOUNT')
    SNOWFLAKE_WAREHOUSE = os.getenv('SNOWFLAKE_WAREHOUSE', 'COMPUTE_WH')
    SNOWFLAKE_DATABASE = os.getenv('SNOWFLAKE_DATABASE', 'YOUTUBE_ANALYTICS')
    SNOWFLAKE_SCHEMA = os.getenv('SNOWFLAKE_SCHEMA', 'CORE')
    
    # Classification Keywords
    POSITIVE_KEYWORDS = [
        'tutorial', 'guide', 'learn', 'teach', 'education', 'how-to', 'tips',
        'inspire', 'motivate', 'success', 'improve', 'growth', 'help',
        'advice', 'solution'
    ]
    
    NEGATIVE_KEYWORDS = [
        'drama', 'exposed', 'controversy', 'scandal', 'fail', 'worst',
        'terrible', 'hate', 'trash', 'clickbait', 'rant', 'angry',
        'crisis', 'disaster', 'warning'
    ]
    
    # Category Classifications
    POSITIVE_CATEGORIES = [19, 26, 27, 28, 29]
    NEGATIVE_CATEGORIES = [20, 23, 24, 25]
    MIXED_CATEGORIES = [1, 2, 10, 15, 17, 22]
    
    # Search Configuration
    REGIONS = ['US', 'IN', 'GB', 'CA', 'AU']
    SEARCH_KEYWORDS = ['technology', 'lifestyle', 'tutorial', 'daily vlog', 'news update', 'gaming']
    VIDEOS_PER_KEYWORD = 10
    
    # For testing - set to True to collect less data
    DRY_RUN = False
    
    @classmethod
    def validate(cls):
        """Validate required configuration"""
        errors = []
        
        if not cls.YOUTUBE_API_KEY:
            errors.append("YOUTUBE_API_KEY not set")
        if not cls.AZURE_CONNECTION_STRING:
            errors.append("AZURE_STORAGE_CONNECTION_STRING not set")
        if not cls.SNOWFLAKE_USER:
            errors.append("SNOWFLAKE_USER not set")
            
        if errors:
            raise ValueError(f"Configuration errors: {', '.join(errors)}")
        
        print("Configuration validated")
        return True

# For dry run testing
if Config.DRY_RUN:
    Config.REGIONS = ['US']
    Config.SEARCH_KEYWORDS = ['technology', 'tutorial']
    Config.VIDEOS_PER_KEYWORD = 2
