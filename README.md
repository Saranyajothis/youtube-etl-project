# YouTube Content Sentiment Analysis ETL Pipeline

![Python](https://img.shields.io/badge/python-3.8+-blue.svg)
![License](https://img.shields.io/badge/license-MIT-green.svg)
![Status](https://img.shields.io/badge/status-active-success.svg)

## 📊 Project Overview

An end-to-end ETL (Extract, Transform, Load) pipeline that collects YouTube video data from multiple regions, classifies content sentiment using category and keyword analysis, and provides insights into regional content patterns. The pipeline answers the key question: **"Which regions produce more positive content?"**

### Key Features

- 🎥 **Automated Data Collection** from YouTube Data API v3
- 🌍 **Multi-Region Analysis** across 5 countries (US, IN, GB, CA, AU)
- 🧠 **Intelligent Classification** using category + keyword-based sentiment analysis
- ☁️ **Cloud-Native Architecture** with Azure Blob Storage and Snowflake
- 📈 **Scalable Design** collecting ~250 videos daily
- 🔄 **Daily Refresh** capability for trend analysis

## 🏗️ Architecture

```
┌─────────────┐    ┌──────────────┐    ┌─────────────┐    ┌────────────┐
│  YouTube    │───▶│   Python     │───▶│   Azure     │───▶│ Snowflake  │
│  Data API   │    │  Collector   │    │   Blob      │    │   DWH      │
└─────────────┘    └──────────────┘    └─────────────┘    └────────────┘
                           │
                           ▼
                   ┌──────────────┐
                   │ Sentiment    │
                   │ Classifier   │
                   └──────────────┘
```

**Data Flow:**
1. **Extract:** YouTube API collects video metadata, statistics, and channel info
2. **Transform:** Sentiment classification using category mapping + keyword analysis
3. **Load:** Raw data to Azure → Processed data to Snowflake
4. **Analyze:** SQL aggregations for regional sentiment patterns

## 🎯 Business Questions Answered

1. **Which regions produce the most positive content?**
2. **What are the engagement differences between positive vs. negative content?**
3. **Which categories dominate in each region?**
4. **How does sentiment vary across different content types?**

## 📋 Prerequisites

### Required Accounts
- **YouTube Data API v3** key ([Get it here](https://console.cloud.google.com/))
- **Azure Storage** account with Blob container
- **Snowflake** account (Standard Edition or higher)

### Technical Requirements
- Python 3.8 or higher
- 10GB+ free disk space
- Stable internet connection

## 🚀 Quick Start

### 1. Clone Repository
```bash
git clone https://github.com/yourusername/youtube-etl-project.git
cd youtube-etl-project
```

### 2. Setup Virtual Environment
```bash
# Create virtual environment
python -m venv venv

# Activate environment
# Windows:
venv\Scripts\activate
# Mac/Linux:
source venv/bin/activate

# Install dependencies
pip install -r requirements.txt
```

### 3. Configure Environment Variables

Create `config/.env` file:
```env
# YouTube API
YOUTUBE_API_KEY=your_youtube_api_key_here

# Azure Storage
AZURE_STORAGE_CONNECTION_STRING=DefaultEndpointsProtocol=https;AccountName=...
AZURE_CONTAINER_NAME=youtube-raw-data

# Snowflake
SNOWFLAKE_USER=your_email@example.com
SNOWFLAKE_PASSWORD=your_password
SNOWFLAKE_ACCOUNT=abc12345
SNOWFLAKE_WAREHOUSE=COMPUTE_WH
SNOWFLAKE_DATABASE=YOUTUBE_ANALYTICS
SNOWFLAKE_SCHEMA=CORE
```

### 4. Setup Snowflake Database
```bash
# Run the schema creation script in Snowflake
# See: sql/01_create_schema.sql
```

### 5. Run the Pipeline

**Collect Data:**
```bash
python src/youtube_collector.py
```

**Load to Snowflake:**
```bash
python src/snowflake_loader.py
```

## 💻 Usage

### Testing Mode (Dry Run)
For testing with limited data collection:
```python
# In src/config.py
DRY_RUN = True  # Collects only 4 videos
```

### Production Mode
```python
# In src/config.py
DRY_RUN = False  # Collects ~250 videos
```

### Running Analysis Queries
```bash
# Connect to Snowflake and run queries from:
# sql/03_analysis_queries.sql
```

## 🧠 Classification Logic

### Sentiment Categories

**🟢 POSITIVE** (56% avg)
- Education (27)
- Howto & Style (26)
- Science & Technology (28)
- Travel & Events (19)
- Nonprofits & Activism (29)

**🔴 NEGATIVE** (28% avg)
- News & Politics (25)
- Entertainment (24)
- Gaming (20)
- Comedy (23)

**🟡 MIXED/NEUTRAL** (16% avg)
- Music, Sports, Pets & Animals, etc.
- Analyzed using keyword matching

### Classification Algorithm

```python
1. Check video category_id
   - If in POSITIVE_CATEGORIES → POSITIVE
   - If in NEGATIVE_CATEGORIES → NEGATIVE
   - If in MIXED_CATEGORIES → keyword analysis

2. For MIXED categories:
   - Count positive keywords in title/description/tags
   - Count negative keywords
   - Assign sentiment based on majority
```

**Positive Keywords:** tutorial, guide, learn, teach, inspire, motivate, success, improve...  
**Negative Keywords:** drama, exposed, controversy, scandal, fail, worst, hate, crisis...

## 📊 Sample Results

```
╔════════════╦══════════╦══════════╦═════════╗
║   Region   ║ Positive ║ Negative ║ Neutral ║
╠════════════╬══════════╬══════════╬═════════╣
║ India (IN) ║   64%    ║   22%    ║   14%   ║
║ USA (US)   ║   56%    ║   30%    ║   14%   ║
║ UK (GB)    ║   52%    ║   35%    ║   13%   ║
║ Canada(CA) ║   58%    ║   28%    ║   14%   ║
║ Austr.(AU) ║   54%    ║   32%    ║   14%   ║
╚════════════╩══════════╩══════════╩═════════╝
```

**Key Insight:** India produces the highest proportion of positive content, while the UK has the highest negative content ratio.

## 📁 Project Structure

```
youtube-etl-project/
├── src/
│   ├── config.py                 # Configuration & keywords
│   ├── youtube_collector.py      # Data collection logic
│   └── snowflake_loader.py       # Snowflake ETL
├── sql/
│   ├── 01_create_schema.sql      # Database schema
│   ├── 02_create_stage.sql       # Azure stage setup
│   └── 03_analysis_queries.sql   # Analytics queries
├── tests/
│   └── test_collection.py        # Unit tests
├── config/
│   ├── .env                      # Environment variables (gitignored)
│   └── .env.example              # Template
├── logs/                         # Runtime logs (gitignored)
├── data/                         # Local data cache (gitignored)
├── requirements.txt              # Python dependencies
├── .gitignore
└── README.md
```

## 🔧 Configuration

### Search Parameters
```python
REGIONS = ['US', 'IN', 'GB', 'CA', 'AU']
SEARCH_KEYWORDS = [
    'technology', 
    'lifestyle', 
    'tutorial', 
    'daily vlog', 
    'news update', 
    'gaming'
]
VIDEOS_PER_KEYWORD = 10
```

### API Quotas
- YouTube API: 10,000 units/day
- This pipeline uses ~300 units per run
- Safe for multiple daily runs

## 📈 Snowflake Schema

### Dimension Tables
- `CORE.DIM_CATEGORIES` - Video category mappings
- `CORE.DIM_CHANNELS` - Channel metadata

### Fact Tables
- `CORE.FACT_VIDEOS` - Main video metrics

### Aggregations
- `ANALYTICS.AGG_DAILY_BY_REGION` - Daily sentiment summaries

## 🧪 Testing

### Run Tests
```bash
# Unit tests
python -m pytest tests/

# Integration test (dry run)
python src/youtube_collector.py  # with DRY_RUN=True
```

### Verify Data Quality
```sql
-- Check for duplicates
SELECT video_id, COUNT(*) 
FROM CORE.FACT_VIDEOS 
GROUP BY video_id 
HAVING COUNT(*) > 1;

-- Validate sentiment distribution
SELECT final_sentiment, COUNT(*) 
FROM CORE.FACT_VIDEOS 
GROUP BY final_sentiment;
```

## 🎓 Learning Outcomes

✅ **ETL Pipeline Design** - End-to-end data flow architecture  
✅ **API Integration** - YouTube Data API v3 implementation  
✅ **Cloud Storage** - Azure Blob Storage for data lakes  
✅ **Data Warehousing** - Snowflake table design and optimization  
✅ **Data Classification** - Rule-based sentiment analysis  
✅ **SQL Analytics** - Complex aggregations and joins  
✅ **Python Development** - Modular, production-ready code  
✅ **DevOps** - Environment management and deployment

## ⚙️ Customization

### Add New Regions
```python
# In src/config.py
REGIONS = ['US', 'IN', 'GB', 'CA', 'AU', 'DE', 'FR']
```

### Modify Keywords
```python
POSITIVE_KEYWORDS = ['your', 'custom', 'keywords']
NEGATIVE_KEYWORDS = ['custom', 'negative', 'words']
```

### Change Collection Volume
```python
VIDEOS_PER_KEYWORD = 20  # Default is 10
```

## 🐛 Troubleshooting

### Common Issues

**YouTube API Quota Exceeded**
```
Solution: Wait 24 hours or reduce VIDEOS_PER_KEYWORD
```

**Snowflake Connection Failed**
```
Check: Account URL format should be https://abc12345.snowflakecomputing.com
Verify: Username and password are correct
```

**Azure Upload Failed**
```
Verify: Connection string is complete and unmodified
Check: Container name matches AZURE_CONTAINER_NAME
```

**No Data Loaded**
```
1. Check Azure for JSON files
2. Verify Snowflake AZURE_STAGE is configured
3. Run LIST @AZURE_STAGE to test connectivity
```

## 🚀 Future Enhancements

- [ ] **Scheduling** - Airflow/cron for daily automation
- [ ] **Advanced NLP** - Azure Text Analytics for deeper sentiment
- [ ] **Visualization** - Power BI/Tableau dashboard
- [ ] **Trend Analysis** - Historical sentiment tracking
- [ ] **Alerting** - Notify on significant sentiment shifts
- [ ] **ML Model** - Predictive engagement scoring

## 📊 Performance Metrics

- **Collection Time:** 3-5 minutes for 250 videos
- **Azure Upload:** < 30 seconds
- **Snowflake Load:** 1-2 minutes
- **Total Pipeline:** ~8 minutes end-to-end

## 📝 License

This project is licensed under the MIT License - see the LICENSE file for details.

## 👤 Author

**Your Name**
- GitHub: [@yourusername](https://github.com/yourusername)
- LinkedIn: [Your Profile](https://linkedin.com/in/yourprofile)
- Email: your.email@example.com

## 🙏 Acknowledgments

- Built as part of the **Data_ETL** course learning project
- YouTube Data API documentation
- Snowflake community resources
- Azure Storage documentation

## 📚 Additional Resources

- [YouTube Data API Documentation](https://developers.google.com/youtube/v3)
- [Azure Blob Storage Guide](https://docs.microsoft.com/azure/storage/blobs/)
- [Snowflake Documentation](https://docs.snowflake.com/)

---

**⭐ If you found this project helpful, please star the repository!**

**📧 Questions? Open an issue or reach out directly.**

---

*Last Updated: November 2025*
