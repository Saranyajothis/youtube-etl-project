YouTube Content Sentiment Analysis ETL Pipeline

📊 Project Overview
An end-to-end ETL (Extract, Transform, Load) pipeline that collects YouTube video data from multiple regions, classifies content sentiment using category and keyword analysis, and provides insights into regional content patterns. The pipeline answers the key question: "Which regions produce more positive content?"
Key Features

🎥 Automated Data Collection from YouTube Data API v3
🌍 Multi-Region Analysis across 4 countries (US, IN, GB, PK)
🧠 Intelligent Classification using category + keyword-based sentiment analysis
☁️ Cloud-Native Architecture with Azure Blob Storage and Snowflake
🔐 Secure Credential Management using Azure Key Vault for API keys and passwords
📈 Scalable Design collecting ~250 videos daily
🔄 Daily Refresh capability using Azure Functions for scheduled triggers


🏗️ Architecture
┌─────────────┐    ┌──────────────┐    ┌─────────────┐    ┌────────────┐
│  YouTube    │───▶│ Azure        │───▶│   Azure     │───▶│ Snowflake  │
│  Data API   │    │ Functions    │    │   Blob      │    │   DWH      │
└─────────────┘    └──────────────┘    └─────────────┘    └────────────┘
                           │                    
                           ▼                    
                   ┌──────────────┐    ┌──────────────┐
                   │ Azure Key    │────│ Sentiment    │
                   │   Vault      │    │ Classifier   │
                   └──────────────┘    └──────────────┘
Data Flow:

Secure Access: Azure Functions retrieves API keys from Azure Key Vault
Extract: YouTube API collects video metadata, statistics, and channel info
Transform: Sentiment classification using category mapping + keyword analysis
Load: Raw data to Azure → Processed data to Snowflake
Automate: Azure Functions triggers pipeline daily via scheduled CRON job
Analyze: SQL aggregations for regional sentiment patterns

🎯 Business Questions Answered

Which regions produce the most positive content?
What are the engagement differences between positive vs. negative content?
Which categories dominate in each region?
How does sentiment vary across different content types?

📋 Prerequisites
Required Accounts

YouTube Data API v3 key (Get it here)
Azure Storage account with Blob container
Snowflake account (Standard Edition or higher)

Technical Requirements

Python 3.8 or higher
Azure CLI 2.0+
10GB+ free disk space
Stable internet connection

🚀 Quick Start
1. Clone Repository
bashgit clone https://github.com/yourusername/youtube-etl-project.git
cd youtube-etl-project
2. Setup Virtual Environment
bash# Create virtual environment
python -m venv venv

# Activate environment
# Windows:
venv\Scripts\activate
# Mac/Linux:
source venv/bin/activate

# Install dependencies
pip install -r requirements.txt
3. Azure Infrastructure Setup
bash# Login to Azure
az login

# Create Resource Group
az group create --name youtube-etl-rg --location eastus

# Create Storage Account
az storage account create \
    --name youtubeetlstorage \
    --resource-group youtube-etl-rg \
    --sku Standard_LRS

# Create Key Vault
az keyvault create \
    --name youtube-etl-vault \
    --resource-group youtube-etl-rg \
    --location eastus

# Store Secrets
az keyvault secret set \
    --vault-name youtube-etl-vault \
    --name youtube-api-key \
    --value "YOUR_YOUTUBE_API_KEY"

az keyvault secret set \
    --vault-name youtube-etl-vault \
    --name snowflake-password \
    --value "YOUR_SNOWFLAKE_PASSWORD"
4. Snowflake Configuration
sql-- Create Database & Schema
CREATE DATABASE YOUTUBE_ANALYTICS;
CREATE SCHEMA YOUTUBE_ANALYTICS.CORE;
CREATE SCHEMA YOUTUBE_ANALYTICS.ANALYTICS;

-- Create External Stage
CREATE STAGE YOUTUBE_ANALYTICS.CORE.AZURE_STAGE
    URL = 'azure://youtubeetlstorage.blob.core.windows.net/youtube-data'
    CREDENTIALS = (AZURE_SAS_TOKEN = 'your-sas-token');

-- Run schema creation scripts
!source sql/01_create_schema.sql
5. Deploy Azure Function
bash# Create Function App
az functionapp create \
    --resource-group youtube-etl-rg \
    --consumption-plan-location eastus \
    --runtime python \
    --runtime-version 3.8 \
    --functions-version 4 \
    --name youtube-etl-function \
    --storage-account youtubeetlstorage

# Configure Function App Settings
az functionapp config appsettings set \
    --name youtube-etl-function \
    --resource-group youtube-etl-rg \
    --settings "SNOWFLAKE_ACCOUNT=your_account" \
               "SNOWFLAKE_USER=your_email" \
               "SNOWFLAKE_DATABASE=YOUTUBE_ANALYTICS" \
               "SNOWFLAKE_WAREHOUSE=COMPUTE_WH"

# Deploy code
func azure functionapp publish youtube-etl-function
6. Configure Environment Variables
Create config/.env file:
env# YouTube API
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
7. Run the Pipeline
Manual Execution:
bashpython src/youtube_collector.py
python src/snowflake_loader.py
Automated Scheduling (Azure Functions):
Pipeline automatically runs daily at midnight UTC via Azure Functions Timer Trigger
💻 Usage
Testing Mode (Dry Run)
For testing with limited data collection:
python# In src/config.py
DRY_RUN = True  # Collects only 4 videos
Production Mode
python# In src/config.py
DRY_RUN = False  # Collects ~250 videos
Running Analysis Queries
bash# Connect to Snowflake and run queries from:
# sql/03_analysis_queries.sql
🧠 Classification Logic
Sentiment Categories
🟢 POSITIVE (56% avg)

Education (27)
Howto & Style (26)
Science & Technology (28)
Travel & Events (19)
Nonprofits & Activism (29)

🔴 NEGATIVE (28% avg)

News & Politics (25)
Entertainment (24)
Gaming (20)
Comedy (23)

🟡 MIXED/NEUTRAL (16% avg)

Music, Sports, Pets & Animals, etc.
Analyzed using keyword matching

Classification Algorithm
python1. Check video category_id
   - If in POSITIVE_CATEGORIES → POSITIVE
   - If in NEGATIVE_CATEGORIES → NEGATIVE
   - If in MIXED_CATEGORIES → keyword analysis

2. For MIXED categories:
   - Count positive keywords in title/description/tags
   - Count negative keywords
   - Assign sentiment based on majority
Positive Keywords: tutorial, guide, learn, teach, inspire, motivate, success, improve...
Negative Keywords: drama, exposed, controversy, scandal, fail, worst, hate, crisis...
📊 Sample Results
╔════════════╦══════════╦══════════╦═════════╗
║   Region   ║ Positive ║ Negative ║ Neutral ║
╠════════════╬══════════╬══════════╬═════════╣
║ India (IN) ║   64%    ║   22%    ║   14%   ║
║ USA (US)   ║   56%    ║   30%    ║   14%   ║
║ UK (GB)    ║   52%    ║   35%    ║   13%   ║
║ Pak. (PK)  ║   60%    ║   26%    ║   14%   ║
╚════════════╩══════════╩══════════╩═════════╝
Key Insight: India produces the highest proportion of positive content, while the UK has the highest negative content ratio.
📁 Project Structure
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
🔧 Configuration
Search Parameters
pythonREGIONS = ['US', 'IN', 'GB', 'PK']
SEARCH_KEYWORDS = [
    'technology', 
    'lifestyle', 
    'tutorial', 
    'daily vlog', 
    'news update', 
    'gaming'
]
VIDEOS_PER_KEYWORD = 10
API Quotas

YouTube API: 10,000 units/day
This pipeline uses ~300 units per run
Safe for multiple daily runs

📈 Snowflake Schema
Dimension Tables

CORE.DIM_CATEGORIES - Video category mappings
CORE.DIM_CHANNELS - Channel metadata

Fact Tables

CORE.FACT_VIDEOS - Main video metrics

Aggregations

ANALYTICS.AGG_DAILY_BY_REGION - Daily sentiment summaries

🧪 Testing
Run Tests
bash# Unit tests
python -m pytest tests/

# Integration test (dry run)
python src/youtube_collector.py  # with DRY_RUN=True
Verify Data Quality
sql-- Check for duplicates
SELECT video_id, COUNT(*) 
FROM CORE.FACT_VIDEOS 
GROUP BY video_id 
HAVING COUNT(*) > 1;

-- Validate sentiment distribution
SELECT final_sentiment, COUNT(*) 
FROM CORE.FACT_VIDEOS 
GROUP BY final_sentiment;
🎓 Learning Outcomes
✅ ETL Pipeline Design - End-to-end data flow architecture
✅ API Integration - YouTube Data API v3 implementation
✅ Cloud Storage - Azure Blob Storage for data lakes
✅ Data Warehousing - Snowflake table design and optimization
✅ Data Classification - Rule-based sentiment analysis
✅ SQL Analytics - Complex aggregations and joins
✅ Python Development - Modular, production-ready code
✅ DevOps - Environment management and deployment
✅ Azure Functions - Server

