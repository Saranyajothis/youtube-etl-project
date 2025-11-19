### Key Features

- 🎥 **Automated Data Collection** from YouTube Data API v3
- 🌍 **Multi-Region Analysis** across 4 countries (US, IN, GB, PK)
- 🧠 **Intelligent Classification** using category + keyword-based sentiment analysis
- ☁️ **Cloud-Native Architecture** with Azure Blob Storage and Snowflake
- 🔐 **Secure Credential Management** using Azure Key Vault for API keys and passwords
- 📈 **Scalable Design** collecting ~250 videos daily
- 🔄 **Daily Refresh** capability using Azure Functions for scheduled triggers


==== SECTION 2: Replace entire "Architecture" section with this ====

## 🏗️ Architecture

```
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
```

**Data Flow:**
1. **Secure Access:** Azure Functions retrieves API keys from Azure Key Vault
2. **Extract:** YouTube API collects video metadata, statistics, and channel info
3. **Transform:** Sentiment classification using category mapping + keyword analysis
4. **Load:** Raw data to Azure → Processed data to Snowflake
5. **Automate:** Azure Functions triggers pipeline daily via scheduled CRON job
6. **Analyze:** SQL aggregations for regional sentiment patterns


==== SECTION 3: Add "Azure CLI 2.0+" to Technical Requirements ====

### Technical Requirements
- Python 3.8 or higher
- Azure CLI 2.0+
- 10GB+ free disk space
- Stable internet connection


==== SECTION 4: Add these NEW sections AFTER "Setup Virtual Environment" and BEFORE "Configure Environment Variables" ====

### 3. Azure Infrastructure Setup
```bash
# Login to Azure
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
```

### 4. Snowflake Configuration
```sql
-- Create Database & Schema
CREATE DATABASE YOUTUBE_ANALYTICS;
CREATE SCHEMA YOUTUBE_ANALYTICS.CORE;
CREATE SCHEMA YOUTUBE_ANALYTICS.ANALYTICS;

-- Create External Stage
CREATE STAGE YOUTUBE_ANALYTICS.CORE.AZURE_STAGE
    URL = 'azure://youtubeetlstorage.blob.core.windows.net/youtube-data'
    CREDENTIALS = (AZURE_SAS_TOKEN = 'your-sas-token');

-- Run schema creation scripts
!source sql/01_create_schema.sql
```

### 5. Deploy Azure Function
```bash
# Create Function App
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
```


==== SECTION 5: Update numbering - Change existing sections 3,4,5 to 6,7,8 ====

### 6. Configure Environment Variables
[keep existing content]

### 7. Setup Snowflake Database
[remove this section - already added above as section 4]

### 8. Run the Pipeline

**Manual Execution:**
```bash
python src/youtube_collector.py
python src/snowflake_loader.py
```

**Automated Scheduling (Azure Functions):**
Pipeline automatically runs daily at midnight UTC via Azure Functions Timer Trigger


==== SECTION 6: Replace "Sample Results" table with this ====

## 📊 Sample Results

```
╔════════════╦══════════╦══════════╦═════════╗
║   Region   ║ Positive ║ Negative ║ Neutral ║
╠════════════╬══════════╬══════════╬═════════╣
║ India (IN) ║   64%    ║   22%    ║   14%   ║
║ USA (US)   ║   56%    ║   30%    ║   14%   ║
║ UK (GB)    ║   52%    ║   35%    ║   13%   ║
║ Pak. (PK)  ║   60%    ║   26%    ║   14%   ║
╚════════════╩══════════╩══════════╩═════════╝
```


==== SECTION 7: Replace "Search Parameters" in Configuration section ====

### Search Parameters
```python
REGIONS = ['US', 'IN', 'GB', 'PK']
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


==== SECTION 8: Add to "Learning Outcomes" section (at the end of the list) ====

✅ **Azure Functions** - Serverless compute for scheduled automation  
✅ **Azure Key Vault** - Secure credential management
✅ **Snowflake Data Warehouse** - Cloud data warehousing and SQL analytics


==== SECTION 9: In "Customization" section, update the example ====

### Add New Regions
```python
# In src/config.py
REGIONS = ['US', 'IN', 'GB', 'PK', 'DE', 'FR']
```

<img width="726" height="674" alt="Screenshot 2025-11-18 at 6 19 39 PM" src="https://github.com/user-attachments/assets/2de1956c-dcee-42a2-9324-4c4c5dc98b9a" />



