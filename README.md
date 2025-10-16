\# JetDB v1.o



\*\*Big data for the rest of us\*\*



![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)
![Python](https://img.shields.io/badge/Python-3.11+-blue.svg)
![FastAPI](https://img.shields.io/badge/FastAPI-0.104+-00a393.svg)
![React](https://img.shields.io/badge/React-18+-61dafb.svg)
![TypeScript](https://img.shields.io/badge/TypeScript-5+-3178c6.svg)



Handle 10GB+ CSV files with the simplicity of a spreadsheet. No more Excel crashes, no complex ETL pipelines—just drag, drop, and analyze.


✨ What is JetDB?
JetDB is a full-stack web application that lets you work with massive datasets (up to 10GB) using a familiar spreadsheet interface. Built for analysts, data scientists, and anyone who's ever crashed Excel with too much data.
The Problem: Traditional spreadsheets choke on large files. ETL tools are overkill for quick analysis.
The Solution: JetDB combines the speed of DuckDB with a familiar UI, plus AI-powered queries.

🚀 Key Features
Core Functionality

📊 Spreadsheet Interface - Familiar grid view with sorting, filtering, and search
⚡ Lightning Fast - Powered by DuckDB for sub-second queries on millions of rows
🤖 AI Queries - Ask questions in plain English: "Show top 10 customers by revenue"
💾 Smart Storage - Automatic Parquet conversion for 3-5x compression
🔗 Dataset Merging - Combine multiple CSVs with schema validation

Technical Highlights

🎯 Parallel Processing - 40% faster uploads with streaming and parallel Parquet conversion
🔐 Secure - JWT authentication, rate limiting, input validation
☁️ Cloud-Native - Azure Blob Storage backend with SAS token security
📈 Scalable - Handles 100M+ rows with pagination and virtualization
🐳 Docker Ready - One-command deployment with docker-compose


🏗️ Architecture
┌─────────────┐     ┌──────────────┐     ┌─────────────┐
│   Browser   │────▶│  FastAPI     │────▶│  DuckDB     │
│   (React)   │◀────│  Backend     │◀────│  Engine     │
└─────────────┘     └──────────────┘     └─────────────┘
                           │
                           ├──▶ Azure Blob Storage
                           ├──▶ Supabase Auth
                           └──▶ OpenAI GPT-4o-mini
Tech Stack
Backend

FastAPI 0.104+ - Modern async Python framework
DuckDB 0.9.2 - In-memory analytical database
PyArrow 14.0+ - Fast Parquet conversion
Azure Storage - Cloud file storage
Supabase - Authentication & database

Frontend

React 18 - UI framework
TypeScript 5+ - Type safety
Handsontable - Spreadsheet component
Axios - HTTP client


📦 Quick Start
Prerequisites

Python 3.11+
Node.js 18+
Docker & Docker Compose (optional)

Option 1: Docker (Recommended)
bash# Clone repo
git clone https://github.com/benturneroffice365-web/jetdb.git
cd jetdb

# Copy environment files
cp backend/.env.example backend/.env
cp frontend/.env.example frontend/.env.local

# Edit .env files with your API keys (see Configuration below)

# Start with Docker
docker-compose up --build
Access the app:

Frontend: http://localhost:3000
Backend API: http://localhost:8000
API Docs: http://localhost:8000/docs

Option 2: Manual Setup
Backend:
bashcd backend
python -m venv venv
source venv/bin/activate  # Windows: venv\Scripts\activate
pip install -r requirements.txt
cp .env.example .env
# Edit .env with your keys
uvicorn main:app --reload
Frontend:
bashcd frontend
npm install
cp .env.example .env.local
# Edit .env.local with your config
npm start

⚙️ Configuration
Backend Setup (backend/.env)
env# Required
SUPABASE_URL=https://your-project.supabase.co
SUPABASE_KEY=your-supabase-anon-key
OPENAI_API_KEY=sk-your-openai-key
AZURE_STORAGE_CONNECTION_STRING=DefaultEndpointsProtocol=https;...

# Optional (for private blob access)
AZURE_SAS_TOKEN=?sv=2021-06-08&ss=bfqt&srt=sco...
Get your keys:

Supabase - Create project at supabase.com → Settings → API
OpenAI - Get API key at platform.openai.com
Azure - Create Storage Account at portal.azure.com

Frontend Setup (frontend/.env.local)
envREACT_APP_SUPABASE_URL=https://your-project.supabase.co
REACT_APP_SUPABASE_KEY=your-supabase-anon-key
REACT_APP_API_BASE=http://localhost:8000
```

---

## 🎯 Usage

### 1. Upload a CSV
Drag and drop any CSV file up to 10GB. JetDB will:
- Stream the upload (no memory issues)
- Auto-convert to Parquet (3-5x smaller)
- Analyze schema and row count
- Index for fast queries

### 2. Explore Your Data
- **Grid View** - Spreadsheet interface with 10,000 row preview
- **Sorting & Filtering** - Click column headers
- **Search** - Find data instantly

### 3. Query with AI
Type natural language questions:
```
"Show top 10 products by revenue"
"Average order value by region"
"Count of customers by signup month"
JetDB generates SQL and executes it for you.
4. Run SQL Queries
For advanced users, write raw SQL:
sqlSELECT 
  region, 
  SUM(revenue) as total_revenue 
FROM data 
WHERE date >= '2024-01-01' 
GROUP BY region 
ORDER BY total_revenue DESC
```

### 5. Merge Datasets
Combine multiple CSVs with matching schemas:
- Validates column compatibility
- Streams merge for low memory usage
- Outputs single Parquet file

---

## 🔒 Security Features

- ✅ **JWT Authentication** - Secure user sessions via Supabase
- ✅ **Rate Limiting** - 10 uploads/hour, 100 API calls/minute per user
- ✅ **SQL Injection Protection** - Whitelist-based query validation
- ✅ **CORS Restrictions** - Frontend-only access
- ✅ **Input Validation** - File type and size checks
- ✅ **Request ID Tracking** - Full audit trail for debugging

---

## 📁 Project Structure
```
jetdb/
├── backend/
│   ├── main.py                    # FastAPI app & endpoints
│   ├── error_handlers.py          # Centralized error handling
│   ├── rate_limiter.py            # Rate limiting config
│   ├── supabase_helpers.py        # Database operations
│   ├── state_endpoints.py         # Spreadsheet state persistence
│   ├── requirements.txt           # Python dependencies
│   ├── Dockerfile                 # Backend container
│   └── .env.example               # Environment template
│
├── frontend/
│   ├── src/
│   │   ├── App.tsx                # Main React component
│   │   ├── components/
│   │   │   ├── DataGrid.tsx       # Spreadsheet view
│   │   │   ├── QueryWorkspace.tsx # SQL/AI query interface
│   │   │   ├── MergeModal.tsx     # Dataset merging UI
│   │   │   └── FileUploader.tsx   # Upload component
│   │   └── App.css                # Styles
│   ├── package.json               # Node dependencies
│   ├── Dockerfile                 # Frontend container
│   └── .env.example               # Environment template
│
├── docker-compose.yml             # Multi-container orchestration
├── .gitignore                     # Git ignore rules
└── README.md                      # This file

🚀 Performance
Dataset SizeUpload TimeQuery TimeStorage Format10 MB2 sec<100 msParquet100 MB8 sec<500 msParquet1 GB45 sec1-3 secParquet10 GB6-8 min5-15 secParquet
Tested on Azure Standard_D2s_v3 (2 vCPU, 8 GB RAM)
Optimizations

Streaming uploads - No memory spikes
Parallel Parquet conversion - 40% faster than sequential
Auto-skip CSV backup - For files >100MB, convert directly to Parquet
Connection pooling - Reuse Azure blob connections
Virtualized scrolling - Only render visible rows in UI


🛣️ Roadmap
v1.1 (Next Release)

 Excel file support (.xlsx, .xls)
 Export to multiple formats (Excel, JSON, Parquet)
 Column-level permissions

v1.2

 Real-time collaboration (multiple users editing)
 Scheduled queries
 Email reports

v2.0

 Self-hosted option with PostgreSQL backend
 API access for programmatic usage
 Mobile app (iOS/Android)


🤝 Contributing
Contributions welcome! Please read our Contributing Guide first.

Fork the repo
Create a feature branch (git checkout -b feature/amazing-feature)
Commit changes (git commit -m 'Add amazing feature')
Push to branch (git push origin feature/amazing-feature)
Open a Pull Request


📝 License
This project is licensed under the MIT License - see LICENSE file for details.

🙏 Acknowledgments
Built with amazing open-source tools:

FastAPI - Modern Python web framework
DuckDB - In-process SQL OLAP database
Handsontable - Spreadsheet component
Supabase - Backend as a Service
OpenAI - Natural language processing
PyArrow - Fast data processing


📧 Support

Issues: GitHub Issues
Documentation: Wiki
Email: support@jetdb.dev


<p align="center">
  <strong>Made with ❤️ for data enthusiasts everywhere</strong><br>
  <sub>JetDB v1.0 • 2025</sub>
</p>


\*\*Made with ❤️ for data enthusiasts everywhere\*\*

