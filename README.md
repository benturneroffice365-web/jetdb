\# JetDB v7.2



\*\*Big data for the rest of us\*\*



\[!\[License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

\[!\[Python](https://img.shields.io/badge/Python-3.11+-blue.svg)](https://www.python.org/downloads/)

\[!\[FastAPI](https://img.shields.io/badge/FastAPI-0.104+-00a393.svg)](https://fastapi.tiangolo.com/)

\[!\[React](https://img.shields.io/badge/React-18+-61dafb.svg)](https://reactjs.org/)

\[!\[TypeScript](https://img.shields.io/badge/TypeScript-5+-3178c6.svg)](https://www.typescriptlang.org/)



---



\## 📋 Overview



JetDB is a powerful full-stack application designed to handle massive datasets with the simplicity of a spreadsheet interface. Built for data analysts, scientists, and engineers who need Excel-like functionality for datasets that would make traditional spreadsheets crash.



\### ✨ Key Features



\- \*\*🚀 Lightning Fast\*\* - Powered by DuckDB for blazing-fast analytical queries

\- \*\*📊 Spreadsheet Interface\*\* - Familiar Excel-like UI using Luckysheet

\- \*\*🤖 AI-Powered\*\* - Natural language queries powered by GPT-4o-mini

\- \*\*☁️ Cloud Ready\*\* - Seamless integration with Azure Storage

\- \*\*🔐 Secure\*\* - Authentication via Supabase

\- \*\*📈 Scalable\*\* - Handle millions of rows without breaking a sweat



---



\## 🛠️ Tech Stack



\### Backend

\- \*\*FastAPI\*\* - Modern Python web framework

\- \*\*DuckDB\*\* - High-performance analytical database

\- \*\*OpenAI GPT-4o-mini\*\* - AI-powered natural language processing

\- \*\*Azure Storage\*\* - Cloud storage integration



\### Frontend

\- \*\*React 18\*\* - Modern UI library

\- \*\*TypeScript\*\* - Type-safe JavaScript

\- \*\*Luckysheet\*\* - Excel-like spreadsheet component

\- \*\*Supabase\*\* - Authentication and real-time data



\### DevOps

\- \*\*Docker\*\* - Containerization

\- \*\*Docker Compose\*\* - Multi-container orchestration



---



\## 📦 Installation



\### Prerequisites



\- Python 3.11+

\- Node.js 18+

\- Docker \& Docker Compose (optional)

\- Git



\### Clone the Repository



```bash

git clone https://github.com/benturneroffice365-web/jetdbV7.git

cd jetdbV7

```



---



\## ⚙️ Configuration



\### Backend Setup



1\. Navigate to the backend directory:

```bash

cd backend

```



2\. Create a virtual environment:

```bash

python -m venv venv

source venv/bin/activate  # On Windows: venv\\Scripts\\activate

```



3\. Install dependencies:

```bash

pip install -r requirements.txt

```



4\. Create `.env` file from template:

```bash

cp .env.example .env

```



5\. Configure your `.env` file:

```env

OPENAI\_API\_KEY=your\_openai\_api\_key\_here

AZURE\_STORAGE\_CONNECTION\_STRING=your\_azure\_connection\_string

SUPABASE\_URL=your\_supabase\_url

SUPABASE\_KEY=your\_supabase\_key

```



\### Frontend Setup



1\. Navigate to the frontend directory:

```bash

cd frontend

```



2\. Install dependencies:

```bash

npm install

```



3\. Create `.env.local` file from template:

```bash

cp .env.example .env.local

```



4\. Configure your `.env.local` file:

```env

REACT\_APP\_SUPABASE\_URL=your\_supabase\_url

REACT\_APP\_SUPABASE\_ANON\_KEY=your\_supabase\_anon\_key

REACT\_APP\_API\_URL=http://localhost:8000

```



---



\## 🚀 Running the Application



\### Option 1: Using Docker Compose (Recommended)



```bash

docker-compose up --build

```



\- \*\*Backend API\*\*: http://localhost:8000

\- \*\*Frontend\*\*: http://localhost:3000

\- \*\*API Docs\*\*: http://localhost:8000/docs



\### Option 2: Manual Setup



\*\*Backend:\*\*

```bash

cd backend

uvicorn main:app --reload

```



\*\*Frontend:\*\*

```bash

cd frontend

npm start

```



---



\## 📁 Project Structure



```

jetdbV7/

├── backend/

│   ├── main.py              # FastAPI application

│   ├── requirements.txt     # Python dependencies

│   ├── Dockerfile          # Backend container

│   ├── .env.example        # Environment template

│   └── .gitignore

│

├── frontend/

│   ├── public/

│   │   ├── index.html

│   │   └── manifest.json

│   ├── src/

│   │   ├── App.tsx         # Main React component

│   │   ├── App.css         # Styles

│   │   ├── index.tsx       # Entry point

│   │   └── ...

│   ├── package.json        # Node dependencies

│   ├── tsconfig.json       # TypeScript config

│   ├── .env.example        # Environment template

│   └── .gitignore

│

├── docker-compose.yml      # Container orchestration

├── .gitignore             # Git ignore rules

└── README.md              # This file

```



---



\## 🎯 Usage



1\. \*\*Import Data\*\* - Upload CSV, Excel, or connect to databases

2\. \*\*Query with AI\*\* - Ask questions in plain English

3\. \*\*Visualize\*\* - Create charts and pivot tables

4\. \*\*Export\*\* - Save results in multiple formats

5\. \*\*Collaborate\*\* - Share workbooks with your team



---



\## 🔒 Security



\- \*\*Never commit `.env` files\*\* - Use `.env.example` as templates

\- \*\*API keys are gitignored\*\* - Your secrets are safe

\- \*\*Supabase authentication\*\* - Secure user management

\- \*\*CORS configured\*\* - Proper security headers



---



\## 🤝 Contributing



Contributions are welcome! Please feel free to submit a Pull Request.



1\. Fork the repository

2\. Create your feature branch (`git checkout -b feature/AmazingFeature`)

3\. Commit your changes (`git commit -m 'Add some AmazingFeature'`)

4\. Push to the branch (`git push origin feature/AmazingFeature`)

5\. Open a Pull Request



---



\## 📝 License



This project is licensed under the MIT License - see the \[LICENSE](LICENSE) file for details.



---



\## 🙏 Acknowledgments



\- Built with \[FastAPI](https://fastapi.tiangolo.com/)

\- Powered by \[DuckDB](https://duckdb.org/)

\- UI components from \[Luckysheet](https://mengshukeji.gitee.io/LuckysheetDocs/)

\- Authentication via \[Supabase](https://supabase.com/)

\- AI by \[OpenAI](https://openai.com/)



---



\## 📧 Contact



\*\*Project Link\*\*: https://github.com/benturneroffice365-web/jetdbV7



---



\## 🗺️ Roadmap



\- \[ ] Real-time collaboration

\- \[ ] Custom plugin system

\- \[ ] Advanced data visualization

\- \[ ] Mobile app

\- \[ ] Self-hosted deployment guides

\- \[ ] API documentation improvements



---



\*\*Made with ❤️ for data enthusiasts everywhere\*\*

