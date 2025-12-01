# ⚽ EPL Stats Pipeline

> End-to-end data engineering pipeline for English Premier League statistics with conversational bot interface

[![Python](https://img.shields.io/badge/python-3.10+-blue.svg)](https://www.python.org/downloads/)
[![Airflow](https://img.shields.io/badge/airflow-2.7.3-red.svg)](https://airflow.apache.org/)
[![FastAPI](https://img.shields.io/badge/fastapi-0.104+-green.svg)](https://fastapi.tiangolo.com/)
[![License](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

## 📋 Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Features](#features)
- [Tech Stack](#tech-stack)
- [Prerequisites](#prerequisites)
- [Installation](#installation)
- [Configuration](#configuration)
- [Usage](#usage)
- [Project Structure](#project-structure)
- [API Documentation](#api-documentation)
- [Bot Commands](#bot-commands)
- [Development](#development)
- [Testing](#testing)
- [Deployment](#deployment)
- [Contributing](#contributing)
- [License](#license)

---

## 🎯 Overview

The EPL Stats Pipeline is a comprehensive data engineering solution that:

1. **Ingests** EPL data from external APIs (matches, players, standings)
2. **Processes** raw data using Apache Spark and stores in Delta Lake
3. **Warehouses** analytics-ready data in PostgreSQL
4. **Serves** statistics through a FastAPI REST API
5. **Provides** user-friendly access via Telegram bot

**Use Case:** Settle football debates in WhatsApp groups with real-time statistics!

---

## 🏗️ Architecture

```
Football API → Airflow → S3 (Raw) → Spark → Delta Lake → PostgreSQL → FastAPI → Telegram Bot
```

### Data Flow:
1. **Ingestion:** Airflow DAGs fetch data from Football API daily/hourly
2. **Storage:** Raw JSON stored in S3 with date partitioning
3. **Processing:** Spark jobs transform data and write to Delta Lake
4. **Warehouse:** Processed data loaded into PostgreSQL (star schema)
5. **API:** FastAPI serves pre-aggregated statistics with Redis caching
6. **Interface:** Telegram bot provides conversational access

---

## ✨ Features

- 🔄 **Automated Data Pipeline** - Daily updates, hourly on match days
- 📊 **Rich Statistics** - Matches, players, teams, standings
- ⚡ **Fast Queries** - Materialized views and Redis caching
- 🤖 **Conversational Interface** - Natural language bot commands
- 📈 **Scalable Architecture** - Handles multiple seasons/leagues
- 🧪 **Data Quality** - Automated validation and testing
- 📱 **Real-time Updates** - Near real-time match statistics
- 🔐 **Secure** - Environment-based configuration

---

## 🛠️ Tech Stack

| Layer | Technology |
|-------|-----------|
| **Orchestration** | Apache Airflow 2.7+ |
| **Processing** | Apache Spark (PySpark) |
| **Storage Format** | Delta Lake |
| **Data Warehouse** | PostgreSQL 15 |
| **API Framework** | FastAPI |
| **Caching** | Redis |
| **Bot Framework** | python-telegram-bot |
| **Containerization** | Docker & Docker Compose |
| **Cloud** | AWS (S3, RDS, EC2/ECS) |

---

## ✅ Prerequisites

- **Docker** 20.10+ and Docker Compose 2.0+
- **Python** 3.10+
- **AWS Account** (for S3 storage)
- **Football API Key** from [RapidAPI](https://rapidapi.com/api-sports/api/api-football)
- **Telegram Bot Token** from [@BotFather](https://t.me/BotFather)

---

## 🚀 Installation

### 1. Clone Repository
\`\`\`bash
git clone https://github.com/yourusername/epl-stats-pipeline.git
cd epl-stats-pipeline
\`\`\`

### 2. Create Environment File
\`\`\`bash
cp .env.example .env
# Edit .env and add your API keys and credentials
\`\`\`

### 3. Generate Airflow Fernet Key
\`\`\`bash
python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
# Add output to AIRFLOW__CORE__FERNET_KEY in .env
\`\`\`

### 4. Start Services
\`\`\`bash
docker-compose up -d
\`\`\`

### 5. Access Services
- **Airflow UI:** http://localhost:8080 (admin/admin)
- **FastAPI Docs:** http://localhost:8000/docs
- **PostgreSQL:** localhost:5432

---

## ⚙️ Configuration

### Environment Variables

Key variables to configure in `.env`:

\`\`\`bash
# Required
FOOTBALL_API_KEY=your_rapidapi_key
TELEGRAM_BOT_TOKEN=your_bot_token
AWS_ACCESS_KEY_ID=your_aws_key
AWS_SECRET_ACCESS_KEY=your_aws_secret

# Database
POSTGRES_PASSWORD=secure_password

# Airflow
AIRFLOW__CORE__FERNET_KEY=your_fernet_key
\`\`\`

See `.env.example` for complete configuration options.

---

## 📖 Usage

### Starting the Pipeline

\`\`\`bash
# Start all services
docker-compose up -d

# Check service status
docker-compose ps

# View logs
docker-compose logs -f airflow-scheduler
\`\`\`

### Triggering DAGs

\`\`\`bash
# Via Airflow UI (http://localhost:8080)
# Or via CLI:
docker exec epl_airflow_scheduler airflow dags trigger epl_daily_ingestion
\`\`\`

### Using the Bot

1. Open Telegram and search for your bot
2. Start conversation: `/start`
3. Try commands:
   - `/topscorers` - Top goal scorers
   - `/standings` - League table
   - `/h2h Arsenal Chelsea` - Head-to-head stats

---

## 📁 Project Structure

\`\`\`
epl-stats-pipeline/
├── airflow/
│   ├── dags/              # Airflow DAG definitions
│   ├── plugins/           # Custom operators
│   └── logs/              # Execution logs
├── src/
│   ├── ingestion/         # API clients and data fetchers
│   ├── processing/        # Spark jobs
│   ├── storage/           # Database handlers
│   ├── api/               # FastAPI application
│   ├── bot/               # Telegram bot
│   └── utils/             # Shared utilities
├── sql/
│   ├── ddl/               # Table definitions
│   └── dml/               # Seed data
├── tests/
│   ├── unit/              # Unit tests
│   └── integration/       # Integration tests
├── config/                # Configuration files
├── docs/                  # Documentation
├── docker-compose.yaml    # Docker services
├── Dockerfile            # Container definition
├── requirements.txt       # Python dependencies
└── README.md             # This file
\`\`\`

---

## 📚 API Documentation

Once running, visit: **http://localhost:8000/docs**

### Sample Endpoints

\`\`\`
GET /api/v1/top-scorers?season=2024-25&limit=10
GET /api/v1/standings?season=2024-25
GET /api/v1/head-to-head?team1=Arsenal&team2=Chelsea
GET /api/v1/player-stats/276?season=2024-25
GET /api/v1/team-form/42
\`\`\`

---

## 🤖 Bot Commands

| Command | Description | Example |
|---------|-------------|---------|
| `/start` | Welcome message | `/start` |
| `/topscorers` | Top 10 goal scorers | `/topscorers` |
| `/standings` | League table | `/standings` |
| `/h2h <team1> <team2>` | Head-to-head | `/h2h Arsenal Chelsea` |
| `/form <team>` | Last 5 matches | `/form Liverpool` |
| `/stats <player>` | Player stats | `/stats Haaland` |
| `/upcoming` | Next fixtures | `/upcoming` |
| `/help` | Show all commands | `/help` |

---

## 👨‍💻 Development

### Local Setup (without Docker)

\`\`\`bash
# Create virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\\Scripts\\activate

# Install dependencies
pip install -r requirements.txt

# Run tests
pytest tests/

# Run API locally
uvicorn src.api.main:app --reload

# Run bot locally
python src/bot/telegram_bot.py
\`\`\`

### Code Quality

\`\`\`bash
# Format code
black src/

# Sort imports
isort src/

# Lint
flake8 src/
pylint src/

# Type checking
mypy src/
\`\`\`

---

## 🧪 Testing

\`\`\`bash
# Run all tests
pytest tests/ -v

# With coverage
pytest tests/ --cov=src --cov-report=html

# Run specific test file
pytest tests/unit/test_ingestion.py

# Run integration tests
pytest tests/integration/ -v
\`\`\`

---

## 🚢 Deployment

### AWS Deployment

1. **Provision Infrastructure:**
   \`\`\`bash
   cd infrastructure/terraform
   terraform init
   terraform plan
   terraform apply
   \`\`\`

2. **Deploy Services:**
   - Push Docker images to ECR
   - Deploy to ECS/EC2
   - Set up RDS and S3

3. **Configure Monitoring:**
   - CloudWatch logs and metrics
   - Airflow alerting

See `docs/deployment.md` for detailed instructions.

---

## 🤝 Contributing

Contributions welcome! Please:

1. Fork the repository
2. Create feature branch (`git checkout -b feature/AmazingFeature`)
3. Commit changes (`git commit -m 'Add AmazingFeature'`)
4. Push to branch (`git push origin feature/AmazingFeature`)
5. Open Pull Request

---

## 📄 License

This project is licensed under the MIT License - see [LICENSE](LICENSE) file.

---

## 👤 Author

**Your Name**
- GitHub: [@yourusername](https://github.com/yourusername)
- LinkedIn: [Your Name](https://linkedin.com/in/yourname)

---

## 🙏 Acknowledgments

- [API-Football](https://www.api-football.com/) for EPL data
- [Apache Airflow](https://airflow.apache.org/) community
- [FastAPI](https://fastapi.tiangolo.com/) framework

---

## 📮 Support

For issues and questions:
- 🐛 **Bug Reports:** [GitHub Issues](https://github.com/yourusername/epl-stats-pipeline/issues)
- 💬 **Discussions:** [GitHub Discussions](https://github.com/yourusername/epl-stats-pipeline/discussions)
- 📧 **Email:** your.email@example.com

---

**⭐ If this project helped you, please star it on GitHub!**