# ⚽ EPL Stats Bot

> A conversational WhatsApp bot for English Premier League statistics, powered by a robust data engineering pipeline.

[![Python](https://img.shields.io/badge/python-3.10+-blue.svg)](https://www.python.org/downloads/)
[![Airflow](https://img.shields.io/badge/airflow-2.7.3-red.svg)](https://airflow.apache.org/)
[![FastAPI](https://img.shields.io/badge/fastapi-0.104+-green.svg)](https://fastapi.tiangolo.com/)
[![Twilio](https://img.shields.io/badge/twilio-whatsapp-red.svg)](https://www.twilio.com/whatsapp)

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
- [Bot Commands](#bot-commands)
- [Development](#development)

---

## 🎯 Overview

The **EPL Stats Bot** is an end-to-end data solution that brings real-time football statistics directly to your WhatsApp. It features a fully automated data pipeline that ingests data from external APIs, processes it for analytics, and serves it through a conversational interface.

**Use Case:** Instantly settle football debates in WhatsApp groups with accurate, up-to-date EPL statistics!

---

## 🏗️ Architecture

The project follows a modular architecture designed for reliability and scalability:

```mermaid
graph LR
    A[Football API] --> B[Airflow DAGs]
    B --> C[(PostgreSQL)]
    C --> D[FastAPI App]
    D --> E[Twilio Webhook]
    E --> F[WhatsApp User]
```

1.  **Ingestion & Orchestration:** Apache Airflow schedules daily and match-day tasks to fetch data from API-Football.
2.  **Storage:** Processed data is stored in a structured PostgreSQL 15 database.
3.  **API Layer:** A FastAPI server handles business logic, fuzzy searching, and stats aggregation.
4.  **Bot Interface:** Twilio WhatsApp API serves as the bridge between the user and the backend.

---

## ✨ Features

- 🔄 **Automated Pipeline** - Scheduled ingestion of leagues, seasons, fixtures, and player stats.
- 🤖 **Smart Search** - Uses fuzzy matching (`thefuzz`) to find players and teams even with typos.
- 📊 **Rich Stats** - Detailed player profiles, team standings, and recent match results.
- 🤺 **Head-to-Head** - Compare two teams directly via WhatsApp.
- 🐳 **Dockerized** - Entire stack (Bot, Airflow, Postgres) runs seamlessly in Docker.

---

## 🛠️ Tech Stack

| Layer | Technology |
| :--- | :--- |
| **Orchestration** | Apache Airflow 2.7.3 |
| **Database** | PostgreSQL 15 |
| **API Framework** | FastAPI |
| **Bot Integration** | Twilio WhatsApp SDK |
| **Data Processing** | Python (Pandas) |
| **Fuzzy Matching** | thefuzz |
| **Containerization** | Docker & Docker Compose |

---

## ✅ Prerequisites

- **Docker** and **Docker Compose**
- **Python 3.10+** (for local development)
- **Twilio Account** (Account SID, Auth Token, and WhatsApp Number)
- **API-Football Key** (from [RapidAPI](https://rapidapi.com/api-sports/api/api-football))

---

## 🚀 Installation

### 1. Clone & Setup
```bash
git clone https://github.com/AboladeOluwaseun/epl_stats_bot.git
cd epl_stats_bot
cp .env.example .env
```

### 2. Configure Environment
Edit the `.env` file with your credentials:
- `FOOTBALL_API_KEY`: Your API-Football key.
- `TWILIO_ACCOUNT_SID`: From your Twilio console.
- `TWILIO_AUTH_TOKEN`: From your Twilio console.
- `TWILIO_WHATSAPP_NUMBER`: Your Twilio sandbox or production number.

### 3. Start Services
```bash
docker-compose up -d
```

---

## 📖 Usage

### Accessing Services
- **Airflow UI:** `http://localhost:8080` (Default: `Admin/Admin`)
- **Bot Webhook:** `http://localhost:5000/webhook` (Needs to be exposed via ngrok for Twilio)

### Triggering the Pipeline
The pipeline runs automatically via Airflow, but you can trigger it manually:
```bash
docker exec epl_airflow_scheduler airflow dags trigger epl_daily_ingestion
```

---

## 🤖 Bot Commands

Send these messages to the bot on WhatsApp:

| Command | Example | Result |
| :--- | :--- | :--- |
| **Help/Start** | `help` | Shows available commands. |
| **Player Stats** | `Haaland` | Returns latest stats & photo. |
| **Team Results** | `Arsenal` | Returns the last 5 match results. |
| **Standings** | `table` | Returns the current EPL standings. |
| **Head-to-Head** | `Arsenal vs Chelsea` | Compares recent meetings. |

---

## 📁 Project Structure

```
FOOTBALL STATS BOT/
├── airflow/
│   └── dags/              # Airflow DAG definitions
├── src/
│   ├── bot/               # FastAPI & WhatsApp logic
│   ├── ingestion/         # API clients
│   ├── processing/        # Data transformation logic
│   ├── storage/           # Database handlers
│   └── utils/             # Configs and logging
├── sql/
│   └── ddl/               # Database schema
├── docker-compose.yaml    # Infrastructure orchestration
├── Dockerfile            # Bot container definition
└── requirements.txt       # Python dependencies
```

---

## 👨‍💻 Development

### Local Setup
```bash
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
python src/bot/app.py
```

### Exposing Local Bot
To test on WhatsApp, use ngrok to expose port 5000:
```bash
ngrok http 5000
```
Then update your Twilio Sandbox Webhook URL to `https://<your-ngrok-url>/webhook`.