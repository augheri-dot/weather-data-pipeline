Multi-City Weather Data Pipeline & Dashboard

An end-to-end data engineering project that demonstrates how to build a scalable data pipeline from API ingestion to interactive visualization using modern data tools.

![Python](https://img.shields.io/badge/Python-3.12-blue)
![Streamlit](https://img.shields.io/badge/Streamlit-Dashboard-red)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-Database-blue)
![License](https://img.shields.io/badge/license-MIT-green)

---

## Purpose
This project was built to demonstrate:
- End-to-end data engineering workflow
- Integration between API, database, and dashboard
- Ability to design scalable data pipelines

---

## Overview
This project demonstrates a complete data workflow:
- Extract – Fetch weather data from an external API
- Transform – Process and structure the data
- Load – Store data into a PostgreSQL database (Supabase)
- Visualize – Build an interactive dashboard using Streamlit

The system supports multiple cities and enables dynamic exploration of weather trends.

---

## Key Highlights

- Built a multi-city ETL pipeline using Python
- Implemented batch insert for efficient database loading
- Designed idempotent data pipeline (no duplicates)
- Developed interactive dashboard with filtering & visualization

---

## Data Source
Weather data is retrieved from:
- Open-Meteo API (https://open-meteo.com/)
  The API provides hourly weather data including temperature and humidity.

---

## Tech Stack
- Python – ETL pipeline & data processing
- PostgreSQL (Supabase) – Data storage
- Streamlit – Interactive dashboard
- Plotly – Data visualization
- GitHub – Version control

---

## Features
### ETL Pipeline (weather_etl.py)
- Extract – Fetch weather data from an external API
- Transform – Process and structure the data
- Load – Store data into PostgreSQL
- Visualize – Build dashboard using Streamlit

---

### Dashboard (dashboard_weather.py)
- Sidebar filters (city & date range)
- Key metrics (average, max, min)
- Multi-city comparison charts
- Time-series trends (temperature & humidity)
- Summary statistics
- Downloadable dataset (CSV)

---

## Supported Cities
- Jakarta
- Bandung
- Surabaya
- Medan
- Yogyakarta
- Denpasar

---

## Architecture
API → Python ETL → PostgreSQL (Supabase) → Streamlit Dashboard

---

## Setup Instructions
1. Clone Repository
```bash
git clone https://github.com/your-username/your-repo-name.git
cd your-repo-name
```
2. Create Virtual Environment (optional but recommended)
```bash
python -m venv venv
venv\Scripts\activate   # Windows
pip install -r requirements.txt
```

---

## Environment Variables
### Create a .env file:
```env
  DB_HOST=your_host
  DB_NAME=your_db
  DB_USER=your_user
  DB_PASSWORD=your_password
  DB_PORT=your_port
```

  Do not upload .env to GitHub.

---

### Running the ETL Pipeline
```
python weather_etl.py
```

This will:

  Fetch weather data
  Insert into PostgreSQL database

---

### ETL Behavior
- Runs per city sequentially
- Uses batch insert for performance
- Logs progress (rows inserted, skipped)
- Designed to be scheduled (daily/hourly)

---

### Running the Dashboard
```bash
streamlit run dashboard_weather.py
```

Open in browser:
```
http://localhost:8501
```

---

## Deployment
The dashboard can be deployed using Streamlit Cloud:
- Upload project to GitHub
- Connect repository in Streamlit Cloud
- Set environment variables (DB credentials)
- Deploy dashboard_weather.py

---

## Data Updates
The dashboard always reflects the latest data available in the database.

---

### To keep data updated automatically:
Schedule ETL using:
- GitHub Actions
- Cron jobs
- Cloud scheduler

---

## Project Structure
```txt
├── weather_etl.py          # ETL pipeline
├── dashboard_weather.py    # Streamlit dashboard
├── requirements.txt        # Dependencies
├── README.md               # Project documentation
└── .env                    # Environment variables (not tracked)
```

---

## Future Improvements
- Add real-time API streaming
- Implement automated scheduling (daily ETL)
- Add anomaly detection
- Improve UI with tabs & advanced visualizations
- Add API layer for external access

---

## Dashboard Preview
![Dashboard](./screenshot.png)

---

## Live Demo
https://your-app.streamlit.app

