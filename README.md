# Cryptocurrency Market Analysis and Trading Insights Platform

## Overview

This project is designed to collect, analyze, and visualize real-time and historical cryptocurrency data using the CoinPaprika API. The platform integrates data from various sources, and provides actionable insights to help monitor market trends.

# Current Features

- **Real-Time Data Collection:** Fetches cryptocurrency prices and market data from the CoinPaprika API periodically.

## Planned Features

- **Sentiment Analysis:** Analyzes sentiment from news articles and social media to gauge market mood.
- **Predictive Insights:** Utilizes historical data and machine learning to forecast price trends and trading signals.
- **Interactive Visualizations:** Provides dashboards for real-time and historical data, technical indicators, and sentiment analysis.
- **AI-Driven Queries:** Integrate LLMs for advanced querying of cryptocurrency data.

## Architecture

- **Data Collection:** Retrieves data from the CoinPaprika API for real-time and historical price information.
- **Data Processing:** Cleanses, engineers features, and aggregates data.
- **Data Storage:** Utilizes Apache Cassandra for time-series data and PostgreSQL for structured data.
- **Data Analysis:** Employs Apache Spark for analytics and machine learning.
- **Streaming:** Kafka handles real-time price updates and streaming data.
- **Deployment:** Docker is used to containerize services for easy deployment and scaling.
