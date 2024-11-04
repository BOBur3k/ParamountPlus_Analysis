# Paramount Plus TV Shows Analysis

![Poster](docs/poster.png)
<p align="right">
  Artwork created by AI
</p>

## Overview

This project aims to **collect and analyze data on TV shows available on Paramount Plus** by utilizing various APIs and Reddit data. The goal is to gain insights into popular shows, genres, actors, directors, and audience sentiments to inform business decisions on content production and marketing strategies.

## Motivation

With the significant growth of streaming platforms and the entertainment industry, understanding audience preferences has become crucial. Analyzing data from streaming services and social media can uncover trends that help stakeholders decide which types of shows to produce or promote.

## Data Sources and APIs

- **TMDb API (The Movie Database):** For detailed TV show information, including metadata and ratings.
- **OMDb API (Open Movie Database):** For additional data such as IMDb ratings and awards.
- **Reddit API (PRAW - Python Reddit API Wrapper):** To fetch Reddit submissions and comments mentioning the TV shows.
- **GPT-4 (Planned):** For advanced sentiment analysis on Reddit comments.

## Data Collection and Processing

### Steps:

1. **API Access:**
   - Obtained API keys and set up configurations for TMDb, OMDb, and Reddit APIs.

2. **Data Collection:**
   - **TV Show Data:** Fetched metadata, ratings, genres, actors, and directors using `collect_tv_shows.py`.
   - **Reddit Data:** Collected submissions and comments mentioning the TV shows using `collect_tv_mentions.py` and `collect_tv_comments.py`.

3. **Data Cleaning:**
   - Performed in `data_cleaning.ipynb`, including handling missing values and normalizing data formats.

### Tools and Packages:

- `pandas` for data manipulation.
- `requests` for API interactions.
- `praw` for Reddit API access.
- `logging` for tracking script execution.

## Next Steps

- **Sentiment Analysis:**
  - Implement sentiment analysis on Reddit comments using GPT-4 to gauge audience sentiment toward different TV shows.

- **Data Analysis:**
  - Perform comprehensive analysis to extract business insights.
  - Identify trends in popular genres, actors, directors, and content types.

- **Visualization:**
  - Create visualizations to represent findings clearly.

---

**Note:** This project is for educational and research purposes. Please ensure compliance with the terms of service of all APIs used and respect user privacy when handling data.
