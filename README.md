# Real-Time and Batch Climate Sentiment Analysis Pipeline

## Overview
Climate change is a major global issue, and social media platforms contain large volumes of public discussion about climate-related topics. These discussions provide valuable insight into public sentiment, which is important because public opinion can influence policy decisions, awareness, and how communities respond to climate events. However, analysing this data is challenging due to its large scale, unstructured and informal language, and the need for fast processing in real-time settings.

This project develops an end-to-end prototype Big Data system to analyse climate-related sentiment from Reddit using both batch processing and real-time streaming approaches.

## Batch Processing Workflow
In the batch workflow, Reddit posts were collected using the Reddit API and processed with Apache Spark for cleaning, normalisation, and splitting long posts into smaller text segments. Sentiment analysis was performed using two transformer-based models: Twitter-RoBERTa-base-sentiment as the primary model, and FinBERT as an additional experimental comparison.

## Real-Time Streaming Workflow
In the streaming workflow, new Reddit posts were captured at regular intervals and processed using Spark Structured Streaming. The VADER sentiment model was applied to provide fast, near real-time sentiment scores.

## Visualisation
Dashboards were developed to visualise both batch analysis results and live sentiment trends, allowing comparison between historical analysis and real-time behaviour.

## Limitations
This system represents an initial prototype rather than a complete production solution. Its accuracy is limited because the transformer models are not fine-tuned specifically for climate-related text, real-time text cleaning is minimal, and VADER struggles with longer or more complex messages. As a result, the findings should be interpreted as general sentiment patterns rather than precise sentiment measurements.

## Technologies Used
- Python  
- Apache Spark  
- Spark Structured Streaming  
- Transformer-based NLP models  
- VADER sentiment analysis
