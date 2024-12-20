# Text File Ingestion System

A data processing system designed to analyze academic paper metadata related to COVID-19 research funding patterns across different countries.

## Project Overview

This system processes academic paper metadata to track research activity distribution across different countries, serving as a proxy for understanding research funding allocation patterns during the COVID-19 pandemic.

## Core Functionality

The system analyzes two key data points:
- Paper titles for country name mentions
- Author institutional affiliations through email domain analysis

## Technical Implementation

- Built using Python with SQLite for data persistence
- Implements an in-memory database for efficient processing
- Features a modular design for easy extension to other metadata analysis tasks

## Data Processing Pipeline

1. Ingests paper metadata from structured text files
2. Extracts and normalizes country information
3. Stores processed data in an optimized database schema
4. Provides query interfaces for analysis

## Database Design

The system uses a carefully designed schema optimized for:
- Fast querying of country-specific research patterns
- Efficient storage of paper metadata
- Flexible relationship mapping between papers and countries
