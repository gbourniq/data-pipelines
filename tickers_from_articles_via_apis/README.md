# Tickers from Articles

A Python utility that extracts and enriches company information from news articles using Refinitiv's APIs.

## Overview

This project processes news articles to identify mentioned companies and enriches the data with additional market information. It leverages Refinitiv's Intelligent Tagging API for entity recognition and PermID Info API for company details.

## Features

- Extracts company mentions from article text
- Enriches company data with:
  - Organization PermID (Permanent Identifier)
  - Official company names
  - Ticker symbols
  - IPO dates
- Outputs standardized CSV format
- Includes rate limiting for API compliance

## API Integration

### Refinitiv Intelligent Tagging API
Used for identifying company entities within article text. The API analyzes content and returns structured data about mentioned organizations.

### PermID Info API
Provides additional company information using permanent identifiers (PermIDs) obtained from the Intelligent Tagging API.

## Output Format

The utility outputs a list of comma-separated values containing:
```
<PermID>, '<Organization Name>', <Ticker Symbol>, <IPO Date>
```

Example:
```
4295905573, 'APPLE INC.', AAPL, 1980-12-12
4295907168, 'MICROSOFT CORPORATION', MSFT, 1986-03-13
```

## Usage

[Coming soon: Installation and usage instructions]

## Dependencies

- Python 3.x
- Requests library
- [Additional dependencies to be listed]

## License

[Your chosen license]
