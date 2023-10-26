# company-identifier-enrichment

### Data & Process Workflow
![company_identifier_enrichment_diagram_MR](/images/company_identifier_enrichment_diagram_MR.jpg)

### Project Description
An automated and scheduled process designed for the collection and mapping of diverse publicly available company identifiers, further enriched by company metadata retrieved from public APIs.
The process is entirely cloud based (various Google Cloud Services) and fully automated with weekly updates (end-to-end).

### Challenge
Accessing company information from dispersed data sources can be challenging. My proposed process aims to tackle this challenge by creating and maintaining an updated database of company ticker codes and related company metadata for prominent data providers based on **open-source** APIs:
- Ticker and CIK codes are directly sourced from the US SEC (more than 12,000 company tickers)
- PermID's, company names and URLs to full company profiles are sourced from Refinitiv's PermID API

### Data Sources
- Edgar SEC
- Refintiv PermID API

### Tech
- Python
- Google Cloud Functions
- Google Cloud BigQuery
- Google Cloud Run
- Google Cloud Scheduler & Pub/Sub
- Google Cloud Storage

### BigQuery Database
![bigquery_db_output](/images/bigquery_output.png)
