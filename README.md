# telecom-databricks-pipeline

CDR processing and subscriber churn pipeline built on Azure Databricks + ADLS gen2. covers ingestion through bronze/silver/gold layers.

domain: US telecom operator - call records, billing, support tickets, network outages.

---

## what's in here

```
data/raw/          - sample source datasets (see note below)
notebooks/         - databricks notebooks, bronze -> silver -> gold
schema/            - column definitions and source-to-target mapping
```

source data comes from 5 systems: subscriber CRM, CDR mediation, billing engine, support ticketing, and network management.

---

## data flow

```
ADLS raw/ -> bronze (append) -> silver (cleanse/dedup/merge) -> gold (analytics)
```

bronze: raw load only, schema enforcement, audit columns, autoloader for incremental picks  
silver: type casting, dedup, DQ flags, SCD1 merge (SCD2 for plan changes), anomaly flagging  
gold: monthly snapshots, churn scores, ARPU, network quality rollups

---

## notebooks

| notebook | layer | notes |
|---|---|---|
| bronze_subscribers.py | bronze | autoloader, incremental |
| bronze_cdr.py | bronze | CSV + NDJSON (tickets) |
| silver_subscribers.py | silver | SCD1 merge, SCD2 plan history |
| silver_cdr.py | silver | late arriving, fraud scoring, z-score anomaly |
| silver_billing.py | silver | explodes nested charges array |
| silver_network_events.py | silver | tower outage cleanse |
| gold_subscriber_churn.py | gold | churn score, ARPU, multi-source join |
| gold_network_quality.py | gold | MTTR, outage summary by state |

---

## raw data (sample files)

the `data/raw/` folder has sample versions of each source dataset - same schema and same messy patterns as production but reduced row counts for repo purposes.

| file | format | rows |
|---|---|---|
| subscribers_sample.csv | CSV | ~1,400 |
| call_detail_records_sample.csv | CSV | ~3,000 |
| billing_events_sample.json | JSON | ~500 |
| support_tickets_sample.ndjson | NDJSON | ~400 |
| network_events_sample.csv | CSV | ~600 |

known issues in source data (intentional): duplicate subscriber records with casing differences, ~8% unrated CDRs, late-arriving CDRs where event_date != call date, schema drift fields in billing and tickets.

---

## TODOs

- quarantine table for bad records not wired up yet
- churn_score is rule-based, needs to be swapped with mlflow model output
- storage account name is hardcoded in each notebook, should be cluster env var or config notebook
- billing account_number doesn't always match subscribers - open bug with source team
