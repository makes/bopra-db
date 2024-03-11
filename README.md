# bopra-db

ETL process to build research database for the [BOPRA](https://clinicaltrials.gov/study/NCT04144803) study. This repository does not contain any data, just the process to create the database from raw data.

### Simplified workflow

1. Read source csv files.
2. Apply proper data types.
3. Remove irrelevant and sensitive fields.
4. Convert all timestamps to nanosecond intervals relative to the alert time of the physician-staffed EMS unit.
5. Remove periods marked as artifacts in manual validation of physiological signals.
6. Write the database on disk in SQLite and Parquet formats.

Please refer to the source code for more detailed insight.
