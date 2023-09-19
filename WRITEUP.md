#### High Level Write-up: Ingestion Project

##### Data Cleaning and Transformation:

- The purpose of the Jupyter Notebook was to discover and explore the tables that existed in the original relational database provided via sqlite.  The sqlite3 and Pandas modules were used to get acccess to the tables, clean them by imputing the Nan with appropriate values, breaking out fields and renaming columns.
- Three helper functions were created to assist in data cleaning.

##### Relational Database:

- Once the tables from the relational database had been fully cleaned and tidied, the decision was made to produce one final analytics ready table and csv file.  This was done due to the low amount of fields (14) and only approx. 5,000 records within the dataset.
- Although the original database was accessed via the sqlite3 standard module, the final production database was created and population utilizing the Sqlalchemy third-party module.
