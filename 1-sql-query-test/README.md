# Overview
This project is a SQL query test of employees database. I use `PostgreSQL` running on `Docker` as a RDBMS to store provided raw source data from excel file.

# Result
For the final output, please refer to the Excel file `./final.xlsx`, where each query and its corresponding result are organized by sheet name.

If you'd like to run it yourself, simply execute:
```bash
docker compose up -d
```
This command will automatically initialize the table creation using the SQL files located in the `./initdb` directory.

After the containers are up, you can manually run the queries from `./query.sql` using your preferred database administration tool, such as DBeaver, to view the corresponding results for each query.