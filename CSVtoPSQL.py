#Created by Leon Kohlhaussen on 22.02.2022
#Version 1.2
#----------------------------------------------------------------
#A script to import a CSV file into a PostgreSQL database table using Python 

import pandas as pd
import psycopg2
from psycopg2.extras import RealDictCursor
import time
import os


timestamp = time.strftime('%d.%m.%y.%H-%M-%S')


# Create the log directory if it doesn't exist
log_dir = './log'
if not os.path.exists(log_dir):
    os.makedirs(log_dir)

# Read the data from the CSV file
data = pd.read_csv('src\zuordnung_plz_ort.csv', delimiter=',')

# Get the column names and data types from the data
col_names = data.columns
col_types = data.dtypes.astype(str).str.replace('object', 'text').str.replace('int', 'integer').str.replace('float', 'numeric').str.replace('bool', 'boolean').str.replace('datetime', 'timestamp with time zone').str.replace('numeric64', 'numeric').str.replace('integer64', 'integer')

# Create a connection to the database
conn = psycopg2.connect(
    host="localhost",
    database="gewasser",
    user="postgres",
    password="POSTGRES"
)
# cur = conn.cursor()
# Create a cursor object to execute SQL queries
cur = conn.cursor(cursor_factory=RealDictCursor)


# Check if the table exists and ask user if they want to drop it
table_name = 'plz_ort'
cur.execute("SELECT EXISTS(SELECT * FROM information_schema.tables WHERE table_name=%s)", (table_name,))
table_exists = cur.fetchone()
print(table_exists)
if table_exists and table_exists['exists']:
    drop_table = input(f"The table {table_name} already exists. Do you want to drop it and create a new one? (y/n): ")
    if drop_table.lower() == 'y':
        cur.execute(f"DROP TABLE {table_name}")
    else:
        print("Aborting...")
        cur.close()
        conn.close()
        exit()

start_time = time.perf_counter()




# Create the SQL statement to create the table
columns = ', '.join([f"{name} {col_types[i]}" for i, name in enumerate(col_names)])
sql_statement = f"CREATE TABLE {table_name} ({columns})"

# Write the SQL statement to a text file with the current timestamp
with open(f"{log_dir}/{timestamp}create_table.sql", "w") as f:
    f.write(sql_statement)

# Execute the SQL statement to create the table
cur.execute(sql_statement)

# Loop through each row of the data and insert it into the database
for index, row in data.iterrows():
    query = f"INSERT INTO {table_name} ({', '.join(col_names)}) VALUES ({', '.join(['%s' for i in range(len(col_names))])})"
    cur.execute(query, tuple(row))

# Commit the changes and close the cursor and connection
conn.commit()
cur.close()
conn.close()

elapsed_time = time.perf_counter() - start_time

# Write the INSERT statement to a text file
with open(f"{log_dir}/{timestamp}insert_data.sql", "w") as f:
    for index, row in data.iterrows():
        values = ','.join(["'" + str(value).replace("'", "''") + "'" for value in row])
        query = f"INSERT INTO {table_name} VALUES ({values})"
        f.write(query + '\n')

print(f"Script completed successfully in {elapsed_time:.2f} seconds.")