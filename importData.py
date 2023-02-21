import pandas as pd
import psycopg2
from psycopg2.extras import RealDictCursor

# Read the data from the CSV file
data = pd.read_csv('/home/leon/Documents/GitHub/Database/dataseen.csv', delimiter=',', dtype=str)

# Get the column names and data types from the data
col_names = data.columns
col_types = data.dtypes.astype(str).str.replace('object', 'text').str.replace('int', 'integer').str.replace('float', 'numeric').str.replace('datetime', 'timestamp with time zone')

# Create a connection to the database
conn = psycopg2.connect(
    host="localhost",
    database="badeseen",
    user="leon",
    password="leon"
)

# Create a cursor object to execute SQL queries
# cur = conn.cursor()
cur = conn.cursor(cursor_factory=RealDictCursor)

# Create the SQL statement to create the table
table_name = 'alle_badeseen2'
columns = ', '.join([f"{name} {col_types[i]}" for i, name in enumerate(col_names)])
sql_statement = f"CREATE TABLE {table_name} ({columns})"

# Execute the SQL statement to create the table
cur.execute(sql_statement)

# Loop through each row of the data and insert it into the database

#for index, row in data.iterrows():
#    values = ','.join(["'" + str(value).replace("'", "''") + "'" for value in row])
#    query = f"INSERT INTO {table_name} VALUES ({values})"
#    cur.execute(query)

for index, row in data.iterrows():
    query = f"INSERT INTO {table_name} ({', '.join(col_names)}) VALUES ({', '.join(['%s' for i in range(len(col_names))])})"
    cur.execute(query, tuple(row))

# Commit the changes and close the cursor and connection
conn.commit()
cur.close()
conn.close()

