import pandas as pd
import psycopg2
from psycopg2.extras import RealDictCursor

# Read the data from the CSV file
data = pd.read_csv('dataseen.csv', delimiter=',')

data = data.infer_objects()

# Get the column names and data types from the data
col_names = data.columns
col_types = data.dtypes.astype(str).str.replace('object', 'text').str.replace('int', 'text').str.replace('float', 'numeric').str.replace('datetime', 'timestamp with time zone').str.replace('numeric64', 'numeric')


# Create a connection to the database
conn = psycopg2.connect(
    host="localhost",
    database="gewasser",
    user="postgres",
    password="POSTGRES"
)

# Create a cursor object to execute SQL queries
# cur = conn.cursor()
cur = conn.cursor(cursor_factory=RealDictCursor)

# Create the SQL statement to create the table
table_name = 'alle_badeseen1'
columns = ', '.join([f"{name} {col_types[i]}" for i, name in enumerate(col_names)])
sql_statement = f"CREATE TABLE {table_name} ({columns})"



# Drop the table if it already exists
def drop_table():
    cur.execute(f"DROP TABLE IF EXISTS {table_name}")


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
