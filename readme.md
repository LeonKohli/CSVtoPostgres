# CSV to PostgreSQL Database Importer

This is a Python script that reads data from a CSV file and imports it into a PostgreSQL database.

## Getting Started

### Prerequisites

- Python 3.x
- pandas
- psycopg2

### Installing

1. Clone the repository to your local machine.
2. Install the required Python packages: `pip install pandas psycopg2`

### Usage

1. Modify the `importData.py` script to match your specific database and CSV file.
2. Open a terminal and navigate to the directory where the script is located.
3. Run the script: `python importData.py`

### Configuration

- The `read_csv()` function in the script reads the CSV file and can be modified to use a different delimiter or data types.
- The `psycopg2.connect()` function in the script creates a connection to the database and can be modified to use a different database or credentials.
- The `cursor.execute()` function is used to create the table and insert data, and can be modified to match your specific database schema.

### Troubleshooting

- If you encounter any errors when running the script, check that your PostgreSQL server is running and that your database credentials are correct.

## Authors

- Leon
