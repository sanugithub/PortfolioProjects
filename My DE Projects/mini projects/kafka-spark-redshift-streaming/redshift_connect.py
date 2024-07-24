# Dummy script to test redshift connection

import psycopg2

# Redshift connection parameters
host = "redshift-cluster-1.cp6taicsq2ry.us-east-1.redshift.amazonaws.com"  # e.g., cluster-name.region.redshift.amazonaws.com
port = "5439"  # Default Redshift port
dbname = "dev"
user = "admin"
password = "Admin123"

# Connection string
conn_string = f"dbname='{dbname}' user='{user}' host='{host}' port='{port}' password='{password}'"

# Connect to Redshift
try:
    conn = psycopg2.connect(conn_string)
    print("Connected to Redshift!")

    # Create a cursor
    cursor = conn.cursor()

    # Execute a query
    query = "SELECT version();"  # Example query: get version of Redshift
    cursor.execute(query)

    # Fetch and print the result
    version = cursor.fetchone()
    print(f"Redshift version: {version}")

    # Close the cursor and connection
    cursor.close()
    conn.close()

except Exception as error:
    print(f"Error: {error}")