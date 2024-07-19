from pyspark.sql import SparkSession

def process_data():
    # spark = SparkSession.builder.appName("GCPDataprocJob").config("spark.sql.warehouse.dir","/airflow_assmt1/hive_data/").enableHiveSupport().getOrCreate()
    spark = SparkSession.builder.appName("GCPDataprocJob").config("spark.sql.warehouse.dir","gs://airflow_assmt1/hive_data/").enableHiveSupport().getOrCreate()

    # Define your GCS bucket and paths
    bucket = "airflow_assmt1"
    emp_data_path = f"gs://{bucket}/input_files/employee.csv"

    # Read datasets
    employee = spark.read.csv(emp_data_path, header=True, inferSchema=True)


    # Filter employee data
    filtered_employee = employee.filter(employee.salary >=60000) # Adjust the salary threshold as needed
    
    # Hive database and table names
    # hive_database_name = "airflow"
    # hive_table_name = "filtered_employee"

    # HQL to create the Hive database if it doesn't exist
    hive_create_database_query = f"CREATE DATABASE IF NOT EXISTS airflow"
    spark.sql(hive_create_database_query)

# HQL to create the Hive table inside the 'airflow' database if it doesn't exist
    hive_create_table_query = f"""
        CREATE TABLE IF NOT EXISTS airflow.filtered_employee (
        emp_id INT,
        emp_name STRING,
        dept_id INT,
        salary INT
        )
        STORED AS PARQUET
    """
    # Execute the HQL to create the Hive table
    spark.sql(hive_create_table_query)

    # # Write the filtered employee data to the Hive table in append mode
    filtered_employee.write.mode("append").format("hive").saveAsTable("airflow.filtered_employee")

    

if __name__ == "__main__":
    process_data()