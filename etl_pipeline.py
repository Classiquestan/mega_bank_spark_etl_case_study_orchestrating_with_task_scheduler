# Import necessary libraries

from pyspark.sql import SparkSession
from pyspark.sql import DataFrameWriter
from pyspark.sql.functions import monotonically_increasing_id
import os
import psycopg2
from dotenv import load_dotenv
from pyspark.sql.functions import col


# Initialize my spark session
jar_path = os.path.abspath(r"postgresql-42.7.11.jar")

spark = SparkSession.builder \
    .appName("Mega Bank ETL") \
    .config("spark.jars", jar_path) \
    .config("spark.driver.extraClassPath", jar_path) \
    .getOrCreate()
print("Spark Session Initialized Successfully")


# Extract this historical data into a spark dataframe
df = spark.read.csv(
    r"dataset\rawdata\nuga_bank_transactions.csv", header=True, inferSchema=True)


# fill up the missing values
df_clean = df.fillna({
    'Customer_Name': 'Unknown',
    'Customer_Address': 'Unknown',
    'Customer_City': 'Unknown',
    'Customer_State': 'Unknown',
    'Customer_Country': 'Unknown',
    'Company': 'Unknown',
    'Job_Title': 'Unknown',
    'Email': 'Unknown',
    'Phone_Number': 'Unknown',
    'Credit_Card_Number': 0,
    'IBAN': 'Unknown',
    'Currency_Code': 'Unknown',
    'Random_Number': 0.0,
    'Category': 'Unknown',
    'Group': 'Unknown',
    'Is_Active': 'Unknown',
    'Description': 'Unknown',
    'Gender': 'Unknown',
    'Marital_Status': 'Unknown'
})


# drop missing values in last updated column
df_clean = df_clean.na.drop(subset=['Last_Updated'])


# Data Transformation to 2NF
# Transaction Table
transaction = df_clean.select('Transaction_Date', 'Amount', 'Transaction_Type') \
    .withColumn('transaction_id', monotonically_increasing_id()) \
    .select('transaction_id', 'Transaction_Date', 'Amount', 'Transaction_Type')


# customer table
customer = df_clean.select('Customer_Name', 'Customer_Address', 'Customer_City', 'Customer_State', 'Customer_Country') \
    .withColumn('customer_id', monotonically_increasing_id()) \
    .select('customer_id', 'Customer_Name', 'Customer_Address', 'Customer_City', 'Customer_State', 'Customer_Country')


# employee table
employee = df_clean.select('Company', 'Job_Title', 'Email', 'Phone_Number', 'Gender', 'Marital_Status') \
    .withColumn('employee_id', monotonically_increasing_id()) \
    .select('employee_id', 'Company', 'Job_Title', 'Email', 'Phone_Number', 'Gender', 'Marital_Status')


# fact table
fact_table = df_clean.join(transaction, ['Transaction_Date', 'Amount', 'Transaction_Type'], 'inner') \
    .join(customer, ['Customer_Name', 'Customer_Address', 'Customer_City', 'Customer_State', 'Customer_Country'], 'inner') \
    .join(employee, ['Company', 'Job_Title', 'Email', 'Phone_Number', 'Gender', 'Marital_Status'], 'inner') \
    .select('transaction_id', 'customer_id', 'employee_id', 'Credit_Card_Number', 'IBAN', 'Currency_Code', 'Random_Number', 'Category', col('Group').alias('Category_Group'), 'Is_Active', 'Last_Updated', 'Description')


# Data Loading
load_dotenv()


def get_db_connection():
    connection = psycopg2.connect(

        host=os.getenv('DB_HOST'),
        database=os.getenv('DB_NAME'),
        user=os.getenv('DB_USER'),
        password=os.getenv('DB_PASSWORD')
    )

    return connection


# connect to sql database
conn = get_db_connection()


# create a function to create tables
def create_table():
    conn = get_db_connection()
    cursor = conn.cursor()
    create_table_query = '''
                        DROP TABLE IF EXISTS customer;
                        DROP TABLE IF EXISTS transaction;
                        DROP TABLE IF EXISTS employee;
                        DROP TABLE IF EXISTS fact_table;

                        CREATE TABLE customer(
                            customer_id BIGINT PRIMARY KEY,
                            Customer_Name VARCHAR(1000),
                            Customer_Address VARCHAR(1000),
                            Customer_City VARCHAR(1000),
                            Customer_State VARCHAR(1000),
                            Customer_Country VARCHAR(1000)
                            );

                        CREATE TABLE transaction(
                            transaction_id BIGINT PRIMARY KEY,
                            Transaction_Date DATE,
                            Amount FLOAT,
                            Transaction_Type VARCHAR(1000)
                            
                            );

                        CREATE TABLE employee(
                            employee_id BIGINT PRIMARY KEY,
                            Company VARCHAR(1000),
                            Job_Title VARCHAR(1000),
                            Email VARCHAR(1000),
                            Phone_Number VARCHAR(1000),
                            Gender VARCHAR(1000),
                            Marital_Status VARCHAR(1000)
                            );

                        CREATE TABLE fact_table(
                            transaction_id BIGINT,
                            customer_id BIGINT,
                            employee_id BIGINT,
                            Credit_Card_Number BIGINT,
                            IBAN VARCHAR(1000),
                            Currency_Code VARCHAR(1000),
                            Random_Number FLOAT,
                            Category VARCHAR(1000),
                            Category_Group VARCHAR(1000),
                            Is_Active VARCHAR(1000),
                            Last_Updated DATE,
                            Description VARCHAR(1000)
                            
                            );
                                                
                        '''
    cursor.execute(create_table_query)
    conn.commit()
    cursor.close()
    conn.close()


create_table()

url = "jdbc:postgresql://localhost:5432/mega_bank"
properties = {
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD'),
    'driver': 'org.postgresql.Driver'
}

customer.write.jdbc(url=url, table="customer",
                    mode="append", properties=properties)
employee.write.jdbc(url=url, table="employee",
                    mode="append", properties=properties)
transaction.write.jdbc(url=url, table="transaction",
                       mode="append", properties=properties)
fact_table.write.jdbc(url=url, table="fact_table",
                      mode="append", properties=properties)

print('database, table and data loaded successfully')
