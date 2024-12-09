import pyspark
from pyspark.sql import SparkSession
import os
from pyspark.sql import Row
from pyspark.sql import functions as F

class IcebergApp:
    def __init__(self, app_name, warehouse_path):
    # ---------- Defining all spark configuration------------
        self.conf = (
            pyspark.SparkConf()
                .setAppName("IcebergApp") # -----Name of the spark application-----
                .set("spark.jars.packages", "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.1") # ----------using iceberg spark package--------
                .set("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") # -------using this to enabling iceberg sql extensions------
                .set("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkCatalog")  # ------Defining spark catalog-------
                .set("spark.sql.catalog.spark_catalog.type", "hadoop") # ------ specifing the hadoop catalog --------
                .set("spark.sql.catalog.spark_catalog.warehouse", warehouse_path)  # -----Defining warehouse location for iceberg table--------
            )

        # --------creating spark session-------------
        spark = SparkSession.builder.config(conf=conf).enableHiveSupport().getOrCreate()

    # "/content/drive/MyDrive/retail_transactions.csv"
    def load_data(self, file_path):
        # ------------Loading dataset into a PySpark DataFrame-----------
        try:
            df = self.spark.read.csv(file_path, header=True, inferSchema=True)
            df.show() # ---------Display the contents of the DataFrame----------
            return df

        except Exception as e:
            print(f"Error reading the CSV file: {e}")
            return None


    # --------declaring all the variables, paths, database_name and table_name-----------
    database_name = "iceberg_table"
    table_name = "retail_transactions"
    # iceberg_table_name = f"spark_catalog.{database_name}.{table_name}"
    # iceberg_table_path = f"/content/warehouse/{database_name}" # ----Path where the iceberg table will be stored ("iceberg_table" database)-----
    # ------Create the database name--------

    def create_database(self, database_name):
        # spark.sql(f"CREATE DATABASE IF NOT EXISTS {database_name}")
        # print(f"Database '{database_name}' created or already exists.")
        try:
            # -------filtering out all the existing databases first--------------
            existing_databases = self.spark.sql("SHOW DATABASES").filter(f"namespace = '{database_name}'")

            # --------If the database not present, then creating it--------
            if existing_databases.count() == 0:  # -----Checking if the count of existing databases is 0-------
                self.spark.sql(f"CREATE DATABASE {database_name}")  # ----- Creating the new database-----
                return f"Database '{database_name}' created."
            else:
                return f"Database '{database_name}' already exists." # ------- printing the statement, database already present--------

        except Exception as e:
            return f"Error creating database: {e}"

        # # Show all databases
        # spark.sql("SHOW databases").show()


    # --------Checking if table already present-------

    def create_iceberg_table_if_not_exists(self, database_name, table_name):
        try:
            existing_tables = self.park.sql(f"SHOW TABLES IN {database_name}").filter(f"tableName = '{table_name}'")

            # ---------If the table not present, then creating it----------
            if existing_tables.count() == 0:  # ------Checking if the count of existing tables is 0-------
                self.spark.sql(f"""
                    CREATE TABLE spark_catalog.{database_name}.{table_name} (  # Create the table with the specified name
                        TransactionNo STRING,
                        CustomerID STRING,
                        ProductID STRING,
                        ProductCategory STRING,
                        Quantity INT,
                        Price DECIMAL(10, 2),
                        Date DATE
                    )
                    USING iceberg  # Specify that this table is an Iceberg table
                    PARTITIONED BY (Date)  -- Partitioning by the Date column for efficient querying
                """)
                return f"Table '{table_name}' created."
            else:
                return f"Table '{table_name}' already exists."

        except Exception as e:
            return f"Error creating table: {e}"
        # --------show all tables in the iceberg_table database--------
        # spark.sql(f"SHOW TABLES IN {database_name}").show()

    # ---------Inserting dataframe to the iceberg_table database--------

    def insert_dataframe_to_iceberg(self, df, database_name, table_name):
        try:
            df.write \
                .format("iceberg") \
                .mode("append") \
                .option("merge-schema", "true") \
                .partitionBy("Date") \
                .saveAsTable(f"spark_catalog.{database_name}.{table_name}")

            return f"DataFrame inserted into table '{table_name}' in database '{database_name}'."

        except Exception as e:
            return f"Error while inserting DataFrame into Iceberg table: {e}"

    # ---------function for quering top 10 records from the iceberg table dataset---------
    def show_top_10(self, database_name, table_name):
        try:
            return self.spark.sql(f"select * from spark_catalog.{database_name}.{table_name}").show(10)

        except Exception as e:
            return f"Error while showing top 10 records: {e}"


    def insert_new_entries(self, new_tracnsaction, database_name, table_name):
        try:
            new_df = self.spark.createDataFrame(new_tracnsaction)
            new_df = new_df.withColumn("Date", F.to_date(F.col("Date"), "yyyy-MM-dd"))

            new_df.write \
                .format("iceberg") \
                .mode("append") \
                .option("merge-schema", "true") \
                .partitionBy("Date") \
                .saveAsTable(f"spark_catalog.{database_name}.{table_name}")

            return self.spark.sql(f"select * from spark_catalog.{database_name}.{table_name} WHERE CustomerID IN ('C001', 'C002')").show()

        except Exception as e:
            return f"Error while inserting new entries into Iceberg table: {e}"

    def update_record(self, database_name, table_name, col_to_update, value, where_col, where_value):

        try:
            # -------checking if values are string or numeric to format the SQL query accordingly-------
            value_str = f"'{value}'" if isinstance(value, str) else value
            where_value_str = f"'{where_value}'" if isinstance(where_value, str) else where_value

            # ----------SQL command to update the specific column in the table----------
            self.spark.sql(f"""
                UPDATE spark_catalog.{database_name}.{table_name}
                SET {col_to_update} = {value_str}
                WHERE {where_col} = {where_value_str}
            """)
            # -----Returning a confirmation message indicating the update was successful--------
            return f"Record updated successfully: Set {col_to_update} to {value_str} where {where_col} = {where_value_str}."

        except Exception as e:
            return f"Error while updating record: {e}"

    def delete_record(self, database_name, table_name, where_col, where_value):
        try:
            # -------checking if where_value is a string or numeric to format the SQL query accordingly--------
            where_value_str = f"'{where_value}'" if isinstance(where_value, str) else where_value

            # --------SQL command to delete records based on the specific condition--------
            self.spark.sql(f"""
                DELETE FROM spark_catalog.{database_name}.{table_name}
                WHERE {where_col} = {where_value_str}
            """)

            # ---------Returning a confirmation message indicating the delete operation was successful----------
            return f"Records deleted successfully from {table_name} where {where_col} = {where_value_str}."
        except Exception as e:
            return f"Error while deleting records: {e}"

    # -------------------Start of upsert function--------------------------------
    def upsert_transactions(self, transactions, database_name, table_name):
        try:
            # -------Creating a DataFrame from the upsert transactions--------
            upsert_df = self.spark.createDataFrame(transactions)

            # -------Get existing transactions from the Iceberg table---------
            existing_df = self.spark.sql(f"SELECT * FROM spark_catalog.{database_name}.{table_name}")

            # -------Separating existing and new records----------
            existing_transactions = existing_df.select("TransactionNo").distinct()
            to_update_df = upsert_df.join(existing_transactions, "TransactionNo", "inner")
            to_insert_df = upsert_df.join(existing_transactions, "TransactionNo", "left_anti")

            # --------Updating existing records-----------
            if not to_update_df.isEmpty():
                to_update_df.createOrReplaceTempView("to_update")
                self.spark.sql(f"""
                    MERGE INTO spark_catalog.{database_name}.{table_name} AS target
                    USING to_update AS source
                    ON target.TransactionNo = source.TransactionNo
                    WHEN MATCHED THEN
                      UPDATE SET target.Quantity = source.Quantity, target.Price = source.Price
                """)

            # ---------Inserting new records---------
            if not to_insert_df.isEmpty():
                # ---------Appending new records to the Iceberg table---------
                to_insert_df.write \
                    .format("iceberg") \
                    .mode("append") \
                    .option("merge-schema", "true") \
                    .partitionBy("Date") \
                    .save(f"spark_catalog.{database_name}.{table_name}")

            # ---------Returning a completion message----------
            return f"Upsert operation completed: {len(transactions)} transactions processed."

        except Exception as e:
            return f"Error during upsert operation: {e}"

    # # Example-----
    # upsert_transactions_data = [
    #     Row(TransactionNo="T001", CustomerID="C001", ProductID="P001", ProductCategory="Electronics", Quantity=3, Price=199.99, Date="2024-01-01"),  # Existing data
    #     Row(TransactionNo="T003", CustomerID="C003", ProductID="P003", ProductCategory="Books", Quantity=1, Price=29.99, Date="2024-01-01")  # New data
    # ]

    # # Calling the function-------
    # result_message = upsert_transactions(spark, upsert_transactions_data, "iceberg_table", "retail_transactions")
    # print(result_message)

    # -------------------End of upsert function--------------------------------

    # -----------function starts for getting the historical data---------------
    def get_historical_transactions(self, database_name, table_name, timestamp):

        try:
            # --------creating a SQL query to select historical data as of the specified timestamp----------
            query = f"""
                SELECT *
                FROM spark_catalog.{database_name}.{table_name}
                AS OF TIMESTAMP '{timestamp}'
            """

            # ---------Executing the SQL query and return the result as a DataFrame---------
            historical_df = self.spark.sql(query)
            return historical_df

        except Exception as e:
            return f"Error while retrieving historical transactions: {e}"

    # Example usage
    # historical_df = get_historical_transactions(spark, "iceberg_table", "retail_transactions", "2024-10-31 00:00:00")
    # historical_df.show()
    # -----------Ending of function for getting the historical data---------------

    def get_customer_transactions(self, database_name, table_name, timestamp, customer_id):

        try:
            # Step 1: Retrieve historical transactions as of the specified timestamp
            historical_df = self.get_historical_transactions(database_name, table_name, timestamp)

            # Step 2: Filter the historical DataFrame for the specified customer ID
            customer_transactions_df = historical_df.filter(historical_df.CustomerID == customer_id)

            return customer_transactions_df

        except Exception as e:
            return f"Error while retrieving customer transactions: {e}"

# Example usage
# customer_id = "C001"
# customer_transactions_df = get_customer_transactions(historical_df, customer_id)
# customer_transactions_df.show()

if __name__ == "__main__":
    warehouse_path = os.path.join(os.getcwd(), 'warehouse')
    iceberg_app = IcebergApp("IcebergApp", warehouse_path)

    df = iceberg_app.load_data("/content/drive/MyDrive/retail_transactions.csv")

    database_name = "iceberg_table"
    table_name = "retail_transactions"

    print(iceberg_app.create_database(database_name))
    print(iceberg_app.create_iceberg_table_if_not_exists(database_name, table_name))
    print(iceberg_app.insert_dataframe_to_iceberg(df, database_name, table_name))

    print("Top 10 records in the Iceberg table:")
    iceberg_app.show_top_10(database_name, table_name)

    new_transactions = [
        Row(TransactionNo="T001", CustomerID="C001", ProductID="P001", ProductCategory="Stationary", Quantity=2, Price=199.99, Date="2024-01-01"),
        Row(TransactionNo="T002", CustomerID="C002", ProductID="P002", ProductCategory="Electronics", Quantity=1, Price=999.99, Date="2024-01-01")
    ]
    print("-----------Start Inserting New Entries------------")
    print(iceberg_app.insert_new_entries(new_transactions, database_name, table_name))
    print("-----------End Inserting New Entries------------")

    print("-----------Start Updating Existing Entries------------")
    print(iceberg_app.update_record(database_name, table_name, "Price", 249.99, "TransactionNo", "T001"))
    print("-----------End Updating Existing Entries------------")

    print("-----------Start Deleting Existing Entries------------")
    print(iceberg_app.delete_record(database_name, table_name, "TransactionNo", "T002"))
    print("-----------End Deleting Existing Entries------------")



    upsert_transactions_data = [
        Row(TransactionNo="T001", CustomerID="C001", ProductID="P001", ProductCategory="Electronics", Quantity=3, Price=199.99, Date="2024-01-01"),
        Row(TransactionNo="T003", CustomerID="C003", ProductID="P003", ProductCategory="Books", Quantity=1, Price=29.99, Date="2024-01-01")
    ]
    print("-----------Start Upserting Transactions------------")
    print(iceberg_app.upsert_transactions(upsert_transactions_data, database_name, table_name))
    print("-----------End Upserting Transactions------------")


    historical_timestamp = "2024-10-31 00:00:00"
    print("-----------Start Fetching Historical Data-----------")
    historical_df = iceberg_app.get_historical_transactions(database_name, table_name, historical_timestamp)
    if historical_df:
        print(f"Historical transactions as of {historical_timestamp}:")
        historical_df.show()

    print("-----------End Fetching Historical Data-----------")


    print("---------Starts Fetching Transaction for Customer ID---------")
    customer_id = "C001"
    customer_transactions_df = iceberg_app.get_customer_transactions(historical_timestamp, customer_id, database_name, table_name)
    if customer_transactions_df:
        print(f"Transactions for Customer ID {customer_id} as of {historical_timestamp}:")
        customer_transactions_df.show()

    print("---------Ends Fetching Transaction for Customer ID---------")

