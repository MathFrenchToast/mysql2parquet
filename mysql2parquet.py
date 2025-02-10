import pymysql
import pyarrow as pa
import pyarrow.parquet as pq
import pandas as pd
import argparse
import time
import os

default_batch_size = 100000
writer = None

# Function to check if the table has an auto-increment column
def has_auto_increment_column(connection, table_name):
    """
    Check if the given table has an auto-increment column.
    """
    cursor = connection.cursor()
    cursor.execute(f"SHOW CREATE TABLE {table_name};")
    create_table_stmt = cursor.fetchone()[1]
    cursor.close()

    # Check for the presence of AUTO_INCREMENT in the CREATE TABLE statement
    return "AUTO_INCREMENT" in create_table_stmt.upper()

# Updated function to fetch data in batches
def fetch_data_in_batches(connection, table_name, batch_size):
    """
    Fetch data from a table in batches to avoid memory overload.
    Checks if the table has an auto-increment column.
    """
    cursor = connection.cursor()
    
    # Checking for auto-increment column
    if has_auto_increment_column(connection, table_name):
        # Determine the auto-increment column name
        cursor.execute(f"SHOW COLUMNS FROM {table_name};")
        auto_increment_col = None
        auto_increment_index = 0
        for column in cursor.fetchall():
            if column[5]:  # 5 is KEY index in SHOW COLUMNS result
                auto_increment_col = column[0]
                break
            auto_increment_index += 1
        
        if auto_increment_col is None:
            raise ValueError("No auto-increment column found, please check the table.")
        
        # Initialize last_id for fetching rows with greater id
        last_id = 0
        
        while True:
            start_time = time.time()
            query = f"SELECT * FROM {table_name} WHERE {auto_increment_col} > {last_id} ORDER BY {auto_increment_col} ASC LIMIT {batch_size};"
            cursor.execute(query)

            # Fetch the batch
            rows = cursor.fetchall()
            if not rows:
                print("No more rows")
                break

            # Update last_id to the maximum one fetched
            last_id = max(row[auto_increment_index] for row in rows)
            
            # Get column names
            columns = [desc[0] for desc in cursor.description]

            # Convert to DataFrame
            df = pd.DataFrame(rows, columns=columns)

            # Yield the DataFrame as a batch
            yield df

            elapsed_time = time.time() - start_time
            rows_per_second = batch_size / elapsed_time if elapsed_time > 0 else float('inf')

            print(f"Fetched batch with starting id {last_id}. Time: {elapsed_time} s. Rows per second: {rows_per_second:.2f}")

    else:
        # Fallback to original LIMIT/OFFSET strategy
        offset = 0
        while True:
            query = f"SELECT * FROM {table_name} LIMIT {batch_size} OFFSET {offset};"
            cursor.execute(query)

            # Fetch the batch
            rows = cursor.fetchall()
            if not rows:
                print("No more rows")
                break

            # Get column names
            columns = [desc[0] for desc in cursor.description]

            # Convert to DataFrame
            df = pd.DataFrame(rows, columns=columns)

            # Yield the DataFrame as a batch
            yield df

            # Move to the next batch
            offset += batch_size
            print(f"Moved to offset {offset}.")

# Function to save data to a Parquet file
def save_to_parquet(df, parquet_file, tablename: str, batch_count: int):
    """
    Save a DataFrame to a Parquet file. Appends to the file if it already exists.
    """
    table = pa.Table.from_pandas(df)

    writer = pq.ParquetWriter(f"{tablename}/{parquet_file}_{batch_count}.parquet", table.schema)            
    writer.write_table(table)
    writer.close()
   

# Main function to handle parameters and run the process
def main(args):
    # Establish a database connection
    connection = pymysql.connect(
        host=args.host,
        user=args.user,
        password=args.password,
        database=args.database,
        ssl={"fake_flag_to_enable_tls":True}
    )

    tablename = args.table
    parquet_file = args.output_file if args.output_file else tablename    
    batch_size = args.batch_size    
    batch_count = 0

    # Create a new directory because it does not exist
    dirExist = os.path.exists(tablename)
    if not dirExist:
        os.makedirs(tablename)

    try:
        # Process the data in batches and save it to Parquet
        for batch_df in fetch_data_in_batches(connection, tablename, batch_size):
            save_to_parquet(batch_df, parquet_file, tablename, batch_count)
            batch_count += 1
            

    finally:        
        connection.close()

# Command-line argument parsing
def parse_args():
    parser = argparse.ArgumentParser(
        description="Fetch records from a MySQL database and save them as a Parquet file."
    )

    # Define supported parameters
    parser.add_argument(
        '--host', required=True,
        help="The host of the MySQL database."
    )
    parser.add_argument(
        '--user', required=True,
        help="The MySQL user to connect with."
    )
    parser.add_argument(
        '--password', required=True,
        help="The password for the MySQL user."
    )
    parser.add_argument(
        '--database', required=True,
        help="The name of the database to connect to."
    )
    parser.add_argument(
        '--table', required=True,
        help="The name of the table to fetch data from."
    )
    parser.add_argument(
        '--output-file',
        help="The output Parquet file where the data will be saved."
    )
    parser.add_argument(
        '--batch-size', type=int, default=default_batch_size,
        help=f"The number of rows to fetch in each batch (default: {default_batch_size})."
    )

    return parser.parse_args()

# Entry point
if __name__ == "__main__":
    args = parse_args()
    main(args)
