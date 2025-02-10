import pymysql
import pyarrow as pa
import pyarrow.parquet as pq
import pandas as pd
import argparse
import time

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
        for column in cursor.fetchall():
            if column[5]:  # 5 is KEY index in SHOW COLUMNS result
                auto_increment_col = column[0]
                break
        
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
            last_id = max(row[0] for row in rows)  # Assuming the first column is the auto-increment id
            
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
def save_to_parquet(df, parquet_file):
    """
    Save a DataFrame to a Parquet file. Appends to the file if it already exists.
    """
    table = pa.Table.from_pandas(df)

    # If the Parquet file exists, append the data, otherwise create a new one.
    try:
        pq.read_table(parquet_file)
        with pq.ParquetWriter(parquet_file, table.schema) as writer:
            writer.write_table(table)
    except:
        # Create a new Parquet file if it doesn't exist
        pq.write_table(table, parquet_file)

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

    parquet_file = args.output_file
    batch_size = args.batch_size

    try:
        # Process the data in batches and save it to Parquet
        for batch_df in fetch_data_in_batches(connection, args.table, batch_size):
            save_to_parquet(batch_df, parquet_file)
            print(f"Processed batch with {len(batch_df)} rows.")

    finally:
        # Close the connection after processing
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
        '--output-file', required=True,
        help="The output Parquet file where the data will be saved."
    )
    parser.add_argument(
        '--batch-size', type=int, default=10000,
        help="The number of rows to fetch in each batch (default: 10000)."
    )

    return parser.parse_args()

# Entry point
if __name__ == "__main__":
    args = parse_args()
    main(args)