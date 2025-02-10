# MySQL to Parquet Conversion Tool

This project provides a simple tool to convert data from a MySQL database to Parquet files, which are widely used for efficient data storage and processing in big data frameworks.
Memory consumption is kept low by processing data in chunks.
If the table has an auto-incrementing primary key, the tool will use it to fetch data in chunks. If not, it will use a the limit and offset method.

## Installation

To install the tool, you need to have Python 3.x and pip installed. (tested with Python 3.12) Follow these steps:

1. **Clone the repository**:
   ```bash
   git clone https://github.com/yourusername/mysql2parquet.git
   cd mysql2parquet
   ```

2. **Install required packages**:
   You can install the necessary dependencies using pip:
   ```bash
   pip install -r requirements.txt
   ```

   Ensure you have `mysql-connector` and `pyarrow` (or another library for Parquet support) listed in your `requirements.txt`.

Here’s the updated documentation for using the `mysql2parquet.py` tool that reflects the parameterized usage:


#### Configuration

1. **Database Connection**: 
   You will  specify your MySQL database connection parameters (host, user, password, database) via command-line arguments when running the script.

#### Usage

Execute the script from the command line by providing the required parameters:

```bash
python mysql2parquet.py --host <host> --user <user> --password <password> --database <database> --table <table_name> --output-file <output_file.parquet> [--batch-size <batch_size>]
```

parameters:
-  `--host` : MySQL server host (e.g., `localhost`).
-  `--user` : MySQL username.
-  `--password` : password for your MySQL user.
-  `--database` : name of the database you want to connect to.
-  `--table` : name of the table you wish to convert.
-  `--output-file` ! desired output path for the Parquet file.
-  `--batch_size` (optionnal) to adjust the number of rows fetched in each batch (default is `10000`).

#### Example

To convert a table named `employees` from a database `company` running on `localhost` with the user `root` and password `mypassword`, to a Parquet file named `employees.parquet`, you would run:

```bash
python mysql2parquet.py --host localhost --user root --password mypassword --database company --table employees --output-file employees.parquet
```

--- 

Make sure to update any other documentation or resources to reflect these changes in the usage pattern.

## Contributions

This code was done quickly to solve a single pain point, so it is far from perfect and would need lot of refactoring to be production ready. But, feel free to fork the repository and submit pull requests for improvements and enhancements!

## License

This project is licensed under the MIT License. See the LICENSE file for details.