import mysql.connector
from contextlib import contextmanager


class MysqlExecuteError(Exception):
    """Custom exception class for MysqlExecute."""

    pass


class MysqlExecute:
    def __init__(
        self,
        db_host: str,
        db_port: int,
        db_user: str,
        db_password: str,
        db_name: str,
        pool_size: int = 3,
    ):
        self.db_host = db_host
        self.db_port = db_port
        self.db_user = db_user
        self.db_password = db_password
        self.db_name = db_name

        self.pool = mysql.connector.pooling.MySQLConnectionPool(
            pool_name=f"{self.db_name}_pool",
            pool_size=pool_size,
            host=self.db_host,
            port=self.db_port,
            user=self.db_user,
            password=self.db_password,
            database=self.db_name,
        )

    @contextmanager
    def manage_connection(self):
        """
        Context manager to handle database connections.
        """

        connection = None
        try:
            connection = self.get_connection()
            yield connection
        finally:
            if connection.is_connected():
                connection.close()

    def get_connection(
        self,
    ) -> mysql.connector.pooling.PooledMySQLConnection:
        """
        Get a connection from the pool.

        Returns:
            MySQLConnection: A connection to the database.

        Raises:
            mysql.connector.Error: If an error occurs during the operation.
        """

        return self.pool.get_connection()

    def _is_valid_table_or_column(
        self, table_name: str, column_name: str = None
    ) -> bool:
        """
        Check if a table or column exists in the database.

        Args:
            table_name (str): The name of the table to check.
            column_name (str, optional): The name of the column to check, if applicable. Defaults to None.

        Raises:
            e: mysql.connector.Error: If an error occurs during the operation.

        Returns:
            bool: True if the table or column exists, False otherwise.
        """

        with self.manage_connection() as connection:
            with connection.cursor() as cursor:
                if column_name:
                    cursor.execute(
                        "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS "
                        "WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s AND COLUMN_NAME = %s",
                        (self.db_name, table_name, column_name),
                    )
                else:
                    cursor.execute(
                        "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES "
                        "WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s",
                        (self.db_name, table_name),
                    )
                result = cursor.fetchone()
                return result is not None

    def safe_table_column(self, table_name: str, column_name: str = None) -> str:
        """
        Prevent SQL Injection by checking if a table or column exists in the database.

        Args:
            table_name (str): The name of the table to check.
            column_name (str, optional): The name of the column to check, if applicable. Defaults to None.

        Raises:
            ValueError: If the table or column does not exist.

        Returns:
            str: The table or column name.
        """

        if not self._is_valid_table_or_column(table_name, column_name):
            raise ValueError("Invalid table or column name.")
        return table_name if not column_name else f"{table_name}.{column_name}"

    def get_single(self, key: str, value: str, table_name: str) -> dict:
        """
        Get a single row from the database.

        Args:
            key (str): The column name to search.
            value (str): The value to search for.
            table_name (str): The name of the table to search.

        Returns:
            dict: A dictionary containing the row data.

        Raises:
            mysql.connector.Error: If an error occurs during the operation.
        """

        table_name = self.safe_table_column(table_name)
        key = self.safe_table_column(table_name, key)

        with self.manage_connection() as connection:
            with connection.cursor(dictionary=True) as cursor:
                query = "SELECT * FROM `{}` WHERE `{}` = %s LIMIT 1".format(
                    table_name, key
                )
                cursor.execute(query, (value,))
                result = cursor.fetchone()
        return result

    def get_multiple(self, key: str, value: str, table_name: str) -> list:
        """
        Get multiple rows from the database.

        Args:
            key (str): The column name to search.
            value (str): The value to search for.
            table_name (str): The name of the table to search.

        Returns:
            list: A list of dictionaries containing the row data.

        Raises:
            mysql.connector.Error: If an error occurs during the operation.
        """

        table_name = self.safe_table_column(table_name)
        key = self.safe_table_column(table_name, key)

        with self.manage_connection() as connection:
            with connection.cursor(dictionary=True) as cursor:
                query = "SELECT * FROM `{}` WHERE `{}` = %s".format(table_name, key)
                cursor.execute(query, (value,))
                result = cursor.fetchall()
        return result

    def update_single_field(
        self,
        search_key: str,
        search_value: str,
        update_key: str,
        update_value: str,
        table_name: str,
    ) -> bool:
        """
        Update a single field in a single row.

        Args:
            search_key (str): The column name to search.
            search_value (str): The value to search for.
            update_key (str): The column name to update.
            update_value (str): The value to update to.
            table_name (str): The name of the table to search.

        Returns:
            bool: True if the entry was successfully updated, False otherwise.

        Raises:
            mysql.connector.Error: If an error occurs during the operation.
        """

        table_name = self.safe_table_column(table_name)
        search_key = self.safe_table_column(table_name, search_key)
        update_key = self.safe_table_column(table_name, update_key)

        with self.manage_connection() as connection:
            with connection.cursor() as cursor:
                query = "UPDATE `{}` SET `{}` = %s WHERE `{}` = %s".format(
                    table_name, update_key, search_key
                )
                cursor.execute(query, (update_value, search_value))
                connection.commit()
                result = cursor.rowcount > 0
        return result

    def add_entry(self, table_name: str, key_value: dict) -> bool:
        """
        Add an entry to the database.

        Args:
            table_name (str): The name of the table to add the entry to.
            key_value (dict): A dictionary containing the column names and values to add.

        Returns:
            bool: True if the entry was successfully added, False otherwise.
        """

        table_name = self.safe_table_column(table_name)
        columns = ", ".join("`{}`".format(column) for column in key_value.keys())
        placeholders = ", ".join(["%s"] * len(key_value))

        with self.manage_connection() as connection:
            with connection.cursor() as cursor:
                query = "INSERT INTO `{}` ({}) VALUES ({})".format(
                    table_name, columns, placeholders
                )
                cursor.execute(query, tuple(key_value.values()))
                connection.commit()
                result = cursor.rowcount > 0
        return result

    def bulk_insert(self, table_name: str, list_of_key_values: list) -> bool:
        """
        Bulk insert entries into the database.

        Args:
            table_name (str): The name of the table to add the entries to.
            list_of_key_values (list): List of dictionaries containing the column names and values to add.

        Returns:
            bool: True if the entries were successfully added, False otherwise.
        """

        if not list_of_key_values:
            return False

        table_name = self.safe_table_column(table_name)
        columns = ", ".join(
            "`{}`".format(column) for column in list_of_key_values[0].keys()
        )
        placeholders = ", ".join(["%s"] * len(list_of_key_values[0]))
        values_to_insert = [tuple(entry.values()) for entry in list_of_key_values]

        with self.manage_connection() as connection:
            with connection.cursor() as cursor:
                query = "INSERT INTO `{}` ({}) VALUES ({})".format(
                    table_name, columns, placeholders
                )
                cursor.executemany(query, values_to_insert)
                connection.commit()
                return cursor.rowcount == len(list_of_key_values)

    def delete_entry(self, key: str, value: str, table_name: str) -> bool:
        """
        Delete an entry from the database.

        Args:
            key (str): Key to search for.
            value (str): Value to search for.
            table_name (str): The name of the table to delete the entry from.

        Returns:
            bool: True if the entry was successfully deleted, False otherwise.
        """

        table_name = self.safe_table_column(table_name)
        key = self.safe_table_column(table_name, key)

        with self.manage_connection() as connection:
            with connection.cursor() as cursor:
                query = "DELETE FROM `{}` WHERE `{}` = %s".format(table_name, key)
                cursor.execute(query, (value,))
                connection.commit()
                result = cursor.rowcount > 0
        return result
