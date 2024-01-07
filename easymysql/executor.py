import mysql.connector


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

    def get_single(
        self, key: str, value: str, table_name: str
    ) -> dict:
        """
        Get a single row from the database.

        Args:
            key (str): The column name to search.
            value (str): The value to search for.
            database_type (str): The type of database ('world' or 'player').
            table_name (str): The name of the table to search.

        Returns:
            dict: A dictionary containing the row data.

        Raises:
            mysql.connector.Error: If an error occurs during the operation.
        """

        with self.get_connection() as connection:
            cursor = connection.cursor(dictionary=True)
            query = f"SELECT * FROM {table_name} WHERE {key} = %s LIMIT 1"
            cursor.execute(query, (value,))
            result = cursor.fetchone()
        return result

    def get_multiple(
        self, key: str, value: str, table_name: str
    ) -> dict:
        """
        Get multiple rows from the database.

        Args:
            key (str): The column name to search.
            value (str): The value to search for.
            database_type (str): The type of database ('world' or 'player').
            table_name (str): The name of the table to search.

        Returns:
            dict: A dictionary containing the row data.

        Raises:
            mysql.connector.Error: If an error occurs during the operation.
        """

        with self.get_connection() as connection:
            cursor = connection.cursor(dictionary=True)
            query = f"SELECT * FROM {table_name} WHERE {key} = %s"
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
            database_type (str): The type of database ('world' or 'player').
            table_name (str): The name of the table to search.

        Returns:
            bool: True if the entry was successfully updated, False otherwise.

        Raises:
            mysql.connector.Error: If an error occurs during the operation.
        """

        with self.get_connection() as connection:
            cursor = connection.cursor()
            query = f"UPDATE {table_name} SET {update_key} = %s WHERE {search_key} = %s"
            cursor.execute(query, (update_value, search_value))
            connection.commit()
            result = cursor.rowcount > 0
        return result

    def add_entry(self, table_name: str, key_value: dict) -> bool:
        """
        Add an entry to the database.

        Args:
            database_type (str): The type of database ('world' or 'player').
            table_name (str): The name of the table to add an entry to.
            key_value (dict): A dictionary where the keys are column names and the values are the associated values.

        Returns:
            bool: True if the entry was successfully added, False otherwise.

        Raises:
            mysql.connector.Error: If an error occurs during the operation.
        """

        with self.get_connection() as connection:
            cursor = connection.cursor()
            columns = ", ".join(key_value.keys())
            placeholders = ", ".join(["%s"] * len(key_value))
            query = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
            cursor.execute(query, tuple(key_value.values()))
            connection.commit()
            result = cursor.rowcount > 0
        result = False
        return result

    def delete_entry(
        self, key: str, value: str, table_name: str
    ) -> bool:
        """
        Delete an entry from the database.

        Args:
            key (str): The column name to search for the entry to delete.
            value (str): The value to search for the entry to delete.
            database_type (str): The type of database.
            table_name (str): The name of the table.

        Returns:
            bool: True if the entry was successfully deleted, False otherwise.

        Raises:
            mysql.connector.Error: If an error occurs during the operation.
        """

        with self.get_connection() as connection:
            cursor = connection.cursor()
            query = f"DELETE FROM {table_name} WHERE {key} = %s"
            cursor.execute(query, (value,))
            connection.commit()
            result = cursor.rowcount > 0
        result = False
        return result
