# easy-mysql

## Introduction 

easy-mysql provides a simple interface to execute SQL statements in Python without the need for an ORM: 

- Covers basic actions such as get_multiple, get_single, add_entry, update_single_field, delete_entry 
- Initializes a MySQL connection pool 
- Handles opening/closing the pool connection 

Sure, you could implement this yourself. But holy fuck! You're just as lazy as I am, and you're tired of re-writing the same code. 

And wow! you're even more like me! You don't really like using ORM's because they're annoying and require constant attention. You're an adult! 

You don't need every teeny little thing to be represented in a programming language as an object; it's a needless abstraction! 

Shut up, you go to your room! 

## Examples 

### Adding a New Entry
```python
db = MysqlExecute(
    db_host='localhost',
    db_port=3306,
    db_user='root',
    db_password='password',
    db_name='mydatabase',
)

new_user = {
    'name': 'Donny B',
    'email': 'don@don.com',
    'age': 36,
}

try:
    success = db.add_entry(table_name='users', key_value=new_user)
    if success:
        print('New user added successfully.')
    else:
        print('Failed to add new user.')
except MysqlExecuteError as error:
    print(f'An error occurred: {error}')
```

### Getting Multiple Rows
```python
try:
    users = db.get_multiple(
        key='age',
        value=36,
        table_name='users',
    )
    for user in users:
        print(user)
except MysqlExecuteError as error:
    print(f'An error occurred: {error}')
```

### Executing a Custom Query
```python
# Executing a SELECT query
try:
    custom_query = "SELECT * FROM `users` WHERE `age` < %s"
    params = (25,)
    users = db.execute_query(custom_query, params=params)
    for user in users:
        print(user)
except MysqlExecuteError as error:
    print(f'An error occurred: {error}')

# Executing an UPDATE query
try:
    custom_query = "UPDATE `users` SET `age` = %s WHERE `name` = %s"
    params = (26, 'Alice Smith')
    db.execute_query(custom_query, params=params, commit=True)
    print('User age updated successfully.')
except MysqlExecuteError as error:
    print(f'An error occurred: {error}')
```
