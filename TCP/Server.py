import json
import re
import socket
import struct
import pymongo
import random
from datetime import datetime, timedelta

# -------------------------globals------------------------
used_database = "Gradina"


# ------------UTILS--------------#

def mongoDB_conn():
    client = pymongo.MongoClient(host="localhost", port=27017)

    return client


def receive_large_data(conn):
    header = conn.recv(8)
    if not header:
        return None

    message_length = struct.unpack("!Q", header)[0]
    received_data = bytearray()

    while len(received_data) < message_length:
        chunk = conn.recv(min(1024, message_length - len(received_data)))
        if not chunk:
            # Connection closed by the remote host
            raise ConnectionResetError("Connection closed by the remote host")
        received_data.extend(chunk)

    return received_data.decode()


def send_large_data(conn, data):
    msg_length = len(data)
    header = struct.pack("!Q", msg_length)
    conn.send(header)

    chunk_size = 1024
    for i in range(0, msg_length, chunk_size):
        chunk = data[i:i + chunk_size]
        conn.send(chunk)

    # Signal the end of data by sending an empty chunk
    conn.send(b'')


def open_file(path="C:\\Users\\petru\\PycharmProjects\\Database\\resources\\metadata.json"):
    try:
        with open(path) as json_file:
            return json.load(json_file)
    except json.JSONDecodeError:
        return {"databases": {}}


def write_json(json_database):
    with open("C:\\Users\\petru\\PycharmProjects\\Database\\resources\\metadata.json", 'w') as f:
        json.dump(json_database, f, indent=1)


constants = open_file("C:\\Users\\petru\\PycharmProjects\\Database\\resources\\const.json")['const']


def rm_comma(statement):
    start_index = statement.lower().index('(') + 1
    end_index = statement.lower().rfind(')')
    if end_index == -1:
        raise ValueError("Closing parenthesis ')' not found in the statement.")
    end_index = statement.rfind(')', 0, end_index + 1)

    attributes = statement[start_index:end_index].strip()

    att_list = [attr.strip() for attr in attributes.split(',')]

    return att_list


# ---------------------------use database--------------------------#

def use(data, conn):
    # use Gradina
    db = open_file()
    statement = data.split()
    db = db["databases"].get(statement[1], None)
    if db:
        global used_database
        used_database = statement[1]
        message = "NOW USING DATABASE {}".format(statement[1])
        try:
            mongoDB_conn()[used_database]
        except ValueError as e:
            message += f"\n{str(e)}"
        conn.send(message.encode())
    else:
        message = "DATABASE DOES NOT EXIST"
        conn.send(message)


# ------------------------------------------------------------------#


# -------------------------------CREATE database,table,index--------------------#

def create_database(db, db_name):
    # create database Gradina
    if db_name in db["databases"]:
        return "Database {} already exists.".format(db_name)

    db["databases"][db_name] = {"tables": {}}
    write_json(db)
    return "CREATED DATABASE {}".format(db_name)


def create_index(db, db_name, table_name, index_name, columns, unique):
    # create index index_depozit on Depozit (id_teren,id_porumb)
    # create unique index index_teren on Teren (nume,hectare)
    # create unique index index_porumb1 on Porumb (id_teren,denumire)
    # create index index_porumb2 on Porumb (id_teren,data)

    global used_database

    if not used_database:
        return "No database selected. Use a database first."

    if table_name not in db["databases"][db_name]["tables"]:
        return "Table {} does not exist in database {}".format(table_name, db_name)

    existing_indexes = db["databases"][db_name]["tables"][table_name]["IndexFiles"]
    for existing_index in existing_indexes:
        if existing_index['indexName'] == index_name:
            return "INDEX {} ALREADY EXISTS".format(index_name)

    for item in columns:
        if not any(attribute['Attribute']['name'] == item for attribute in
                   db["databases"][db_name]["tables"][table_name]['Structure']):
            return 'Column {} not found'.format(item)

    new_index = {
        'indexName': index_name,
        'keyLength': len(columns),
        'isUnique': unique,
        'indexType': 'BTree',
        'IndexAttributes': columns
    }

    db["databases"][db_name]["tables"][table_name]['IndexFiles'].append(new_index)
    write_json(db)

    if unique:
        # Create UniqIndex collection
        mongoDB_conn()[used_database][f'UniqIndex_{index_name}'].create_index(columns)
    else:
        # Create NonUniqIndex collection
        mongoDB_conn()[used_database][f'NonUniqIndex_{index_name}'].create_index(columns)

    return "Index created!"


def create_table(db, db_name, table_name, attributes):
    # create table Teren (id int(20) primary key not null unique, nume nvarchar(20) not null , hectare int(10) not null, tip nvarchar(20), data int(10) not null )
    # create table Porumb (id int(20) primary key not null, id_teren int(20) foreign key (id_teren) references Teren (id), cantitate int (20) not null , denumire nvarchar(30) not null unique, data int(20) not null )
    # create table Depozit (id int(20) primary key not null unique, id_teren int(20) foreign key (id_teren) references Teren (id), id_porumb int(20) foreign key (id_porumb) references Porumb (id), nume nvarchar(30) not null unique )
    mon = mongoDB_conn()[used_database]
    database = open_file()['databases'][used_database]
    if table_name in db['databases'][db_name]['tables']:
        return f"Table {table_name} already exists in the database."

    table = {
        'fileName': f'{table_name.lower()}.bin',
        'rowLength': str(len(attributes)),
        'Structure': [],
        'IndexFiles': [],
        'uniqueKeys': [],
        'primaryKey': None,
        'foreignKeys': []
    }
    primary_key_check = False

    for attribute in attributes:
        splitter = attribute.split()
        if len(splitter) >= 2:
            column_name = splitter[0]
            column_type = splitter[1]

            # Check for foreign key declaration
            if 'foreign' in splitter and 'references' in splitter:
                # Check if there are enough elements in between
                references_table = splitter[6]
                if references_table in database['tables']:
                    references_column = splitter[7].strip("()")
                    table['foreignKeys'].append({
                        'attributeName': column_name,
                        'references_table': references_table,
                        'references_column': references_column
                    })
                    if column_type.lower().split('(')[0] in constants:
                        if '(' in column_type.lower() and ')' in column_type.lower():
                            value = column_type.split('(')[1].replace(')', '')
                            table['Structure'].append({
                                "Attribute": {
                                    'name': column_name,
                                    'type': column_type.split('(')[0],
                                    'length': int(value),
                                    'isnull': True
                                }
                            })
                        elif '(' not in column_type.lower() and ')' not in column_type.lower():
                            table['Structure'].append({
                                "Attribute": {
                                    'name': column_name,
                                    'type': column_type,
                                    'length': 1,
                                    'isnull': True
                                }
                            })
                        else:
                            return f"Check for value types: {constants}"
                    else:
                        return "Invalid foreign key declaration: {}".format(attribute)
                else:
                    return f'Table {references_table} does not exist!'
            else:
                # Handle other column types
                if column_type.lower().split('(')[0] in constants:
                    if '(' in column_type.lower() and ')' in column_type.lower():
                        value = column_type.split('(')[1].replace(')', '')
                        table['Structure'].append({
                            "Attribute": {
                                'name': column_name,
                                'type': column_type.split('(')[0],
                                'length': int(value),
                                'isnull': True
                            }
                        })
                    elif '(' not in column_type.lower() and ')' not in column_type.lower():
                        table['Structure'].append({
                            "Attribute": {
                                'name': column_name,
                                'type': column_type,
                                'length': 1,
                                'isnull': True
                            }
                        })
                else:
                    return f"Check for value types: {constants}"

                for item in range(2, len(splitter)):
                    if splitter[item].lower() == 'not' and splitter[item + 1].lower() == 'null':
                        table['Structure'][-1]['Attribute']['isnull'] = False
                    elif splitter[item].lower() == 'primary' and splitter[item + 1].lower() == 'key':
                        primary_key_check = True
                        table['primaryKey'] = column_name
                    elif splitter[item].lower() == 'unique':
                        table['uniqueKeys'].append(column_name)
        else:
            return "Invalid attribute format: {}".format(attribute)

    if not primary_key_check:
        return "No primary key specified for the table."

    db['databases'][db_name]["tables"][table_name] = table
    mon.create_collection(table_name)

    write_json(db)
    return "TABLE CREATED {}".format(table_name)


def create(data, conn):
    db = open_file()
    statement = data.split()

    if statement[1].lower() == "database":
        message = create_database(db, statement[2])
    elif statement[1].lower() == 'table':
        if used_database:
            tbName = statement[2]
            attributes = rm_comma(data)
            message = create_table(db, used_database, tbName, attributes)
        else:
            message = "No database selected. Use a database first."
    elif statement[1].lower() == 'index':
        if used_database:
            if statement[3].lower() == 'on':
                index_name = statement[2]
                table_name = statement[4]
                columns = rm_comma(data)
                message = create_index(db, used_database, table_name, index_name, columns, unique=False)
            else:
                message = "Syntax error"
    elif statement[1].lower() == 'unique':
        if used_database:
            if statement[2].lower() == 'index' and statement[4].lower() == 'on':
                index_name = statement[3]
                table_name = statement[5]
                columns = rm_comma(data)
                message = create_index(db, used_database, table_name, index_name, columns, unique=True)
            else:
                message = "Syntax error"
        else:
            message = "No database selected. Use a database first."
    conn.send(message.encode())


# -----------------------------------------------------------------------------------------------------------#


# -------------------------- DROP database,table--------------------------------------------#

def drop_database(db, db_name):
    mon = mongoDB_conn()
    if db_name not in db["databases"]:
        return "Database {} does not exist".format(db_name)
    del db["databases"][db_name]
    mon.drop_database(used_database)
    write_json(db)
    return "DROPPED DATABASE {}".format(db_name)


def drop_table(db, db_name, table_name):
    if not used_database:
        return "No database selected. Use a database first."

    if table_name not in db["databases"][db_name]["tables"]:
        return f"Table {table_name} does not exist in database {db_name}"

    # Drop associated indexes
    drop_indexes(db, db_name, table_name)

    foreign_key_references = []
    for other_table_name, other_table in db["databases"][db_name]["tables"].items():
        for foreign_key in other_table["foreignKeys"]:
            if foreign_key["references_table"] == table_name:
                foreign_key_references.append(other_table_name)

    for referenced_table_name in foreign_key_references:
        drop_table(db, db_name, referenced_table_name)

    del db["databases"][db_name]["tables"][table_name]
    write_json(db)
    mongoDB_conn()[used_database][table_name].drop()

    return f"DROPPED TABLE {table_name}"


def drop_indexes(db, db_name, table_name):
    if table_name not in db["databases"][db_name]["tables"]:
        return

    indexes = db["databases"][db_name]["tables"][table_name]["IndexFiles"]

    for index in indexes:
        index_name = index["indexName"]

        if mongoDB_conn()[used_database]['UniqIndex_' + index_name] is not None:
            mongoDB_conn()[used_database]['UniqIndex_' + index_name].drop()

        if mongoDB_conn()[used_database]['NonUniqIndex_' + index_name] is not None:
            mongoDB_conn()[used_database]['NonUniqIndex_' + index_name].drop()

    return


def drop(data, conn):
    db = open_file()
    statement = data.split()
    if statement[1].lower() == "database":
        message = drop_database(db, statement[2])
    elif statement[1].lower() == "table":
        if not used_database:
            message = "No database selected. Use a database first."
        else:
            message = drop_table(db, used_database, statement[2])
    else:
        message = "Syntax error!"
    conn.send(message.encode())


# --------------------------------------------------------------------------#


# ----------Insert---------------------------------------------#

def check_if_table_exists(tb_name, db):
    if used_database:
        if tb_name in db["databases"][used_database]["tables"]:
            return True
    return False


def compareType(value, column_type, length):
    if not isinstance(value, str):
        return False, "Value must be a string."

    if column_type.lower() == 'int':
        if not value.isdigit():
            return False, f"Invalid value for 'int' type: {value}. Must be a non-negative integer."
    elif column_type.lower().startswith('nvarchar'):
        if len(value) > length:
            return False, f"Value exceeds the maximum length {length} for 'nvarchar' type."
    else:
        return False, f"Unsupported column type: {column_type}."

    return True, None


def insert_string_spacing(insert_string):
    values_start_index = insert_string.lower().find('values') + len('values')

    columns_part = insert_string[insert_string.find('(') + 1:insert_string.find(')')].strip()
    values_part = insert_string[values_start_index:insert_string.rfind(')')].strip().strip('(')

    # Split columns and values by commas and remove extra spaces
    columns = [col.strip() for col in columns_part.split(',')]
    values = [val.strip() for val in values_part.split(',')]

    return columns, values


def insert(data, conn, used_database):
    if not used_database:
        message = "No database selected. Use a database first."
        conn.send(message.encode())
        return

    db = open_file()
    table_name = data.split()[2]

    if (
            data.split()[1].lower() != "into"
            or not check_if_table_exists(table_name, db)
            or data.split()[4].lower() != "values"
    ):
        message = 'Syntax error: INSERT INTO TABLE_NAME VALUES (values)'
        conn.send(message.encode())

        return

    columns, values = insert_string_spacing(data)
    table = db['databases'][used_database]['tables'][table_name]

    if len(columns) != len(table['Structure']):
        message = 'Number of columns do not match!'
        conn.send(message.encode())

    elif len(columns) != len(values):
        message = 'Number of values do not match the number of columns!'
        conn.send(message.encode())

    else:
        record = {'_id': '', 'value': ''}
        structure = table['Structure']
        name_value = []
        for i in range(len(columns)):
            column_name = columns[i]
            column_value = values[i]
            name_value.append((column_name, column_value))
            # Validate and compare column type and length
            is_valid, validation_message = compareType(
                column_value, structure[i]['Attribute']['type'], structure[i]['Attribute']['length']
            )
            if not is_valid:
                message = f"Validation error for column '{column_name}': {validation_message}"
                conn.send(message.encode())

                return

            # Check for duplicate primary key value
            if column_name == table['primaryKey']:
                existing_record = mongoDB_conn()[used_database][table_name].find_one({'_id': column_value})
                if existing_record:
                    message = f"Duplicate value '{column_value}' for primary key column '{column_name}'"
                    conn.send(message.encode())

                    return
                else:
                    # Set the custom primary key value
                    record['_id'] = column_value
            else:
                record['value'] += f"{column_value}$"

            # Check foreign key constraint
            if column_name in [fk['attributeName'] for fk in table['foreignKeys']]:
                foreign_key_check_result = check_foreign_key_constraint(table_name, column_name, column_value)
                if foreign_key_check_result is not None:
                    message = foreign_key_check_result
                    conn.send(message.encode())

                    return

            # Check for unique constraint
            if column_name in table['uniqueKeys']:
                existing_record = mongoDB_conn()[used_database][table_name].find_one({column_name: column_value})
                if existing_record:
                    message = f"Unique constraint violation. Value '{column_value}' already exists for column '{column_name}'."
                    conn.send(message.encode())

                    return

            # Check for not null constraint
            if not structure[i]['Attribute']['isnull'] and (column_value is None or column_value.strip() == ''):
                message = f"Not null constraint violation. Value cannot be null or empty for column '{column_name}'."
                conn.send(message.encode())

                return

        record['value'] = record['value'][:-1]

        update_index_collections(db, table_name, record['_id'], name_value)
        mongoDB_conn()[used_database][table_name].insert_one(record)
        message = 'Insertion was successful'


def update_index_collections(db, table_name, primary_key, column_name):
    global used_database

    if 'IndexFiles' not in db["databases"][used_database]["tables"][table_name]:
        return

    indexes = db["databases"][used_database]["tables"][table_name]["IndexFiles"]

    for index in indexes:
        if index['isUnique']:
            # Update unique index collection
            key_values = [value for col, value in column_name if col in index['IndexAttributes']]
            key_value = '$'.join(map(str, key_values))
            uniq_index_collection = mongoDB_conn()[used_database]['UniqIndex_' + index['indexName']]
            existing_record = uniq_index_collection.find_one({'_id': key_value})
            if existing_record:
                message = "Unique constraint violation. Value already exists for column"
                raise ValueError(message)
            else:
                uniq_index_collection.insert_one({'_id': key_value, 'value': primary_key})
        else:
            # Update non-unique index collection
            key_values = [value for col, value in column_name if col in index['IndexAttributes']]
            key_value = '$'.join(map(str, key_values))
            non_uniq_index_collection = mongoDB_conn()[used_database]['NonUniqIndex_' + index['indexName']]
            existing_record = non_uniq_index_collection.find_one({'_id': key_value})
            if existing_record:
                found_value = existing_record['value']
                # Concatenate existing value with primaryKey separated by '#'
                key_value = '$'.join(map(str, key_values))
                non_uniq_index_collection.update_one({'_id': key_value},
                                                     {'$set': {'value': found_value + '#' + primary_key}})
            else:
                non_uniq_index_collection.insert_one({'_id': key_value, 'value': primary_key})


def check_foreign_key_constraint(table_name, column_name, foreign_key_values):
    global used_database

    if not used_database:
        return "No database selected. Use a database first."

    table_config = open_file()['databases'][used_database]['tables'][table_name]

    if not table_config:
        return f"Table '{table_name}' not found in the database."

    foreign_keys = table_config['foreignKeys']

    if not foreign_keys:
        return f"No foreign keys defined for table '{table_name}'."

    for fk_config in foreign_keys:
        referenced_column = fk_config['references_column']
        parent_table_name = fk_config['references_table']
        attribute_name = fk_config['attributeName']
        if referenced_column not in column_name:
            return f"Foreign key constraint violation. Missing value for '{attribute_name}'."

        parent_record = mongoDB_conn()[used_database][parent_table_name].find_one(
            {"_id": foreign_key_values})

        if parent_record is None:
            return (f"Foreign key constraint violation. Value '{foreign_key_values}' not found in "
                    f"referenced parent table column '{referenced_column}'.")

    return None  # All foreign key constraints are satisfied


# ------------------------------------------------------------------------------#


def delete(data, conn):
    global used_database
    db = open_file()
    statement = data.split()

    if len(statement) < 5 or statement[1].lower() != "from" or statement[3].lower() != "where":
        message = "Syntax error: DELETE FROM <table> WHERE <condition>"
        conn.send(message.encode())
        return

    table_name = statement[2]
    condition = statement[4]

    if not used_database:
        message = "No database selected. Use a database first."
        conn.send(message.encode())
        return

    if table_name not in db["databases"][used_database]["tables"]:
        message = f"Table {table_name} does not exist in the database."
        conn.send(message.encode())
        return

    try:
        # Extract primary key column and value from the condition
        primary_key_value = condition.split('=')[1].strip("'")  # Assumes primary key value is a string

        # Check if there are dependent records in other tables
        if has_dependent_records(table_name, primary_key_value):
            message = f"Cannot delete record from {table_name}. Dependent records exist in other tables."
        else:
            # Delete from unique and non-unique indexes
            delete_from_indexes(db, table_name, primary_key_value)

            # Perform the actual deletion based on the primary key
            result = mongoDB_conn()[used_database][table_name].delete_one({'_id': primary_key_value})
            if result.deleted_count > 0:
                message = f"Deleted {result.deleted_count} record(s) from {table_name} where {condition}."
            else:
                message = f"No records found in {table_name} where {condition}."
    except Exception as e:
        message = f"Error during deletion: {str(e)}"

    conn.send(message.encode())


def delete_from_indexes(db, table_name, primary_key_value):
    global used_database

    if 'IndexFiles' not in db["databases"][used_database]["tables"][table_name]:
        return

    indexes = db["databases"][used_database]["tables"][table_name]["IndexFiles"]

    for index in indexes:
        key_value = primary_key_value

        if index['isUnique']:
            # Delete from unique index collection
            uniq_index_collection = mongoDB_conn()[used_database]['UniqIndex_' + index['indexName']]
            uniq_index_collection.delete_one({'value': key_value})
        else:
            # Delete from non-unique index collection
            non_uniq_index_collection = mongoDB_conn()[used_database]['NonUniqIndex_' + index['indexName']]

            # Retrieve the current concatenated values
            existing_record = non_uniq_index_collection.find_one({'value': {'$regex': f'.*{key_value}.*'}})

            if existing_record:
                current_values = existing_record['value'].split('#')

                # Remove the specific value from the concatenated values
                current_values = [v for v in current_values if v != key_value]

                # If the current values are not empty, update the non-unique index collection
                if current_values:
                    updated_values = '#'.join(current_values)
                    non_uniq_index_collection.update_one({'_id': existing_record['_id']},
                                                         {'$set': {'value': updated_values}})
                else:
                    # If there are no more values, delete the entry from the non-unique index collection
                    non_uniq_index_collection.delete_one({'_id': existing_record['_id']})


def has_dependent_records(table_name, primary_key_value):
    global used_database
    db = open_file()

    # Check all tables in the database for foreign key references
    for other_table_name, other_table in db["databases"][used_database]["tables"].items():
        for foreign_key in other_table["foreignKeys"]:
            if (foreign_key["references_table"] == table_name and
                    foreign_key["references_column"] == "id"):
                # Check if there are dependent records in the other table
                dependent_record = mongoDB_conn()[used_database][other_table_name].find_one(
                    {'value': {'$regex': f'.*{foreign_key["attributeName"] + ":" + primary_key_value}.*'}})
                if dependent_record:
                    return True

    return False


# -------------------------------------------------------------------------------------------------------------#

def convert_condition_to_value(sign, column_value, value):
    if sign == '=':
        return column_value == value
    elif sign == '>':
        return column_value > value
    elif sign == '<':
        return column_value < value
    elif sign == '>=':
        return column_value >= value
    elif sign == '<=':
        return column_value <= value
    else:
        raise ValueError(f"Invalid condition sign: {sign}")


def check_condition(condition):
    match = re.match(r'(\w+)\s*([<>]=?|=)\s*(\w+)', condition)
    if not match:
        raise ValueError(f"Invalid condition format: {condition}")

    column, sign, value = match.groups()
    return column, sign, value


def apply_conditions(row, conditions, table_name):
    for condition in conditions:
        column, sign, value = check_condition(condition)
        if column == open_file()["databases"][used_database]["tables"][table_name][
            'primaryKey']:
            column = '_id'
        col_check = False
        for item in row:

            if item[0] == column:
                col_check = True
                column_value = item[1]
                break

        if not col_check:
            raise ValueError(f"Column '{column}' not found in table columns.")

        if column_value.isdigit() and value.isdigit():
            column_value, value = int(column_value), int(value)

        if not convert_condition_to_value(sign, column_value, value):
            return False

    return True


def select_the_columns(items, items_metadata, selected_columns):
    selected_column_values = []
    all_column_values = []
    for item in items:
        primary_key = item['_id']
        values = item['value'].split('$')
        passed_column = 1
        passed_for_all_column = 1
        column_value = []
        all_column_value = []
        for i in range(0, len(items_metadata["Structure"])):
            if selected_columns[0] != '*':
                if items_metadata["Structure"][i]['Attribute']['name'] in selected_columns:
                    if items_metadata["Structure"][i]['Attribute']['name'] == \
                            items_metadata['primaryKey']:
                        column_value.append(('_id', primary_key))
                    else:
                        column_value.append(
                            (items_metadata["Structure"][i]['Attribute']['name'], values[i - passed_column]))
            else:
                if items_metadata["Structure"][i]['Attribute']['name'] == \
                        items_metadata['primaryKey']:
                    column_value.append(('_id', primary_key))
                else:
                    column_value.append(
                        (items_metadata["Structure"][i]['Attribute']['name'], values[i - passed_column]))

            if items_metadata["Structure"][i]['Attribute']['name'] == items_metadata['primaryKey']:
                all_column_value.append(('_id', primary_key))
            else:
                all_column_value.append(
                    (items_metadata["Structure"][i]['Attribute']['name'], values[i - passed_for_all_column]))

        if len(column_value) > 0:
            selected_column_values.append(column_value)
        if len(all_column_value) > 0:
            all_column_values.append(all_column_value)
    return all_column_values, selected_column_values


def find_column_index(items_metadata, column):
    for i, item in enumerate(items_metadata['Structure']):
        if item['Attribute']['name'] == column:
            return i
    return None


def find_unique_column_condition(items_metadata, conditions):
    unique_keys = items_metadata['uniqueKeys']
    list_of_unique_columns = []
    for condition in conditions:
        column, sign, value = check_condition(condition)
        for unique_key in unique_keys:
            if unique_key == column:
                if unique_key == items_metadata['primaryKey']:
                    list_of_unique_columns.append(
                        {"unique_column": ('_id', sign, find_column_index(items_metadata, unique_key)),
                         "unique_value": value})
                else:
                    list_of_unique_columns.append(
                        {"unique_column": (unique_key, sign, find_column_index(items_metadata, unique_key)),
                         "unique_value": value})
    if list_of_unique_columns:
        return list_of_unique_columns
    else:
        return None


def find_index_condition(items_metadata, conditions):
    list_of_total_index_attributes = []
    for index_info in items_metadata['IndexFiles']:
        index_attributes = index_info['IndexAttributes']
        list_of_index_attributes_for_a_index_file = []
        for condition in conditions:
            column, sign, value = check_condition(condition)
            for index_attr in index_attributes:
                if index_attr == column and column == items_metadata['primaryKey']:
                    list_of_index_attributes_for_a_index_file.append(
                        ('_id', sign, value))
                elif index_attr == column:
                    list_of_index_attributes_for_a_index_file.append(
                        (index_attr, sign, value))
        if list_of_index_attributes_for_a_index_file:
            if len(index_info['IndexAttributes']) == len(list_of_index_attributes_for_a_index_file):
                list_of_total_index_attributes.append(
                    {'index_info': index_info, 'matched_columns': list_of_index_attributes_for_a_index_file})

    if list_of_total_index_attributes:
        return list_of_total_index_attributes
    else:
        return None


def check_column_with_id(column, items_metadata):
    if items_metadata['Structure'][0]['Attribute']['name'] == items_metadata['primaryKey']:
        return True
    index_pk = 0
    for i in range(0, len(items_metadata['Structure'])):
        if items_metadata['Structure'][i]['Attribute']['name'] == items_metadata['primaryKey']:
            index_pk = i
            break

    for i, item in enumerate(items_metadata['Structure']):
        if item['Attribute']['name'] == column:
            if i > index_pk:
                return True
            break
    return False


def fetch_rows_using_uniqueness(unique_column_result, items_metadata, table_name, index_file_data):
    ok_equal = -1
    ok_id_equal = -1
    pr_key = 0
    if unique_column_result:
        for i, item in enumerate(unique_column_result):
            if item['unique_column'][1] == '=' and item['unique_column'][0] != '_id':
                ok_equal = i
                if check_column_with_id(item['unique_column'][0], items_metadata):
                    pr_key = 1
            elif item['unique_column'][1] == '=' and item['unique_column'][0] == '_id':
                ok_id_equal = i
        if ok_id_equal != -1:
            column = mongoDB_conn()[used_database].get_collection(table_name).find(
                {'_id': unique_column_result[ok_id_equal]['unique_value']})
            if column is None:
                raise ValueError(
                    f"Column with id={unique_column_result[ok_id_equal]['unique_value']} not found in table {table_name}")
        elif ok_equal != -1:
            for item in index_file_data:
                values = item['value'].split('$')
                if values[unique_column_result[i]['unique_column'][2] - pr_key] == unique_column_result[i][
                    'unique_value']:
                    column = [item]
                    break
        elif ok_equal == -1 and ok_id_equal == -1:
            return index_file_data
        return column
    else:
        return None


def fetch_uniq_columns_for_index(item, index_name, table_name):
    pr_key = ''
    for i, index_attr in enumerate(item['index_info']['IndexAttributes']):
        if pr_key == '':
            for it in item['matched_columns']:
                if it[0] == index_attr:
                    pr_key += f'{it[2]}'
                    break
        else:
            for it in item['matched_columns']:
                if it[0] == index_attr:
                    pr_key += f'${it[2]}'
                    break
    row = mongoDB_conn()[used_database].get_collection(f'UniqIndex_{index_name}').find_one({'_id': pr_key})
    id_of_the_uniq_col = row['value']
    return [mongoDB_conn()[used_database].get_collection(table_name).find_one({'_id': id_of_the_uniq_col})]


def fetch_non_uniq_columns_for_index(item, index_name, table_name):
    pr_key = ''
    result = []
    for i, index_attr in enumerate(item['index_info']['IndexAttributes']):
        if i == 0:
            for it in item['matched_columns']:
                if it[0] == index_attr:
                    pr_key += f'{it[2]}'
                    break
        else:
            for it in item['matched_columns']:
                if it[0] == index_attr:
                    pr_key += f'${it[2]}'
                    break
    row = mongoDB_conn()[used_database].get_collection(f'NonUniqIndex_{index_name}').find_one({'_id': pr_key})
    ids_of_col = row['value'].split('#')
    for id_of_col in ids_of_col:
        result.append(mongoDB_conn()[used_database].get_collection(table_name).find_one({'_id': id_of_col}))
    return result


def fetch_rows_using_index(items_metadata, index_results, table_name, index_file_data):
    list_of_rows = []
    new_index_res = []
    check_eq = True
    check_eq_per_index = []
    for item in index_results:
        for col in item['matched_columns']:
            if col[1] != '=':
                check_eq = False
                break
        check_eq_per_index.append(check_eq)
        check_eq = True
    if True not in check_eq_per_index:
        return index_file_data
    for item in index_results:
        if len(item['index_info']['IndexAttributes']) == len(item['matched_columns']):
            new_index_res.append(item)
    if new_index_res:
        for i, item in enumerate(new_index_res):
            uniqueness = item['index_info']['isUnique']
            if uniqueness:
                list_of_a_row = fetch_uniq_columns_for_index(item, item['index_info']['indexName'],
                                                             table_name)
            elif not uniqueness:
                list_of_a_row = fetch_non_uniq_columns_for_index(item, item['index_info']['indexName'],
                                                                 table_name)
            if i == 0:
                list_of_rows = list_of_a_row
            elif i != 0:
                cop = []
                for row in list_of_rows:
                    if row in list_of_a_row:
                        cop.append(row)
                list_of_rows = cop
        return list_of_rows
    else:
        return None


def apply_index_or_uniqueness(items_metadata, conditions, table_name, index_file_data):
    if not conditions:
        return index_file_data
    unique_column_results = find_unique_column_condition(items_metadata, conditions)
    index_results = find_index_condition(items_metadata, conditions)
    if unique_column_results is not None and index_results is not None:
        return fetch_rows_using_uniqueness(unique_column_results, items_metadata, table_name, index_file_data)

    elif unique_column_results is not None:
        return fetch_rows_using_uniqueness(unique_column_results, items_metadata, table_name, index_file_data)
    elif index_results is not None:
        return fetch_rows_using_index(items_metadata, index_results, table_name, index_file_data)
    return index_file_data


def process_join_conditions(query, on_index):
    join_conditions = ' '.join(query[on_index:])
    last_char = join_conditions[-1]
    while last_char != ')':
        join_conditions += ' ' + ' '.join(query[len(join_conditions) + on_index:])
        last_char = join_conditions[-1]
    join_conditions = join_conditions.strip('()')
    processed_conditions = [cond.strip().replace(' ', '') for cond in join_conditions.split('and')]
    return processed_conditions


def parse_condition(condition):
    for operator in ['>=', '<=', '=', '<', '>']:
        if operator in condition:
            left_part, right_part = condition.split(operator)
            return left_part.strip(), operator, right_part.strip()
    raise ValueError(f"Invalid condition format: {condition}")


def match_condition(row, join_row, cond, items_metadata, join_table_metadata, table_name, join_table_name):
    left_col, operator, right_col = parse_condition(cond)

    def extract_value(col, row_data, metadata):
        if col == '_id':
            col = metadata['primaryKey']
        for tuple in row_data:
            if tuple[0] == '_id':
                if col == metadata['primaryKey']:
                    return tuple[1]
            else:
                if tuple[0] == col:
                    return tuple[1]
        return None

    if '.' in left_col:
        left_col_table, left_col_name = left_col.split('.')
        left_val = extract_value(left_col_name, row if left_col_table == table_name else join_row,
                                 items_metadata if left_col_table == table_name else join_table_metadata)
    else:
        left_val = left_col

    if '.' in right_col:
        right_col_table, right_col_name = right_col.split('.')
        right_val = extract_value(right_col_name, row if right_col_table == table_name else join_row,
                                  items_metadata if right_col_table == table_name else join_table_metadata)
    else:
        right_val = right_col

    if left_val is None or right_val is None:
        return False

    if left_val.isdigit() and right_val.isdigit():
        left_val, right_val = int(left_val), int(right_val)

    return convert_condition_to_value(operator, left_val, right_val)


def get_table_cond(join_conditions, table_name):
    res_cond = []
    for cond in join_conditions:
        left, op, right = parse_condition(cond)
        if '.' in left and '.' in right:
            tb_name1, tb_col1 = left.split('.')
            tb_name2, tb_col2 = right.split('.')
            if tb_name2 == tb_name1 and tb_name1 == table_name:
                res_cond.append(f'{tb_col1}{op}{tb_col2}')

        elif '.' in left and '.' not in right:
            tb_name1, tb_col1 = left.split('.')
            if tb_name1 == table_name:
                res_cond.append(f'{tb_col1}{op}{right}')

        elif '.' not in left and '.' in right:
            tb_name2, tb_col2 = right.split('.')
            if tb_name2 == table_name:
                res_cond.append(f'{left}{op}{tb_col2}')
    if res_cond != []:
        return res_cond
    return []


def inner_nested_loop_join_logic(table_data, join_table_data, items_metadata, join_table_metadata, join_conditions,
                                 table_name,
                                 join_table_name, selected_table_name_columns, selected_join_table_columns):
    joined_data = []
    table_conditions = get_table_cond(join_conditions, table_name)
    join_table_conditions = get_table_cond(join_conditions, join_table_name)
    hash_normal_cond = apply_index_or_uniqueness(items_metadata, table_conditions, table_name, table_data)
    hash_join_cond = apply_index_or_uniqueness(join_table_metadata, join_table_conditions, join_table_name,
                                               join_table_data)
    hashed_table_data = select_the_columns(hash_normal_cond, items_metadata, '*')[0]
    hashed_joined_data = select_the_columns(hash_join_cond, join_table_metadata, '*')[0]
    primary_key_table = items_metadata['primaryKey']
    primary_key_join_table = join_table_metadata['primaryKey']
    print(hashed_joined_data)
    for row in hashed_table_data:
        for join_row in hashed_joined_data:
            if all(match_condition(row, join_row, cond, items_metadata, join_table_metadata, table_name,
                                   join_table_name) for cond in
                   join_conditions):
                renamed_row = [
                    (table_name + '.' + primary_key_table if col[0] == '_id' else f'{table_name}.{col[0]}', col[1]) for
                    col in
                    row]
                renamed_join_row = [
                    (join_table_name + '.' + primary_key_join_table if col[
                                                                           0] == '_id' else f'{join_table_name}.{col[0]}',
                     col[1]) for col in
                    join_row]
                merged_row = renamed_row + renamed_join_row
                filtered_row = [col for col in merged_row if
                                f"{col[0]}" in selected_table_name_columns or f"{col[0]}" in selected_join_table_columns]
                joined_data.append(filtered_row)
    return joined_data


def inner_hash_join_logic(table_data, join_table_data, items_metadata, join_table_metadata, join_conditions, table_name,
                          join_table_name, selected_table_name_columns, selected_join_table_columns):
    primary_join_condition = None
    for cond in join_conditions:
        left_col, operator, right_col = parse_condition(cond)
        if operator == '=' and '.' in left_col and '.' in right_col:
            primary_join_condition = cond
            break
    if not primary_join_condition:
        raise ValueError("No suitable join condition found.")

    left_col, _, right_col = parse_condition(primary_join_condition)
    table1_name, table1_col = left_col.split('.')
    table2_name, table2_col = right_col.split('.')
    primary_key_table = items_metadata['primaryKey']
    primary_key_join_table = join_table_metadata['primaryKey']
    table1_col = items_metadata['primaryKey'] if table1_col == '_id' else table1_col
    table2_col = join_table_metadata['primaryKey'] if table2_col == '_id' else table2_col

    table1_col = items_metadata['primaryKey'] if table1_col == '_id' else table1_col
    table2_col = join_table_metadata['primaryKey'] if table2_col == '_id' else table2_col

    table1_key = find_column_index(items_metadata, table1_col) if table1_name == table_name else find_column_index(
        join_table_metadata, table1_col)
    table2_key = find_column_index(join_table_metadata,
                                   table2_col) if table2_name == join_table_name else find_column_index(items_metadata,
                                                                                                        table2_col)
    hash_table = {}
    if table_name != table1_name:
        cop = table1_key
        table1_key = table2_key
        table2_key = cop
    for row in table_data:
        key_val = row['_id'] if table1_key == 0 else row['value'].split('$')[table1_key - 1]
        if key_val not in hash_table:
            hash_table[key_val] = []
        hash_table[key_val].append(row)
    joined_data = []

    for join_row in join_table_data:
        join_key_val = join_row['_id'] if table2_key == 0 else join_row['value'].split('$')[table2_key - 1]
        if join_key_val in hash_table.keys():
            matching_rows = hash_table.get(join_key_val)
            for row in matching_rows:
                row = select_the_columns([row], items_metadata, '*')[0][0]
                join_row = select_the_columns([join_row], join_table_metadata, '*')[0][0]
                if all(match_condition(row, join_row, cond, items_metadata, join_table_metadata, table_name,
                                       join_table_name) for cond in
                       join_conditions):
                    renamed_row = [
                        (table_name + '.' + primary_key_table if col[0] == '_id' else f'{table_name}.{col[0]}', col[1])
                        for
                        col in
                        row]
                    renamed_join_row = [
                        (join_table_name + '.' + primary_key_join_table if col[
                                                                               0] == '_id' else f'{join_table_name}.{col[0]}',
                         col[1]) for col in
                        join_row]
                    merged_row = renamed_row + renamed_join_row
                    filtered_row = [col for col in merged_row if
                                    f"{col[0]}" in selected_table_name_columns or f"{col[0]}" in selected_join_table_columns]
                    joined_data.append(filtered_row)

    return joined_data


def perform_join(table_data, join_table_data, items_metadata, join_table_metadata, join_conditions, join_type,
                 join_mode, table_name, join_table_name, selected_table_name_columns, selected_join_table_columns):
    joined_data = []

    if join_type == 'inner':
        if join_mode == 'nested_loop':
            joined_data = inner_nested_loop_join_logic(table_data, join_table_data, items_metadata, join_table_metadata,
                                                       join_conditions, table_name, join_table_name,
                                                       selected_table_name_columns, selected_join_table_columns)
        elif join_mode == 'hash':
            joined_data = inner_hash_join_logic(table_data, join_table_data, items_metadata, join_table_metadata,
                                                join_conditions, table_name, join_table_name,
                                                selected_table_name_columns, selected_join_table_columns)

    return joined_data


def select(data, conn):
    # select (Teren.id,Depozit.id_teren,Teren.nume,Depozit.nume) from Teren inner join Depozit on (Depozit.id_teren=Teren.id and Depozit.id<20)
    # select * from Teren inner join Depozit on (Depozit.id_teren=Teren.id and Depozit.id_porumb<1000 and Teren.tip=Industrial)
    query = data.split()
    distinct = 'distinct' in query
    join = 'join' in query
    where = 'where' in query
    join_mode = 'hash'
    if distinct:
        selected_columns = query[2].strip('()').split(',')
        table_index = 4
    else:
        selected_columns = query[1].strip('()').split(',')
        table_index = 3

    table_name = query[table_index]
    processed_selected_columns = [col.split('.')[-1] for col in selected_columns]
    processed_distinct_selected_columns = []
    table_data = mongoDB_conn()[used_database].get_collection(table_name).find()
    items_metadata = open_file()["databases"][used_database]["tables"][table_name]

    if processed_selected_columns[0] == '*':
        for item in items_metadata['Structure']:
            if item['Attribute']['name'] != items_metadata['primaryKey']:
                processed_distinct_selected_columns.append(item['Attribute']['name'])
            else:
                processed_distinct_selected_columns.append('_id')
    else:
        processed_distinct_selected_columns = processed_selected_columns
    filtered_data = []
    if join:
        join_type = query[query.index('join') - 1].lower()
        join_table = query[query.index('join') + 1]
        join_table_data = mongoDB_conn()[used_database].get_collection(join_table).find()
        join_table_metadata = open_file()["databases"][used_database]["tables"][join_table]
        on_index = query.index('on') + 1
        join_conditions = process_join_conditions(query, on_index)
        selected_table_name_columns, selected_join_table_columns = [], []
        if selected_columns[0] != '*':
            for sel_col in selected_columns:
                tb_name, col_name = sel_col.split('.')
                if tb_name == table_name:
                    selected_table_name_columns.append(sel_col)
                else:
                    selected_join_table_columns.append(sel_col)
        else:
            for sel_col in items_metadata['Structure']:
                selected_table_name_columns.append(table_name + '.' + sel_col['Attribute']['name'])

            for sel_col in join_table_metadata['Structure']:
                selected_join_table_columns.append(join_table + '.' + sel_col['Attribute']['name'])
        filtered_data = perform_join(table_data, join_table_data, items_metadata, join_table_metadata, join_conditions,
                                     join_type, join_mode, table_name, join_table, selected_table_name_columns,
                                     selected_join_table_columns)

    elif where:
        where_index = query.index('where') + 1
        conditions = ' '.join(query[where_index:])
        last_char = conditions[-1]
        while last_char != ')':
            conditions += ' ' + ' '.join(query[len(conditions) + where_index:])
            last_char = conditions[-1]
        conditions = conditions.strip('()')

        processed_conditions = []
        for cond in conditions.split('and'):
            parts = cond.split()
            if '.' in parts[0]:
                parts[0] = parts[0].split('.')[-1]
            processed_conditions.append(' '.join(parts))

        stripped_conditions = [cond.strip().replace(' ', '') for cond in processed_conditions]
        source_data = apply_index_or_uniqueness(items_metadata, stripped_conditions, table_name,
                                                filtered_data) if join else apply_index_or_uniqueness(items_metadata,
                                                                                                      stripped_conditions,
                                                                                                      table_name,
                                                                                                      table_data)
        for row in source_data:
            all_column_values, selected_column_values = select_the_columns([row], items_metadata,
                                                                           processed_selected_columns)
            if apply_conditions(all_column_values[0], stripped_conditions, table_name):
                filtered_data.append(selected_column_values[0])

    if not where and not join:
        all_column_values, selected_column_values = select_the_columns(table_data, items_metadata,
                                                                       processed_selected_columns)
        filtered_data = selected_column_values
    final_data = []

    if distinct:
        unique_data = set()
        for row in filtered_data:
            row_tuple = tuple()
            for items in row:
                if items[0] in processed_distinct_selected_columns:
                    row_tuple += (items[1],)
            if row_tuple not in unique_data:
                unique_data.add(row_tuple)
                final_data.append(row)
    else:
        final_data = filtered_data

    # Constructing result string
    result = f'{table_name}:\n'
    for row in final_data:
        result += ' | '.join(f'{col[0]}:{col[1]}' for col in row) + '\n'

    # Sending data in chunks
    chunk_size = 1024
    for i in range(0, len(result), chunk_size):
        chunk = result[i:i + chunk_size]
        send_large_data(conn, chunk.encode())
    send_large_data(conn, 'END_OF_DATA'.encode())


def insert_data(conn, used_database, first_i, last_i):
    for _ in range(first_i, last_i):
        teren_id = _ + 1
        nume = f"Teren_{teren_id}"
        hectare = random.randint(1, 400)
        tip = random.choice(["Agricultural", "Residential", "Industrial"])
        data = int((datetime.now() - timedelta(days=random.randint(1, 365))).timestamp())

        teren_insert_query = f"insert into Teren (id,nume,hectare,tip,data) values ({str(teren_id)},{str(nume)},{str(hectare)},{str(tip)},{str(data)})"
        insert(teren_insert_query, conn, used_database)

        porumb_id = _ + 1
        teren_id_porumb = teren_id
        cantitate = random.randint(1, 1000)
        denumire = f"Porumb_{porumb_id}"
        data_porumb = int((datetime.now() - timedelta(days=random.randint(1, 365))).timestamp())

        porumb_insert_query = f"insert into Porumb (id,id_teren,cantitate,denumire,data) values ({str(porumb_id)},{str(teren_id_porumb)},{str(cantitate)},{str(denumire)},{str(data_porumb)})"
        insert(porumb_insert_query, conn, used_database)

        depozit_id = _ + 1
        teren_id_depozit = random.randint(1, teren_id)
        porumb_id_depozit = random.randint(1, teren_id)
        nume_depozit = f"Depozit_{depozit_id}"

        depozit_insert_query = f"insert into Depozit (id,id_teren,id_porumb,nume) values ({str(depozit_id)},{str(teren_id_depozit)},{str(porumb_id_depozit)},{str(nume_depozit)})"
        insert(depozit_insert_query, conn, used_database)


def server_program():
    global used_database
    host = socket.gethostname()
    port = 5050
    server_socket = socket.socket()
    server_socket.bind((host, port))
    server_socket.listen(5)
    conn, address = server_socket.accept()
    print("Connection from:" + str(address))

    while True:
        data = receive_large_data(conn)
        if not data:
            break
        if str(data).split()[0].lower() == 'create':
            create(str(data), conn)
        elif str(data).split()[0].lower() == 'use':
            use(str(data), conn)
        elif str(data).split()[0].lower() == 'drop':
            drop(str(data), conn)
        elif str(data).split()[0].lower() == 'insert':
            insert(str(data), conn, used_database)
        elif str(data).split()[0].lower() == 'delete':
            delete(str(data), conn)
        elif str(data).split()[0].lower() == 'select':
            select(str(data), conn)
        elif str(data).split()[0].lower() == '1':
            for i in range(0, 100000, 99):
                insert_data(conn, used_database, i + 1, i + 99)
            conn.send('Done'.encode())

        else:
            conn.send('Syntax error'.encode())

        print("Received from connected user: ")

    conn.close()


if __name__ == '__main__':
    server_program()
