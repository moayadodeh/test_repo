
def read_sql_query(file_path):
    print(file_path)
    with open(file_path, 'r') as file:
        sql_query = file.read()
    return sql_query

    