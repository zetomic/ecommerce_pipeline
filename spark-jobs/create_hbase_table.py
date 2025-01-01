import happybase

def create_table():
    connection = happybase.Connection('hbase-master', port=9090)
    connection.open()

    table_name = 'product_data_HBase_tbl'
    column_families = {
        'product_data': dict(),
        'order_data': dict(),
        'customer_data': dict(),
        'pricing': dict()
    }

    if table_name.encode() not in connection.tables():
        connection.create_table(table_name, column_families)
        print(f"Table {table_name} created successfully.")
    else:
        print(f"Table {table_name} already exists.")

    connection.close()

if __name__ == "__main__":
    create_table()
