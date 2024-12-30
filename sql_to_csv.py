import mysql.connector
import os
import csv


output_dir = './exported_tables'
os.makedirs(output_dir, exist_ok=True)

db_config = {
    'host': 'localhost',
    'user': 'root',
    'password': '',
    'database': 'chinook',
}
column_names = None
conn = mysql.connector.connect(**db_config)

try:
    cursor = conn.cursor()

    # Get the list of tables
    cursor.execute("SHOW TABLES;")
    tables = cursor.fetchall()  # Returns a list of tuples

    for table_name_tuple in tables:
        table_name = table_name_tuple[0]
        
        # Fetch data from the current table
        cursor.execute(f"SELECT * FROM {table_name};")
        rows = cursor.fetchall()

        # Get column names from the table
        column_names = [desc[0] for desc in cursor.description]
        
        output_file = os.path.join(output_dir, f"{table_name}.csv")
        
        # Write the data into the CSV file
        with open(output_file, 'w', newline='', encoding='utf-8') as csvfile:
            csvwriter = csv.writer(csvfile)
            
            # Write the header row
            csvwriter.writerow(column_names)
            
            # Write the table data rows
            csvwriter.writerows(rows)

            csvfile.close()
        
        print(f"Exported table '{table_name}' to {output_file}")
finally:
    conn.close()