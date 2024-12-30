import os


def clean_csv_file(file_path):
    temp_file = file_path + '.tmp'

    with open(file_path, 'r', newline='', encoding='utf-8') as infile, \
         open(temp_file, 'w', newline='', encoding='utf-8') as outfile:

        for line in infile:
            cleaned_line = line.replace('$ ', '')
            outfile.write(cleaned_line)

        outfile.close()
        infile.close()

    os.replace(temp_file, file_path)
    print(f"Cleaned and saved: {file_path}")


csv_files = ['your_file_name']

for csv_file in csv_files:
    clean_csv_file(csv_file)