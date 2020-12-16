import os
import csv
from simpledbf import Dbf5
import lib.database


def parse_data(file_name, mode):
    relative_file_path = f'./database/{file_name}'
    
    if mode == 'csv':
        with open(relative_file_path) as file_name:
            return [row for row in file_name]
    elif mode == 'dbf':
        dbf = Dbf5(relative_file_path, codec='utf-8')
        os.system('rm tmp.csv > /dev/null 2>&1')
        dbf.to_csv('tmp.csv')
        with open('tmp.csv') as file_name:
            csv_list = [row for row in file_name]
        os.system('rm tmp.csv')
    else:
        raise
    return csv_list


def process_file(file_name):
    if 'ON_OFFS_NT' in file_name:
        on_offs = True
        zones = False
        on_offs_period = file_name[:2].lower()
        collection = 'on_offs_nt'
    elif 'zones_' in file_name and 'min.csv' in file_name:
        zones = True
        on_offs = False
        zone_name = file_name.split('zones_')[1].replace('.csv', '')
        collection = 'zones'
    else:
        on_offs = False
        zones = False
        collection = file_name[:-4].lower()

    if file_name[-3:].lower() == 'csv':
        mode = 'csv'
    elif file_name[-3:].lower() == 'dbf':
        mode = 'dbf'
    else:
        print(f'File not supported >> {file_name}')
        raise

    
    data = parse_data(file_name, mode)
    header_list = [h.rstrip().lower() for h in data[0].split(',')]
    # remove header
    del data[0]

    # special cases
    if on_offs:
        header_list.append('period')
    elif zones:
        header_list.append('zone_name')
    else:
        pass

    all_rows = []
    for row in data:
        clean_row = [r.replace('"', '').replace('*', '').rstrip() for r in row.split(',')]
        if on_offs:
            clean_row.append(on_offs_period)
        elif zones:
            clean_row.append(zone_name)
        else:
            pass

        row_dict = dict(zip(header_list, clean_row))
        all_rows.append(row_dict)
    
    lib.database.insert_many(collection, all_rows) 
    return


def main():
    current_path = os.path.dirname(__file__)
    appended_path = os.path.join(current_path, 'database/')
    files_list = os.listdir(appended_path)

    lib.database.drop_database('cca') 
    for file_name in files_list:
        process_file(file_name)
    return


if __name__ == '__main__':
    main()
