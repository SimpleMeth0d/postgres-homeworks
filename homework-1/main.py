"""Скрипт для заполнения данными таблиц в БД Postgres."""
import psycopg2
import csv

customers_data_file = 'north_data/customers_data.csv'
employees_data_file = 'north_data/employees_data.csv'
orders_data_file = 'north_data/orders_data.csv'


def insert_data_to_sql(filename, name_table):

    try:
        with open(filename, 'r', encoding='UTF-8') as csvfile:
            data = csv.DictReader(csvfile, delimiter=',')
            with psycopg2.connect(host='localhost', database='north', user='postgres', password='1707') as conn:
                with conn.cursor() as curs:
                    for row in data:
                        row_data = []
                        values_designation = []
                        for key in list(row.keys()):
                            row_data.append(row[key])
                            values_designation.append('%s')
                        curs.execute(f"INSERT INTO {name_table} VALUES ({', '.join(values_designation)})", tuple(row_data))
                        row_data.clear()
                        values_designation.clear()
            conn.close()
    except FileNotFoundError:
        raise FileNotFoundError(f'Отсутствует файл {filename}')


insert_data_to_sql(customers_data_file, 'customers')
insert_data_to_sql(employees_data_file, 'employees')
insert_data_to_sql(orders_data_file, 'orders')