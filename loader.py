import zipfile
import re
import json
import psycopg2 as pg
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import time


DATA_PATH = '/tmp/data/data.zip'
DB_NAME = 'tank1'
USER_NAME = 'postgres'
PASSWORD = 'qwerty'


def load_data_user(conn, cur, data):
    load_slice = []
    for item in data:
        load_slice.append((item.get('id'), item.get('first_name'), item.get('last_name'), item.get('birth_date'), item.get('gender'), item.get('email')))
    sql = 'INSERT INTO users (id, first_name, last_name, birth_date, gender, email) VALUES (%s,%s,%s,%s,%s,%s)'
    cur.executemany(sql, load_slice)
    conn.commit()


def load_data_locations(conn, cur, data):
    load_slice = []
    for item in data:
        load_slice.append((item.get('id'), item.get('place'), item.get('city'), item.get('distance'), item.get('country')))
    sql = 'INSERT INTO locations (id, place, city, distance, country) VALUES (%s,%s,%s,%s,%s)'
    cur.executemany(sql, load_slice)
    conn.commit()


def load_data_visits(conn, cur, data):
    load_slice = []
    for item in data:
        load_slice.append((item.get('id'), item.get('mark'), item.get('user'), item.get('location'), item.get('visited_at')))
    sql = 'INSERT INTO visits (id, mark, user_id, location_id, visited_at) VALUES (%s,%s,%s,%s,%s)'
    cur.executemany(sql, load_slice)
    conn.commit()


execs = [
    {'pattern': r'users_\d?[\d].json', 'obj': 'users', 'func': load_data_user},
    {'pattern': r'locations_\d?[\d].json', 'obj': 'locations', 'func': load_data_locations},
    {'pattern': r'visits_\d?[\d].json', 'obj': 'visits', 'func': load_data_visits}
]


def load_data(conn):
    cur = conn.cursor()
    with zipfile.ZipFile(DATA_PATH, 'r') as zip_ref:
        for filename in zip_ref.namelist():
            for ex_item in execs:
                if re.match(ex_item['pattern'], filename):
                    t0 = time.perf_counter()
                    content = zip_ref.read(filename).decode('utf-8')
                    data = json.loads(content).get(ex_item['obj'], [])
                    ex_item['func'](conn, cur, data)
                    print('Load {} by {}'.format(filename, time.perf_counter() - t0))


def create_base(db_name):
    conn = pg.connect(dbname='postgres', user=USER_NAME, host='localhost', password=PASSWORD)
    conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    try:
        sql = 'create database %s;' % db_name
        cur = conn.cursor()
        try:
            cur.execute(sql)
        except pg.DatabaseError as E:
            print(str(E))
    finally:
        conn.close()


def create_table_users(conn):
    cur = conn.cursor()

    sql = """
      create table IF NOT EXISTS users(
        id integer PRIMARY KEY, 
        first_name varchar(255), 
        last_name varchar(255), 
        birth_date integer, 
        gender char(1), 
        email varchar(255)) 
    """
    cur.execute(sql)
    conn.commit()


def create_table_location(conn):
    cur = conn.cursor()
    sql = """
        create table IF NOT EXISTS locations(
          id integer PRIMARY KEY,
          place varchar (255),
          city varchar (255),
          distance integer,
          country varchar (255)
        )
    """
    cur.execute(sql)
    conn.commit()


def create_table_visits(conn):
    cur = conn.cursor()
    sql = """
        create table IF NOT EXISTS visits(
          id integer primary key,
          mark smallint,
          user_id integer,
          location_id integer,
          visited_at integer 
        )    
    """
    cur.execute(sql)
    conn.commit()


def clear_table(conn):
    cur = conn.cursor()
    sql = 'delete from users;'
    cur.execute(sql)
    sql = 'delete from locations;'
    cur.execute(sql)
    sql = 'delete from visits;'
    cur.execute(sql)
    conn.commit()


def create_indexes(conn):

    t0 = time.perf_counter()
    cur = conn.cursor()
    sql = 'create index idx_user_01 on users (birth_date);'
    cur.execute(sql)

    sql = 'create index idx_visits_01 on visits (user_id);'
    cur.execute(sql)

    sql = 'create index idx_visits_02 on visits (location_id);'
    cur.execute(sql)

    sql = 'create index idx_visits_03 on visits (visited_at);'
    cur.execute(sql)

    sql = 'create index idx_location_01 on locations (country);'
    cur.execute(sql)

    print('Indexes created by {}'.format(time.perf_counter() - t0))


def create_tables(conn):

    t0 = time.perf_counter()
    create_table_users(conn)
    create_table_location(conn)
    create_table_visits(conn)
    print('Tables created by {}'.format(time.perf_counter() - t0))


if __name__ == '__main__':

    # create_base(DB_NAME)

    conn = pg.connect(dbname=DB_NAME, user=USER_NAME, host='localhost', password=PASSWORD)

    # create_tables(conn)
    # create_indexes(conn)

    clear_table(conn)

    load_data(conn)

    conn.close()

