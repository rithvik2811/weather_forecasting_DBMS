import sys
import os
import psycopg2

table_names = ['COUNTRY', 'CITY', 'USERS_CITY', 'WEATHER_STATUS',
               'WEATHER_HOURLY_FORECAST_LOG', 'WEATHER_DAILY_FORECAST_LOG']
attributes_dict = {
    "USERS_CITY":                   ['user_id', 'city_id', 'added_on'],
    "COUNTRY":                      ['id', 'country_name'],
    "CITY":                         ['id', 'city_name', 'city_longitude',
                                     'city_latitude', 'zip', 'country_id'],
    "WEATHER_STATUS":               ['id', 'weather_st'],
    "WEATHER_HOURLY_FORECAST_LOG":  ['id', 'city_id', 'start_timestamp', 'end_timestamp',
                                     'weather_status_id', 'temperature',
                                     'humidity_in_percentage', 'wind_speed_in_mph',
                                     'wind_direction', 'pressure_in_mmhg',
                                     'visibility_in_mph'],
    "WEATHER_DAILY_FORECAST_LOG":   ['city_id', 'calendar_date', 'weather_status_id',
                                     'min_temperature', 'max_temperature',
                                     'avg_humidity_in_percentage',
                                     'sunrise_time', 'sunset_time', 'source_system']
}

hostname = 'localhost'
database = 'MiniProjectDB'
username = 'postgres'
pwd = 'arotq'
port_id = 5432

def delete_DB():
    cur = None
    conn = None
    try:
        conn = psycopg2.connect(
            host = hostname,
            dbname = database,
            user = username,
            password = pwd,
            port = port_id)

        cur = conn.cursor()
        
        cur.execute('DROP TABLE IF EXISTS users_city CASCADE')
        cur.execute('DROP TABLE IF EXISTS country CASCADE')
        cur.execute('DROP TABLE IF EXISTS city CASCADE')
        cur.execute('DROP TABLE IF EXISTS weather_status CASCADE')
        cur.execute('DROP TABLE IF EXISTS weather_hourly_forecast_log CASCADE')
        cur.execute('DROP TABLE IF EXISTS weather_daily_forecast_log CASCADE')

        conn.commit()    
    except Exception as error:
        print(error)
    finally:
        if cur is not None:
            cur.close()
        if conn is not None:
            conn.close()       


    
def add_DBtables():

    cur = None
    conn = None
    try:
        conn = psycopg2.connect(
            host = hostname,
            dbname = database,
            user = username,
            password = pwd,
            port = port_id)

        cur = conn.cursor()

        create_script = ''' CREATE TABLE IF NOT EXISTS country (
                            id              int PRIMARY KEY,
                            country_name    varchar(100)) '''
        cur.execute(create_script)        


        create_script = ''' CREATE TABLE IF NOT EXISTS city (
                            id             int PRIMARY KEY,
                            city_name      varchar(100) NOT NULL,
                            city_longitude int NOT NULL,
                            city_latitude  int NOT NULL,
                            zip            varchar(10) NOT NULL,
                            country_id     int NOT NULL,
                            CONSTRAINT fk_city
                                FOREIGN KEY(country_id)
                                    REFERENCES country(id) ON DELETE CASCADE) '''
        cur.execute(create_script)

        create_script = ''' CREATE TABLE IF NOT EXISTS users_city (
                            user_id      int PRIMARY KEY,
                            city_id      int UNIQUE,
                            added_on     DATE NOT NULL,
                            CONSTRAINT fk_users_city
                                FOREIGN KEY(city_id)
                                references city(id) ON DELETE CASCADE) '''
        cur.execute(create_script)

        create_script = ''' CREATE TABLE IF NOT EXISTS weather_status (
                            id          int PRIMARY KEY,
                            weather_st  varchar(40)) '''
        cur.execute(create_script)

        create_script = ''' CREATE TABLE IF NOT EXISTS weather_hourly_forecast_log (
                            id                      int PRIMARY KEY,
                            city_id                 int NOT NULL,
                            start_timestamp         timestamp NOT NULL,
                            end_timestamp           timestamp NOT NULL,
                            weather_status_id       int NOT NULL,
                            temperature             int NOT NULL,
                            humidity_in_percentage  int NOT NULL,
                            wind_speed_in_mph       int NOT NULL,
                            wind_direction       varchar(2) NOT NULL,
                            pressure_in_mmhg        int NOT NULL,
                            visibility_in_mph       int NOT NULL,
                            CONSTRAINT fk_weather_hourly_forecast_log
                                FOREIGN KEY(city_id) REFERENCES city(id) ON DELETE CASCADE,
                                FOREIGN KEY(weather_status_id) REFERENCES weather_status(id)
                                ON DELETE CASCADE)
                            '''
        cur.execute(create_script)

        create_script = ''' CREATE TABLE IF NOT EXISTS weather_daily_forecast_log (
                            city_id                    int PRIMARY KEY,
                            calendar_date              date UNIQUE,
                            weather_status_id          int NOT NULL,
                            min_temperature            int NOT NULL,
                            max_temperature            int NOT NULL,
                            avg_humidity_in_percentage int NOT NULL,
                            sunrise_time               timestamp NOT NULL,
                            sunset_time                timestamp NOT NULL,
                            source_system              varchar(20) NOT NULL,
                            CONSTRAINT fk_weather_daily_forecast_log
                                FOREIGN KEY(city_id) REFERENCES city(id) ON DELETE CASCADE,
                                FOREIGN KEY(weather_status_id) REFERENCES weather_status(id)
                                ON DELETE CASCADE)
                            '''
        cur.execute(create_script) 
        
        conn.commit()    
    except Exception as error:
        print(error)
    finally:
        if cur is not None:
            cur.close()
        if conn is not None:
            conn.close()


def clear_screen():
    os.system('cls')

 
def screen_display():
    print('1. Delete existing DB')
    print('2. Add tables to DB')
    print('3. Populate DB with default values')
    print('\n\n "0"  to exit...')



def populate_users_city():
    cur = None
    conn = None
    try:
        conn = psycopg2.connect(
            host = hostname,
            dbname = database,
            user = username,
            password = pwd,
            port = port_id)

        cur = conn.cursor()

        insert_script  = '''INSERT INTO users_city (user_id, city_id, added_on)
                          VALUES (%s, %s, %s)'''
        insert_values = [(1, 1, '2000-01-18'), (2, 2, '2000-01-01'), (3, 3,
                                                                      '2000-01-03')]
        for record in insert_values:
            cur.execute(insert_script, record)
        
        conn.commit()    
    except Exception as error:
        print(error)
    finally:
        if cur is not None:
            cur.close()
        if conn is not None:
            conn.close()

# 1 - India
# 2 - Scotland
# 3 - US
def populate_country():
    cur = None
    conn = None
    try:
        conn = psycopg2.connect(
            host = hostname,
            dbname = database,
            user = username,
            password = pwd,
            port = port_id)

        cur = conn.cursor()

        insert_script  = 'INSERT INTO country (id, country_name) VALUES (%s, %s)'
        insert_values = [(1, 'India'), (2, 'Scotland'), (3, 'US')]
        for record in insert_values:
            cur.execute(insert_script, record)
        
        conn.commit()    
    except Exception as error:
        print(error)
    finally:
        if cur is not None:
            cur.close()
        if conn is not None:
            conn.close()

# 1 - Bangalore
# 2 - Delhi
# 3 - Mumbai
# 4 - California
# 5 - Las Vegas
def populate_city():
    cur = None
    conn = None
    try:
        conn = psycopg2.connect(
            host = hostname,
            dbname = database,
            user = username,
            password = pwd,
            port = port_id)

        cur = conn.cursor()

        insert_script  = '''INSERT INTO city (id, city_name, city_longitude,
                                              city_latitude, zip, country_id)
                                              VALUES (%s, %s, %s, %s, %s, %s)'''
        insert_values = [(1, 'Bangalore', 103, 29, '560103', 1),
                         (2, 'Delhi', 180, 106, '110003', 1),
                         (3, 'Mumbai', 89, 28, '400001', 1),
                         (4, 'Atlanta', 89, 28, '400001', 1),
                         (5, 'Las Vegas', 89, 28, '400001', 1)]
        for record in insert_values:
            cur.execute(insert_script, record)
        
        conn.commit()    
    except Exception as error:
        print(error)
    finally:
        if cur is not None:
            cur.close()
        if conn is not None:
            conn.close()    

def populate_weather_status():
    cur = None
    conn = None
    try:
        conn = psycopg2.connect(
            host = hostname,
            dbname = database,
            user = username,
            password = pwd,
            port = port_id)

        cur = conn.cursor()

        insert_script  = '''INSERT INTO weather_status (id, weather_st)
                          VALUES (%s, %s)'''
        insert_values = [(1, 'good'), (2, 'bad'), (3, 'average')]
        for record in insert_values:
            cur.execute(insert_script, record)
        
        conn.commit()    
    except Exception as error:
        print(error)
    finally:
        if cur is not None:
            cur.close()
        if conn is not None:
            conn.close()




def populate_weather_hourly_forecast_log():
    cur = None
    conn = None
    try:
        conn = psycopg2.connect(
            host = hostname,
            dbname = database,
            user = username,
            password = pwd,
            port = port_id)

        cur = conn.cursor()

        insert_script  = '''INSERT INTO weather_hourly_forecast_log (id, city_id,
                          start_timestamp, end_timestamp, weather_status_id,
                          temperature, humidity_in_percentage,
                          wind_speed_in_mph, wind_direction, pressure_in_mmhg,
                          visibility_in_mph)
                          VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'''
        
        insert_values = [(1, 1, '2004-10-19 10:23:54', '2004-10-20 10:23:54', 1,
                          26, 69, 12, 'E', 29, 4),
                         (2, 2, '2005-10-19 10:23:54', '2005-10-05 10:23:54', 2,
                          35, 40, 10, 'N', 32, 8),
                         (3, 3, '2004-01-19 10:23:54', '2004-01-20 10:23:54', 3,
                          28, 70, 18, 'W', 23, 10)]
        
        for record in insert_values:
            cur.execute(insert_script, record)
        
        conn.commit()    
    except Exception as error:
        print(error)
    finally:
        if cur is not None:
            cur.close()
        if conn is not None:
            conn.close()


def populate_weather_daily_forecast_log():
    cur = None
    conn = None
    try:
        conn = psycopg2.connect(
            host = hostname,
            dbname = database,
            user = username,
            password = pwd,
            port = port_id)

        cur = conn.cursor()

        insert_script  = '''INSERT INTO weather_daily_forecast_log (city_id,
                          calendar_date, weather_status_id, min_temperature,
                          max_temperature, avg_humidity_in_percentage,
                          sunrise_time, sunset_time, source_system)
                          VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)'''
        
        insert_values = [(1, '2004-10-19', 1, 18, 28, 60, '2004-10-19 05:30:00',
                          '2004-10-19 19:00:00', 'Dell'),
                         (2, '2004-10-29', 2, 26, 32, 65, '2004-10-29 06:30:00',
                          '2004-10-29 20:00:00', 'Cisco'),
                         (3, '2004-11-8', 3, 20, 32, 57, '2004-11-8 05:30:00',
                          '2004-11-8 20:15:00', 'HPE')]
        
        for record in insert_values:
            cur.execute(insert_script, record)
        
        conn.commit()    
    except Exception as error:
        print(error)
    finally:
        if cur is not None:
            cur.close()
        if conn is not None:
            conn.close()    
    

def populate_defaults():
    delete_DB()
    add_DBtables()

    populate_country()
    populate_city()
    populate_users_city()
    populate_weather_status()
    populate_weather_hourly_forecast_log()
    populate_weather_daily_forecast_log()
    

def user_input():
    while(1):
        user_option = int(input())
        if user_option == 1:
            delete_DB()
            clear_screen()
            print('Committed to DB successfully!\n\n')
            screen_display()
        elif user_option == 2:
            add_DBtables()
            clear_screen()
            print('Committed to DB successfully!\n\n')
            screen_display()
        elif user_option == 3:
            populate_defaults()
            clear_screen()
            print('Committed to DB successfully!\n\n')
            screen_display()
        else:
            return

def open_DBmanager_dialogue():
    print('Would you like to manage DB?(y/n)')
    open_DBmanager = input()
    if open_DBmanager == 'y' or open_DBmanager == 'Y':
        clear_screen()
        screen_display()
        user_input()
    elif open_DBmanager == 'n' or open_DBmanager == 'N':
        return
    else:
        sys.exit("INVALID ENTRY!")

# main
open_DBmanager_dialogue()
