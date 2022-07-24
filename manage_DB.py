import sys
import os
import psycopg2

table_names = ['users_city', 'country', 'city', 'weather_status', 'weather_hourly_forecast_log', 'weather_daily_forecast_log']
attributes_dict = {
    "USERS_CITY": ['user_id', 'city_id', 'added_on'],
    "COUNTRY":                     ['id', 'country_name'],
    "CITY":                        ['id', 'city_name', 'city_longitude', 'city_latitude', 'zip', 'country_id'],
    "WEATHER_STATUS":              ['id', 'weather_st'],
    "WEATHER_HOURLY_FORECAST_LOG": ['id', 'city_id', 'start_timestamp', 'end_timestamp',
                                    'weather_status_id', 'temperature', 'feels_like_temperature',
                                    'humidity_in_percentage', 'wind_speed_in_mph',
                                    'wind_direction', 'pressure_in_mmhg', 'visibility_in_mph'],
    "WEATHER_DAILY_FORECAST_LOG":  ['city_id', 'calendar_date', 'weather_status_id',
                                    'min_temperature', 'max_temperature', 'avg_humidity_in_percentage',
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
                                    REFERENCES country(id)) '''
        cur.execute(create_script)

        create_script = ''' CREATE TABLE IF NOT EXISTS users_city (
                            user_id      int PRIMARY KEY,
                            city_id      int UNIQUE,
                            added_on     DATE NOT NULL,
                            CONSTRAINT fk_users_city
                                FOREIGN KEY(city_id)
                                references city(id)) '''
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
                            feels_like_temperature  int NOT NULL,
                            humidity_in_percentage  int NOT NULL,
                            wind_speed_in_mph       int NOT NULL,
                            wind_direction       varchar(2) NOT NULL,
                            pressure_in_mmhg        int NOT NULL,
                            visibility_in_mph       int NOT NULL,
                            CONSTRAINT fk_weather_hourly_forecast_log
                                FOREIGN KEY(city_id) REFERENCES city(id),
                                FOREIGN KEY(weather_status_id) REFERENCES weather_status(id))
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
                                FOREIGN KEY(city_id) REFERENCES city(id),
                                FOREIGN KEY(weather_status_id) REFERENCES weather_status(id))
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

def screen_display():
    os.system('cls')
    print('1. Delete existing DB')
    print('2. Add tables to DB')
    print('3. Populate DB with default values')
    print('\n\n "0"  to exit...')

def populate_defaults():
    print('pragya kindly fill this <3')

def user_input():
    while(1):
        user_option = int(input())
        if user_option == 1:
            delete_DB()
            screen_display()
        elif user_option == 2:
            add_DBtables()
            screen_display()
        elif user_option == 3:
            populate_defaults()
            screen_display()
        else:
            return

# main
screen_display()
user_input()
