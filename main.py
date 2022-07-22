import sys
import os
import psycopg2

table_names = ['EMPLOYEE', 'DEPARTMENT']
attributes_dict = {
    "EMPLOYEE": ['id', 'name', 'salary', 'dept_id'],
    "DEPARTMENT": ['dept_id', 'dept_name', 'HOD']
}

hostname = 'localhost'
database = 'MiniProjectDB'
username = 'postgres'
pwd = 'arotq'
port_id = 5432

def prep_DB():

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

        cur.execute('DROP TABLE IF EXISTS employee')
        create_script = ''' CREATE TABLE IF NOT EXISTS employee (
                            id      int PRIMARY KEY,
                            name    varchar(40) NOT NULL,
                            salary  int,
                            dept_id varchar(30)) '''
        cur.execute(create_script)

        insert_script  = 'INSERT INTO employee (id, name, salary, dept_id) VALUES (%s, %s, %s, %s)'
        insert_values = [(6, 'James', 1200, 'D1'), (2, 'Robin', 1500, 'D2'), (3, 'John', 1700, 'D3')]
        for record in insert_values:
            cur.execute(insert_script, record)

        cur.execute('SELECT * FROM EMPLOYEE')
        for record in cur.fetchall():
            print(record)

        cur.execute('DROP TABLE IF EXISTS department')
        create_script = ''' CREATE TABLE IF NOT EXISTS department (
                            dept_id      varchar(30) PRIMARY KEY,
                            dept_name    varchar(40) NOT NULL,
                            HOD          varchar(30)) '''
        cur.execute(create_script)

        insert_script  = 'INSERT INTO department (dept_id, dept_name, HOD) VALUES (%s, %s, %s)'
        insert_values = [('D1', 'ECE', 'SNR'), ('D2', 'CSE', 'Jospeh'), ('D3', 'Mech', 'Kris')]
        for record in insert_values:
            cur.execute(insert_script, record)
        
        cur.execute('SELECT * FROM DEPARTMENT')
        for record in cur.fetchall():
            print(record)

        conn.commit()    
    except Exception as error:
        print(error)
    finally:
        if cur is not None:
            cur.close()
        if conn is not None:
            conn.close()

def display_title():
    
    asterisk_str = 85 * '*'
    newline_chars = '\n\n'
    summer_course_title = ' DBMS Summer Course Mini Project'
    main_title = newline_chars + asterisk_str + summer_course_title + asterisk_str
    print(main_title)

def user_options():
    
    print('\nCHOOSE ONE OF THE OPTIONS!.....\n')
    
    option_one = '1. To display all records of a table.'    
    option_two = '2. To add records to a table. '
    option_three = '3. To update a record in a table.'
    option_four = '4. To delete a record of a table.'
    exit_option = '0. To exit.\n'

    print(option_one)
    print(option_two)
    print(option_three)
    print(option_four)
    print(exit_option)

def get_user_option():
    return int(input())

def check_user_option(user_option, max_options):

    if user_option<0 or user_option>max_options:
        sys.exit("INVALID ENTRY!")

#display title and options. Gets user option.
def screen_display():
    
    display_title()
    user_options()
    user_option = get_user_option()
    max_options = 4
    check_user_option(user_option, max_options)
    return user_option

def display_table_options():
    os.system('cls')

    option_one = '1. Employee'
    option_two = '2. Department'

    print(option_one)
    print(option_two)

    display_table_option = int(input())
    max_options = 2
    check_user_option(display_table_option, max_options)
    return display_table_option

def display_table(display_table_option):
    table_name = table_names[display_table_option]

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
        
        cur_execute_str = 'SELECT * FROM ' + table_name
        cur.execute(cur_execute_str)

        for record in cur.fetchall():
            print(record)

        conn.commit()
    except Exception as error:
        print(error)
    finally:
        if cur is not None:
            cur.close()
        if conn is not None:
            conn.close()
        
def get_no_records():

    print('Enter no. of records to insert:')

    record_count = int(input())
    max_records = 3
    check_user_option(record_count, max_records)
    return record_count

def gen_insert_script(table_name):
    insert_script = 'INSERT INTO '
    insert_script = insert_script + table_name

    attributes = attributes_dict[table_name]
    attributes_max  = len(attributes)
    attributes_str = '('

    attributes_count = 0
    while(attributes_count<attributes_max):
        attributes_str = attributes_str + attributes[attributes_count]        

        if (attributes_count != (attributes_max-1)):
            attributes_str = attributes_str + ', '
        else:
            attributes_str = attributes_str + ') '
        attributes_count = attributes_count + 1
    attributes_str = attributes_str + 'VALUES ('

    attributes_count = 0
    while(attributes_count<attributes_max):
        attributes_str = attributes_str + '%s'
        if (attributes_count != (attributes_max-1)):
            attributes_str = attributes_str + ', '
        else:
            attributes_str = attributes_str + ') '
        attributes_count = attributes_count + 1

    return insert_script + attributes_str

    
def insert_record(display_table_option):

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

        table_name = table_names[display_table_option]
        insert_script = gen_insert_script(table_name)

        total_record_count = get_no_records()
        record_count = total_record_count

        total_attributes = len(attributes_dict[table_name])
        attr_count = total_attributes

        insert_value = []

        while(record_count):
            print('Enter Record-', record_count)
            insert_values = []
            while(attr_count):
                insert_value = input()
                insert_values.append(insert_value)            

                attr_count = attr_count - 1
            cur.execute(insert_script, insert_values)
            record_count = record_count - 1

        conn.commit()
    except Exception as error:
        print(error)
    finally:
        if cur is not None:
            cur.close()
        if conn is not None:
            conn.close()
            
def gen_update_script(display_table_option, attribute_name):

    table_name = table_names[display_table_option]
    table_attributes = attributes_dict[table_name]
    table_id_name = table_attributes[0]
    
    update_script = 'UPDATE ' + table_name
    update_script = update_script + ' SET '
    update_script = update_script + attribute_name
    update_script = update_script + ' = %s WHERE '
    
    update_script = update_script + table_id_name
    update_script = update_script + ' = %s'

    return update_script
        
def update_record(display_table_option):

    print('\nEnter the id of record to be updated....')
    record_id = input()
    
    print('\nEnter the attribute name to be updated....')
    attribute_name = str(input())

    print('\nEnter the value')
    attribute_val = input()

    update_script = gen_update_script(display_table_option, attribute_name)

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
        
        cur.execute(update_script, (attribute_val, record_id))

        conn.commit()
    except Exception as error:
        print(error)
    finally:
        if cur is not None:
            cur.close()
        if conn is not None:
            conn.close()
    

def gen_delete_script(display_table_option):
    table_name = table_names[display_table_option]
    table_attributes = attributes_dict[table_name]
    table_id_name = table_attributes[0]

    delete_script = 'DELETE FROM ' + table_name
    delete_script = delete_script + ' WHERE '
    delete_script = delete_script + table_id_name
    delete_script = delete_script + ' = %s'

    return delete_script


def delete_record(display_table_option):

    delete_script = gen_delete_script(display_table_option)

    print('\nEnter the record id to be deleted....')
    record_id = input()
    delete_record = (record_id,)

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
        
        cur.execute(delete_script, delete_record)

        conn.commit()
    except Exception as error:
        print(error)
    finally:
        if cur is not None:
            cur.close()
        if conn is not None:
            conn.close()

    
    
    
def exe_operation(user_option):
    if user_option == 1:
        display_table_option = display_table_options()
        display_table(display_table_option-1)
    if user_option == 2:
        display_table_option = display_table_options()
        insert_record(display_table_option-1)
    if user_option == 3:
        display_table_option = display_table_options()
        update_record(display_table_option-1)
    if user_option == 4:
        display_table_option = display_table_options()
        delete_record(display_table_option-1)
    if user_option == 0:
        return


# Connect to your postgres DB
def postgresDB():
    
    hostname = 'localhost'
    database = 'MiniProjectDB'
    username = 'postgres'
    pwd = 'arotq'
    port_id = 5432

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

        cur.execute('DROP TABLE IF EXISTS employee')
        create_script = ''' CREATE TABLE IF NOT EXISTS employee (
                            id      int PRIMARY KEY,
                            name    varchar(40) NOT NULL,
                            salary  int,
                            dept_id varchar(30)) '''
        cur.execute(create_script)

        insert_script  = 'INSERT INTO employee (id, name, salary, dept_id) VALUES (%s, %s, %s, %s)'
        insert_values = [(6, 'James', 1200, 'D1'), (2, 'Robin', 1500, 'D2'), (3, 'John', 1700, 'D3')]
        for record in insert_values:
            cur.execute(insert_script, record)

        cur.execute('SELECT * FROM EMPLOYEE')
        for record in cur.fetchall():
            print(record[1], record[2])
    

        conn.commit()    
    except Exception as error:
        print(error)
    finally:
        if cur is not None:
            cur.close()
        if conn is not None:
            conn.close()

#main
#prep_DB()
user_option = screen_display()
exe_operation(user_option)
#postgresDB()
