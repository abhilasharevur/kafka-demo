from configparser import ConfigParser
import psycopg2
import logging
import inspect, os

logging.basicConfig(
    format='%(asctime)s.%(msecs)s:%(name)s:%(thread)d:%(levelname)s:%(process)d:%(message)s',
    level=logging.INFO
)

log = logging.getLogger(__name__)

cur_dir = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
print(cur_dir)


def config(filename='database.ini', section='postgresql'):
    # create a parser
    parser = ConfigParser()
    # read config file
    config_file = os.path.join(cur_dir, filename)
    parser.read(config_file)

    # get section, default to postgresql
    db = {}
    if parser.has_section(section):
        params = parser.items(section)
        for param in params:
            # print("param 0: ", param[0], param[1])
            db[param[0]] = param[1]
    else:
        raise Exception('Section {0} not found in the {1} file'.format(section, filename))

    return db


def connect():
    """ Connect to the PostgreSQL database"""

    db_conn = None
    try:
        # get the connection parameters
        conn_params = config()

        # make connection to the PostgreSQL server
        db_conn = psycopg2.connect(**conn_params)

        # create cursor
        cursor = db_conn.cursor()

        # print connection properties
        print(db_conn.get_dsn_parameters())

        # print PostgreSQL version
        cursor.execute("SELECT version();")
        db_version = cursor.fetchone()
        print("DB Version: ", db_version)

        cursor.execute("SELECT current_database()")
        result = cursor.fetchone()
        print("Connected to: {}".format(result[0]))

        return cursor, db_conn

    except Exception as error:
        log.error(error)
        print(error)


def insert(fake_data, table_name=None, insert_qry=None):
    d_conn = None
    try:
        d_cursor, d_conn = connect()
        if not table_name:
            table_name = "orders_03"

        # Create table if not exists
        create_cmd = ("""
        CREATE TABLE {} (
            id SERIAL NOT NULL PRIMARY KEY,
            customer VARCHAR(255) NOT NULL,
            product VARCHAR(255) NOT NULL,
            quant SMALLINT NOT NULL
        )
        """.format(table_name))

        # d_cursor.execute(create_cmd)
        # d_conn.commit()

        # Enter the data into the table
        cmd = (
            """
            INSERT INTO {}(customer, product, quant)
            VALUES(%s, %s, %s)
            RETURNING *
            """.format(table_name)
        )
        if insert_qry:
            cmd = insert_qry
        print("Exceuting insert command:\n", cmd)
        print("inserting into the table")
        # print(fake_data)
        d_cursor.executemany(cmd, fake_data)
        print("Number of rows inserted into {} is {}".format(table_name, len(fake_data)))
        d_conn.commit()
    except Exception as e:
        log.error(e)
        print("Exception: ", e)
    finally:
        # close the cursor and the database connection
        if d_conn:
            d_cursor.close()
            d_conn.close()
            print("Postgresql connection closed")


def query(query_cmd):
    q_conn = None
    try:
        select_qry = query_cmd
        # Check for connection
        q_cursor, q_conn = connect()

        print("Executing query:\n", select_qry)
        q_cursor.execute(select_qry)
        res = q_cursor.fetchall()
        print(len(res))
        # for record in res:
        #     print("ID:", record[0])
        #     print("Customer: ", record[1])
        #     print("Product: ", record[2])
        #     print("Quantity: ", record[3])
    except Exception as e:
        log.error(e)
        print("Exception: ", e)
    finally:
        # close the cursor and the database connection
        if q_conn:
            q_cursor.close()
            q_conn.close()
            print("Postgresql connection closed")


if __name__ == "__main__":
    # data = [('John Henry', 'Diaper', 7), ('Kristine Morgan', 'Toy Train', 7), ('Alexandra Brown', 'Beer', 4),
    #        ('Kayla Lewis', 'Toy Train', 2), ('Stephanie Porter', 'Toy Car', 6)]
    # insert(data)

    qry = "select * from orders_03;"
    query(qry)
