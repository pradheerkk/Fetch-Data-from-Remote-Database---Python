import csv
import logging
import math
import os
import pathlib
import threading
from datetime import date, datetime, timedelta
from logging.handlers import RotatingFileHandler

import psycopg2
import psycopg2.extensions
from psycopg2.extras import LoggingConnection


class Example:
    def __init__(self):
        logger.info("Started: " + str(datetime.now()))

    def to_csv(self, connection):
        connection.initialize(logger)
        end_date = date.today().isoformat()
        start_date = (datetime.now() + timedelta(-30)).isoformat()
        try:
            connection.set_session(readonly=True, isolation_level="READ UNCOMMITTED", autocommit=False)
            cursor = connection.cursor(name="my_cursor_name")
            cursor.itersize = 20000
            flag = True
            args = [start_date, end_date]
            if flag is False:
                returnargs = connection.callproc(connection, 'Procedure_Name', args)
                logger.info("Proc is :: '+'('+str(start_date)+','+str(end_date)+')")
            else:
                assets = []
                query = 'select* from Table_Name;'
                cursor.execute(query)
                logger.info("Query :" + query)
                while True:
                    rows = cursor.fetchmany(5000)
                    if not rows:
                        break
                    for row in rows:
                        assets.append(row[0])
                asset_tuples = tuple(assets)
                date_of_folder = (date.today() + timedelta(-1)).isoformat()
                path_of_folder = os.path.join('Data', date_of_folder)
                if pathlib.Path(path_of_folder).exists() is False:
                    os.makedirs(path_of_folder)
                logger.info("Creating Threads::Total number of AssetIds :: " + str(len(asset_tuples)))
                threadcount = 1
                first = 0
                last = 10000
                if len(asset_tuples) > 0:
                    threads = list()
                    for index in range(math.ceil(len(asset_tuples) / 10000)):
                        t1 = threading.Thread(target=self.write_to_csv, args=(
                            asset_tuples[first:last], 'Thread_' + str(threadcount), path_of_folder, connection))
                        t1.start()
                        threadcount = threadcount + 1
                        if last + 10000 < len(asset_tuples):
                            first = last
                            last = first + 10000
                        else:
                            first = last
                            last = len(asset_tuples)
                        threads.append(t1)
                    logger.info(str(len(threads)) + " Threads Created. Waiting for them to execute ")
                    for index, thread in enumerate(threads):
                        thread.join()
                else:
                    logger.info("There is nothing to do")
        except(Exception) as error:
            logger.error("Exception while getting asset Data or executing proc", exc_info=True)
        finally:
            if connection is not None:
                connection.close()
                logger.info("Connection is closed")

    def write_to_csv(self, asset_tuple, threadname, pathname, connection):
        try:
            start_time = datetime.now()
            connection.initialize(logger)
            path_of_file = os.path.join(pathname, 'Folder_Name' + str(threadname) + '.csv')
            connection = psycopg2.connect(dbname="xxxxx", user="xxxxx", password="xxxxx", host="xx.xx.xx.xx")
            connection.set_session(readonly=True, isolation_level="READ UNCOMMITTED", autocommit=False)
            cursor = connection.cursor(name="my_cursor_name")
            cursor.itersize = 20000
            logger.info("Starting Time of " + threadname + " is :: %s" + str(start_time))
            columns = [] #give a list of column names
            query = 'select * from table_name'
            cursor.execute(
                'select * from table_name'
            logger.info("Query::" + query)
            logger.info("Path :::" + str(pathname))
            has_header = False
            files = 1
            while True:
                rows = cursor.fetchmany(5000)
                if not rows:
                    break
                for row in rows:
                    with open(path_of_file, 'a', newline='') as f:
                        writer = csv.writer(f)
                        if has_header is False:
                            writer.writerow(columns)
                            has_header = True
                        writer.writerow(row)
                        statinfo = os.stat(path_of_file)
                        if statinfo.st_size > 2147483648:
                            path_of_file_new = os.path.join(pathname,
                                                            'Folder_name' + threadname + "-" + str(
                                                                files) + '.csv')
                            path_of_file = path_of_file_new
                            has_header = False
                            files = files + 1
                        f.close()
            end_time = datetime.now()
            logger.info("Ending time of " + threadname + "::" + str(end_time - start_time))
        except(Exception) as error:
            logger.error("Exception while getting the data", exc_info=True)
        finally:
            if connection is not None:
                connection.close()


if __name__ == '__main__':
    logger = logging.getLogger('Example')
    logger.setLevel(logging.INFO)
    handler = RotatingFileHandler('logger.log', maxBytes=5000000, backupCount=10)
    formatter = logging.Formatter('%(asctime)s : %(levelname)s : %(name)s : %(funcName)s :  %(message)s ')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    connection = psycopg2.connect(connection_factory=LoggingConnection, dbname="xxxx", user="xxxxx",
                                  password="xxxxx", host="xx.xx.xx.xxx")
    ex = Example()
    ex.to_csv(connection)
