import logging.config
import time
import psutil
import configparser
import pandas as pd
import sqlite3

##basic config
##logging.config.fileConfig('logging.conf')
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


#job parameters config
config = configparser.ConfigParser()
config.read('etlConfig.ini')
JobConfig = config['ETL_Log_Job']


formatter = logging.Formatter('%(levelname)s:  %(asctime)s:  %(process)s:  %(funcName)s:  %(message)s')
##creating handler
stream_handler = logging.StreamHandler()
file_handler = logging.FileHandler(JobConfig['LogName'])
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)


def extract():

    logger.info('Start Extract Session')
    logger.info('Source Filename: {}'.format(JobConfig['SrcObject']))

    try:
        df = pd.read_csv(JobConfig['SrcObject'])
        logger.info('Records count in source file: {}'.format(len(df.index)))
    except ValueError as e:
        logger.error(e)
        return
    logger.info("Read completed!!")
    return df

def transformation(tdf):
    try:
        tdf = pd.read_csv(JobConfig['SrcObject'])
        tdf[['fname', 'lname']] = tdf.NAME.str.split(expand=True)
        ndf = tdf[['ID', 'fname', 'lname', 'ADDRESS']]
        logger.info('Transformation completed, data ready to load!')
    except Exception as e:
        logger.error(e)
        return
    return ndf

def load(ldf):
    logger.info('Start Load Session')
    try:
        conn = sqlite3.connect(JobConfig['TgtConnection'])
        cursor = conn.cursor()
        logger.info('Connection to {} database established'.format(JobConfig['TgtConnection1']))
    except Exception as e:
        logger.error(e)
        return
    #3Load dataframe to table
    try:
        for index,row in ldf.iterrows():
            query = """INSERT OR REPLACE INTO {0}(id,fname,lname,address) VALUES('{1}','{2}','{3}','{4}')""".format(JobConfig['TgtObject'],row['ID'],row['fname'],row['lname'],row['ADDRESS'])
            cursor.execute(query)

    except Exception as e:
        logger.error(e)
        return
    conn.commit()
    logger.info("Data Loaded into target table: {}".format(JobConfig['TgtObject']))
    return

def main():

    start = time.time()

    ##extract
    start1 = time.time()
    tdf = extract()
    end1 = time.time() - start1
    logger.info('Extract CPU usage {}%'.format(psutil.cpu_percent()))
    logger.info("Extract function took : {} seconds".format(end1))

    ##transformation
    start2 = time.time()
    ldf = transformation(tdf)
    end2 = time.time() - start2
    logger.info('Transform CPU usage {}%'.format(psutil.cpu_percent()))
    logger.info("Transformation took : {} seconds".format(end2))

    ##load
    start3 = time.time()
    load(ldf)
    end3 = time.time() - start3
    logger.info('Load CPU usage {}%'.format(psutil.cpu_percent()))
    logger.info("Load took : {} seconds".format(end3))
    end = time.time() - start
    logger.info("ETL Job took : {} seconds".format(end))
    ##p = psutil.Process()
    ##ls = p.as_dict()
    ##print(p.as_dict())
    logger.info('Session Summary')
    logger.info('RAM memory {}% used:'.format(psutil.virtual_memory().percent))
    logger.info('CPU usage {}%'.format(psutil.cpu_percent()))
    print("multiple threads took : {} seconds".format(end))


if __name__=="__main__":
    logger.info('ETL Process Initialized')
    main()

