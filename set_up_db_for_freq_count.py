import argparse
import sqlite3
import yaml
from logging import getLogger,config

def main(args):
    db_filepath:str=args.db_filepath
    word_list_filepath:str=args.word_list_filepath
    num_records_to_commit:int=args.num_records_to_commit

    #Set up logger
    with open("./logging_config.yaml","r",encoding="utf-8") as r:
        logging_config=yaml.safe_load(r)
    
    config.dictConfig(logging_config)

    logger=getLogger(__name__)
    logger.debug(args)

    #Set up DB
    logger.info("Start setting up DB...")

    conn=sqlite3.connect(db_filepath)
    cur=conn.cursor()

    cur.execute(
        "CREATE TABLE frequencies ("
            "id INTEGER PRIMARY KEY AUTOINCREMENT,"
            "word STRING NOT NULL,"
            "frequency INTEGER NOT NULL"
        ")"
    )
    conn.commit()

    with open(word_list_filepath,"r",encoding="utf-8") as r:
        for idx,line in enumerate(r):
            word=line.strip()
            cur.execute("INSERT INTO frequencies (word,frequency) values(?,0)",(word,))

            if (idx+1)%num_records_to_commit==0:
                conn.commit()
                logger.info(f"{(idx+1)/1000000}M records inserted")

    conn.commit()
    conn.close()

    logger.info("Finished setting up DB")

if __name__=="__main__":
    parser=argparse.ArgumentParser()
    parser.add_argument("-o","--db-filepath",type=str)
    parser.add_argument("-l","--word-list-filepath",type=str)
    parser.add_argument("-n","--num-records-to-commit",type=int,default=1000000)
    args=parser.parse_args()

    main(args)
