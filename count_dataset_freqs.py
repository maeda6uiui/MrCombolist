import argparse
import sqlite3
import yaml
from logging import getLogger,config
from pathlib import Path

def main(args):
    input_dirname:str=args.input_dirname
    db_filepath:str=args.db_filepath
    remove_db_if_exists:bool=args.remove_db_if_exists
    start_index:int=args.start_index
    end_index:int=args.end_index
    
    #Set up logger
    with open("./logging_config.yaml","r",encoding="utf-8") as r:
        logging_config=yaml.safe_load(r)
    
    config.dictConfig(logging_config)

    logger=getLogger(__name__)
    logger.debug(args)

    #Get all db files in the input directory
    input_dir=Path(input_dirname)
    input_files=list(input_dir.glob("*.db"))

    logger.info(f"{len(input_files)} files exist in the input directory")

    #Create a subset of the list if either the start or the end index is specified
    start_index=start_index if start_index is not None else 0
    end_index=end_index if end_index is not None else len(input_files)

    input_files=input_files[start_index:end_index]

    #Remove DB if already exists
    db_file=Path(db_filepath)
    if remove_db_if_exists and db_file.exists():
        db_file.unlink()
        logger.info(f"DB file '{db_file.name}' was removed")

    #Create table to gather local frequencies
    logger.info("Creating table to gather local frequencies...")
    with sqlite3.connect(db_filepath) as conn:
        cur=conn.cursor()

        cur.execute(
            """
            CREATE TABLE freqs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                word STRING NOT NULL,
                freq INTEGER NOT NULL
            );
            """
        )
        conn.commit()

    #Insert all records of the local frequency tables into the gathering table
    logger.info("Start gathering records from local frequency tables...")
    with sqlite3.connect(db_filepath) as conn:
        cur=conn.cursor()

        for input_file in input_files:
            logger.info(f"Processing '{input_file.name}'")

            #Attach temp DB
            cur.execute(f"ATTACH DATABASE '{str(input_file)}' AS tmpdb;")

            #Insert records
            cur.execute(
                """
                INSERT INTO freqs (word,freq)
                SELECT word,freq
                FROM tmpdb.freqs;
                """
            )
            conn.commit()

            #Detach temp DB
            cur.execute("DETACH tmpdb;")

    logger.info("Finished gathering records from local frequency tables")

    #Count dataset frequencies
    logger.info("Start counting dataset frequencies...")
    with sqlite3.connect(db_filepath) as conn:
        cur=conn.cursor()

        #Create table for dataset frequencies
        cur.execute(
            """
            CREATE TABLE dataset_freqs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                word STRING NOT NULL,
                freq INTEGER NOT NULL
            );
            """
        )
        conn.commit()

        #Consolidate local frequencies
        cur.execute(
            """
            INSERT INTO dataset_freqs (word,freq)
            SELECT word,SUM(freq)
            FROM freqs
            GROUP BY word;
            """
        )
        conn.commit()

        #Remove table for local frequencies
        cur.execute("DROP TABLE freqs;")
        conn.commit()

    logger.info("Finished counting dataset frequencies")

if __name__=="__main__":
    parser=argparse.ArgumentParser()
    parser.add_argument("-i","--input-dirname",type=str)
    parser.add_argument("-o","--db-filepath",type=str)
    parser.add_argument("--remove-db-if-exists",action="store_true")
    parser.add_argument("--start-index",type=int)
    parser.add_argument("--end-index",type=int)
    args=parser.parse_args()

    main(args)
