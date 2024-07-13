import argparse
import sqlite3
import yaml
from logging import getLogger,config
from pathlib import Path

#Set up logger
with open("./logging_config.yaml","r",encoding="utf-8") as r:
    logging_config=yaml.safe_load(r)

config.dictConfig(logging_config)
logger=getLogger(__name__)

def fn_gather_local(input_dirname:str,output_filepath:str):
    #Get input files
    input_dir=Path(input_dirname)
    input_files=list(input_dir.glob("*.db"))
    input_files.sort()

    logger.info(f"{len(input_files)} files exist in the input directory")

    #Create table to gather local frequencies
    logger.info("Creating table to gather local frequencies...")
    with sqlite3.connect(output_filepath) as conn:
        cur=conn.cursor()

        cur.execute(
            """
            CREATE TABLE local_freqs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                word STRING NOT NULL,
                freq INTEGER NOT NULL
            );
            """
        )
        conn.commit()

    #Insert all records of the local frequency tables into the gathering table
    logger.info("Start gathering records from local frequency tables...")
    with sqlite3.connect(output_filepath) as conn:
        cur=conn.cursor()

        for input_file in input_files:
            logger.info(f"Processing '{input_file.name}'")

            #Attach temp DB
            cur.execute(f"ATTACH DATABASE '{str(input_file)}' AS tmpdb;")

            #Insert records
            cur.execute(
                """
                INSERT INTO local_freqs (word,freq)
                SELECT word,freq
                FROM tmpdb.freqs;
                """
            )
            conn.commit()

            #Detach temp DB
            cur.execute("DETACH tmpdb;")

    logger.info("Finished gathering records from local frequency tables")

def fn_merge_local(output_filepath:str):
    #Merge local frequencies
    logger.info("Start merging local frequencies...")
    with sqlite3.connect(output_filepath) as conn:
        cur=conn.cursor()

        #Create table to merge frequencies
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

        #Consolidate local frequencies
        cur.execute(
            """
            INSERT INTO freqs (word,freq)
            SELECT word,SUM(freq)
            FROM local_freqs
            GROUP BY word;
            """
        )
        conn.commit()

        #Remove table for local frequencies
        cur.execute("DROP TABLE local_freqs;")
        conn.commit()

    logger.info("Finished merging local frequencies")

def main(args):
    input_dirname:str=args.input_dirname
    output_filepath:str=args.output_filepath
    gather_local:bool=args.gather_local
    merge_local:bool=args.merge_local

    logger.debug(args)

    if gather_local:
        fn_gather_local(input_dirname,output_filepath)
    if merge_local:
        fn_merge_local(output_filepath)

if __name__=="__main__":
    parser=argparse.ArgumentParser()
    parser.add_argument("-i","--input-dirname",type=str)
    parser.add_argument("-o","--output-filepath",type=str)
    parser.add_argument("--gather-local",action="store_true")
    parser.add_argument("--merge-local",action="store_true")
    args=parser.parse_args()

    main(args)
