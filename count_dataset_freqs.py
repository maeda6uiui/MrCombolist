import argparse
import sqlite3
import yaml
from logging import getLogger,config
from pathlib import Path

def main(args):
    input_dirname:str=args.input_dirname
    db_filepath:str=args.db_filepath
    remove_db_if_exists:bool=args.remove_db_if_exists
    
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

    #Remove DB if already exists
    db_file=Path(db_filepath)
    if remove_db_if_exists and db_file.exists():
        db_file.unlink()
        logger.info(f"DB file '{db_file.name}' was removed")

    #Set up DB
    logger.info("Start setting up DB...")

    conn=sqlite3.connect(db_filepath)
    cur=conn.cursor()

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

    #Count dataset frequencies
    logger.info("Start counting dataset frequencies...")
    for input_file in input_files:
        logger.info(f"Processing '{input_file.name}'")

        #Attach temp DB
        cur.execute(f"ATTACH DATABASE '{str(input_file)}' AS tmpdb;")

        #Update records if exists
        cur.execute(
            """
            UPDATE dataset_freqs SET freq=freq+COALESCE(
                (
                    SELECT tmpdb.freqs.freq
                    FROM tmpdb.freqs
                    WHERE tmpdb.freqs.word=dataset_freqs.word
                ),0
            )
            WHERE EXISTS (
                SELECT 1
                FROM dataset_freqs,tmpdb.freqs
                WHERE dataset_freqs.word=tmpdb.freqs.word
            );
            """
        )
        conn.commit()

        #Insert records if not exist
        cur.execute(
            """
            INSERT INTO dataset_freqs (word,freq)
            SELECT word,freq
            FROM tmpdb.freqs
            WHERE NOT EXISTS (
                SELECT 1
                FROM dataset_freqs
                WHERE dataset_freqs.word=tmpdb.freqs.word
            );
            """
        )
        conn.commit()

        #Detach temp DB
        cur.execute("DETACH tmpdb;")

    conn.close()

    logger.info("Finished counting dataset frequencies")

if __name__=="__main__":
    parser=argparse.ArgumentParser()
    parser.add_argument("-i","--input-dirname",type=str)
    parser.add_argument("-o","--db-filepath",type=str)
    parser.add_argument("--remove-db-if-exists",action="store_true")
    args=parser.parse_args()

    main(args)
