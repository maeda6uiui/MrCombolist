import argparse
import sqlite3
import yaml
from logging import getLogger,config
from pathlib import Path

def main(args):
    input_dirname:str=args.input_dirname
    output_filepath:str=args.output_filepath
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
    output_file=Path(output_filepath)
    if remove_db_if_exists and output_file.exists():
        output_file.unlink()
        logger.info(f"DB file '{output_file.name}' was removed")

    #Concatenate personae
    logger.info("Start concatenating personae...")
    with sqlite3.connect(output_filepath) as conn:
        cur=conn.cursor()

        #Create table to concatenate personae
        cur.execute(
            """
            CREATE TABLE personae (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                email STRING,
                poh STRING
            );
            """
        )
        conn.commit()

        #Insert records from each DB
        for input_file in input_files:
            logger.info(f"Processing '{input_file.name}'")

            #Attach as temp DB
            cur.execute(f"ATTACH DATABASE '{str(input_file)}' AS tmpdb;")

            #Insert records
            cur.execute(
                """
                INSERT INTO personae (email,poh)
                SELECT email,poh
                FROM tmpdb.personae;
                """
            )
            conn.commit()

            #Detach temp DB
            cur.execute("DETACH tmpdb;")

    logger.info("Finished concatenating personae")

if __name__=="__main__":
    parser=argparse.ArgumentParser()
    parser.add_argument("-i","--input-dirname",type=str)
    parser.add_argument("-o","--output-filepath",type=str)
    parser.add_argument("--remove-db-if-exists",action="store_true")
    args=parser.parse_args()

    main(args)
