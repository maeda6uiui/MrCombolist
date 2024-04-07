import argparse
import sqlite3
import yaml
from logging import getLogger,config
from pathlib import Path

def main(args):
    input_dirname:str=args.input_dirname
    input_filepaths:str=args.input_filepaths
    output_filepath:str=args.output_filepath
    remove_db_if_exists:bool=args.remove_db_if_exists

    #Set up logger
    with open("./logging_config.yaml","r",encoding="utf-8") as r:
        logging_config=yaml.safe_load(r)
    
    config.dictConfig(logging_config)

    logger=getLogger(__name__)
    logger.debug(args)

    #Get input files
    input_files:list[Path]=[]
    if input_filepaths is not None:
        lst_input_filepaths=input_filepaths.split(",")

        for input_filepath in lst_input_filepaths:
            input_file=Path(input_filepath)
            if not input_file.exists():
                logger.error(f"'{str(input_file)}' does not exist")
                return
            
            input_files.append(input_file)

        logger.info(f"{len(input_files)} files are specified as input")
    else:
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
                email STRING NOT NULL,
                poh STRING NOT NULL
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
                FROM tmpdb.personae
                WHERE email IS NOT NULL AND poh IS NOT NULL;
                """
            )
            conn.commit()

            #Detach temp DB
            cur.execute("DETACH tmpdb;")

    logger.info("Finished concatenating personae")

if __name__=="__main__":
    parser=argparse.ArgumentParser()
    parser.add_argument("-i","--input-dirname",type=str)
    parser.add_argument("-f","--input-filepaths",type=str)
    parser.add_argument("-o","--output-filepath",type=str)
    parser.add_argument("--remove-db-if-exists",action="store_true")
    args=parser.parse_args()

    main(args)
