import argparse
import pandas as pd
import sqlite3
import yaml
from logging import getLogger,config
from pathlib import Path

def main(args):
    input_dirname:str=args.input_dirname
    output_dirname:str=args.output_dirname
    email_freqs_db:bool=args.email_freqs_db
    poh_freqs_db:bool=args.poh_freqs_db
    personae_db:bool=args.personae_db
    start_index:int=args.start_index
    end_index:int=args.end_index

    #Set up logger
    with open("./logging_config.yaml","r",encoding="utf-8") as r:
        logging_config=yaml.safe_load(r)
    
    config.dictConfig(logging_config)

    logger=getLogger(__name__)
    logger.debug(args)

    #Get all parquet files in the input directory
    input_dir=Path(input_dirname)
    input_files=list(input_dir.glob("*.parquet"))
    input_files.sort()

    logger.info(f"{len(input_files)} files exist in the input directory")

    #Create output directory
    output_dir=Path(output_dirname)
    output_dir.mkdir(exist_ok=True,parents=True)

    #Create a subset of the list if either the start or the end index is specified
    start_index=start_index if start_index is not None else 0
    end_index=end_index if end_index is not None else len(input_files)

    input_files=input_files[start_index:end_index]

    #Determine value column name and table name
    val_column_name=""
    table_name=""

    if email_freqs_db:
        val_column_name="email"
        table_name="freqs"
    elif poh_freqs_db:
        val_column_name="poh"
        table_name="freqs"
    elif personae_db:
        table_name="personae"
    else:
        logger.error("Must specify the type of DB to be created")
        return

    #Convert to sql
    logger.info("Start converting to sql...")
    for input_file in input_files:
        logger.info(f"Processing '{input_file.name}'")

        #Load input file
        df=pd.read_parquet(input_file)

        #Rename value column to "word"
        if val_column_name!="":
            df.rename(columns={val_column_name: "word"},inplace=True)

        #Create connection and output to sql
        db_file=output_dir.joinpath(f"{input_file.stem}.db")
        with sqlite3.connect(db_file) as conn:
            df.to_sql(table_name,conn,if_exists="replace")

    logger.info("Finished converting to sql")

if __name__=="__main__":
    parser=argparse.ArgumentParser()
    parser.add_argument("-i","--input-dirname",type=str)
    parser.add_argument("-o","--output-dirname",type=str)
    parser.add_argument("--email-freqs-db",action="store_true")
    parser.add_argument("--poh-freqs-db",action="store_true")
    parser.add_argument("--personae-db",action="store_true")
    parser.add_argument("--start-index",type=int)
    parser.add_argument("--end-index",type=int)
    args=parser.parse_args()

    main(args)
