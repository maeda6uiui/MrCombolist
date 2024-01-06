import argparse
import gzip
import sqlite3
import yaml
from logging import getLogger,config,Logger
from pathlib import Path

def print_records(db_filepath:str,query:str,max_num_records_to_print:int):
    with sqlite3.connect(db_filepath) as conn:
        cur=conn.cursor()
        cur.execute(query)
        rows=cur.fetchmany(max_num_records_to_print)
        for row in rows:
            print(row)

def export_records(
        db_filepath:str,
        query:str,
        output_filepath:str,
        num_records_for_logging:int,
        logger:Logger):
    #Create output directory
    logger.info("Creating output directory...")

    output_file=Path(output_filepath)
    output_dir=output_file.parent
    output_dir.mkdir(exist_ok=True,parents=True)

    #Execute query
    logger.info("Executing query...")

    conn=sqlite3.connect(db_filepath)
    cur=conn.cursor()
    cur.execute(query)

    #Fetch one row at a time and write it to a gzipped TSV file
    logger.info("Start exporting records to a file...")
    with gzip.open(output_filepath,"wt",encoding="utf-8") as wt:
        row=cur.fetchone()
        num_records_exported=0
        while row is not None:
            line="\t".join(map(str,row))
            wt.write(f"{line}\n")

            num_records_exported+=1
            if num_records_exported%num_records_for_logging==0:
                logger.info(f"{num_records_exported/1000000}M records exported")

            row=cur.fetchone()

    conn.close()

    logger.info("Finished exporting records to a file")

def main(args):
    db_filepath:str=args.db_filepath
    output_filepath:str=args.output_filepath
    query:str=args.query
    max_num_records_to_print:int=args.max_num_records_to_print
    num_records_for_logging:int=args.num_records_for_logging

    #Set up logger
    with open("./logging_config.yaml","r",encoding="utf-8") as r:
        logging_config=yaml.safe_load(r)
    
    config.dictConfig(logging_config)

    logger=getLogger(__name__)
    logger.debug(args)

    #Check if DB file exists
    db_file=Path(db_filepath)
    if not db_file.exists():
        logger.error(f"DB file '{db_file.name}' does not exist")
        return

    #Print query result
    if output_filepath is None:
        print_records(db_filepath,query,max_num_records_to_print)
    #Export to a gzipped TSV file
    else:
        export_records(db_filepath,query,output_filepath,num_records_for_logging,logger)

if __name__=="__main__":
    parser=argparse.ArgumentParser()
    parser.add_argument("-i","--db-filepath",type=str)
    parser.add_argument("-o","--output-filepath",type=str)
    parser.add_argument("-q","--query",type=str)
    parser.add_argument("--max-num-records-to-print",type=int,default=10)
    parser.add_argument("--num-records-for-logging",type=int,default=1000000)
    args=parser.parse_args()

    main(args)
