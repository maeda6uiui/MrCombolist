import argparse
import json
import yaml
import pandas as pd
from logging import getLogger,config
from pathlib import Path
from tqdm import tqdm

def main(args):
    input_dirname:str=args.input_dirname
    output_filepath:str=args.output_filepath

    #Set up logger
    with open("./logging_config.yaml","r",encoding="utf-8") as r:
        logging_config=yaml.safe_load(r)
    
    config.dictConfig(logging_config)

    logger=getLogger(__name__)
    logger.debug(args)

    #Get all parquet files in the input directory
    input_dir=Path(input_dirname)
    input_files=list(input_dir.glob("*.parquet"))

    logger.info(f"{len(input_files)} files exist in the input directory")

    #Count
    email_total_counts:dict[str,int]={}
    email_unique_counts:dict[str,int]={}
    poh_total_counts:dict[str,int]={}
    poh_unique_counts:dict[str,int]={}

    logger.info("Start counting the number of records...")
    for input_file in tqdm(input_files):
        df=pd.read_parquet(input_file)

        total_counts=df.count()
        email_total_count=total_counts["email"].item()
        poh_total_count=total_counts["poh"].item()

        email_unique_count=df["email"].nunique()
        poh_unique_count=df["poh"].nunique()

        email_total_counts[input_file.name]=email_total_count
        email_unique_counts[input_file.name]=email_unique_count
        poh_total_counts[input_file.name]=poh_total_count
        poh_unique_counts[input_file.name]=poh_unique_count

    count_result={
        "email": {
            "total": sum(email_total_counts.values())
        },
        "poh": {
            "total": sum(poh_total_counts.values())
        },
        "dataframes": [
            {
                "filename": filename,
                "email": {
                    "total": email_total_counts[filename],
                    "unique": email_unique_counts[filename]
                },
                "poh": {
                    "total": poh_total_counts[filename],
                    "unique": poh_unique_counts[filename]
                }
            } for filename in email_total_counts.keys()
        ]
    }

    if output_filepath is None:
        print(count_result)
    else:
        with open(output_filepath,"w",encoding="utf-8") as w:
            json.dump(count_result,w,ensure_ascii=False)

    logger.info("Finished counting the number of records")

if __name__=="__main__":
    parser=argparse.ArgumentParser()
    parser.add_argument("-i","--input-dirname",type=str)
    parser.add_argument("-o","--output-filepath",type=str)
    args=parser.parse_args()

    main(args)
