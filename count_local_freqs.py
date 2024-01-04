import argparse
import pandas as pd
import yaml
from logging import getLogger,config
from pathlib import Path

def main(args):
    input_dirname:str=args.input_dirname
    output_root_dirname:str=args.output_root_dirname
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

    logger.info(f"{len(input_files)} files exist in the input directory")

    #Create output directories
    output_root_dir=Path(output_root_dirname)
    output_root_dir.mkdir(exist_ok=True,parents=True)

    email_output_dir=output_root_dir.joinpath("Email")
    email_output_dir.mkdir(exist_ok=True)

    poh_output_dir=output_root_dir.joinpath("POH")
    poh_output_dir.mkdir(exist_ok=True)

    #Create a subset of the list if either the start or the end index is specified
    start_index=start_index if start_index is not None else 0
    end_index=end_index if end_index is not None else len(input_files)

    input_files=input_files[start_index:end_index]

    #Count local frequencies
    logger.info("Start counting local frequencies...")
    for input_file in input_files:
        logger.info(f"Processing '{input_file.name}'")

        #Load input file
        df=pd.read_parquet(input_file)

        #Count value frequencies of "email" column and save it to a file
        df_freq_email=df["email"].value_counts().to_frame(name="freq").reset_index(names=["email"])
        email_output_file=email_output_dir.joinpath(input_file.name)
        df_freq_email.to_parquet(email_output_file,index=False)

        #Count value frequencies of "poh" column and save it to a file
        df_freq_poh=df["poh"].value_counts().to_frame(name="freq").reset_index(names=["poh"])
        poh_output_file=poh_output_dir.joinpath(input_file.name)
        df_freq_poh.to_parquet(poh_output_file,index=False)

    logger.info("Finished counting local frequencies")

if __name__=="__main__":
    parser=argparse.ArgumentParser()
    parser.add_argument("-i","--input-dirname",type=str)
    parser.add_argument("-o","--output-root-dirname",type=str)
    parser.add_argument("--start-index",type=int)
    parser.add_argument("--end-index",type=int)
    args=parser.parse_args()

    main(args)
