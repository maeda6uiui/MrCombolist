import argparse
import gzip
import yaml
from logging import getLogger,config
from pathlib import Path

def main(args):
    input_dirname:str=args.input_dirname
    output_dirname:str=args.output_dirname
    start_index:int=args.start_index
    end_index:int=args.end_index

    #Set up logger
    with open("./logging_config.yaml","r",encoding="utf-8") as r:
        logging_config=yaml.safe_load(r)
    
    config.dictConfig(logging_config)

    logger=getLogger(__name__)
    logger.debug(args)

    #Get all text files in the input directory
    input_dir=Path(input_dirname)
    input_files=list(input_dir.glob("*.txt"))

    logger.info(f"{len(input_files)} files exist in the input directory")

    #Create output directory
    output_dir=Path(output_dirname)
    output_dir.mkdir(exist_ok=True,parents=True)

    #Create a subset of the list if either the start or the end index is specified
    start_index=start_index if start_index is not None else 0
    end_index=end_index if end_index is not None else len(input_files)

    input_files=input_files[start_index:end_index]

    #Rearchive
    logger.info("Start rearchiving the files...")
    for input_file in input_files:
        logger.info(f"Creating archive of {input_file.name}")

        output_file=output_dir.joinpath(f"{input_file.name}.gz")
        with gzip.open(output_file,"wt",encoding="utf-8") as wt:
            with input_file.open("r",encoding="utf-8") as r:
                for line in r:
                    line=line.strip()
                    wt.write(f"{line}\n")

    logger.info("Finished rearchiving the files")

if __name__=="__main__":
    parser=argparse.ArgumentParser()
    parser.add_argument("-i","--input-dirname",type=str)
    parser.add_argument("-o","--output-dirname",type=str)
    parser.add_argument("--start-index",type=int)
    parser.add_argument("--end-index",type=int)
    args=parser.parse_args()
    
    main(args)
