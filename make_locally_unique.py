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

    #Get all gzip files in the input directory
    input_dir=Path(input_dirname)
    input_files=list(input_dir.glob("*.txt.gz"))

    logger.info(f"{len(input_files)} files exist in the input directory")

    #Create output directory
    output_dir=Path(output_dirname)
    output_dir.mkdir(exist_ok=True,parents=True)

    #Create a subset of the list if either the start or the end index is specified
    start_index=start_index if start_index is not None else 0
    end_index=end_index if end_index is not None else len(input_files)

    input_files=input_files[start_index:end_index]

    #Make locally unique
    logger.info("Starting to drop duplicates and sort...")
    for input_file in input_files:
        logger.info(f"Processing '{input_file.name}'")

        #Store each line in a set (equivalent to dropping duplicates)
        unique_lines=set()
        with gzip.open(input_file,"rt",encoding="utf-8") as rt:
            for line in rt:
                line=line.strip()
                unique_lines.add(line)

        #Convert the set to a list and sort the values
        lines=list(unique_lines)
        lines.sort()

        #Output sorted values to a gzip file
        output_file=output_dir.joinpath(input_file.name)
        with gzip.open(output_file,"wt",encoding="utf-8") as wt:
            for line in lines:
                wt.write(f"{line}\n")

    logger.info("Finished dropping duplicates and sorting")

if __name__=="__main__":
    parser=argparse.ArgumentParser()
    parser.add_argument("-i","--input-dirname",type=str)
    parser.add_argument("-o","--output-dirname",type=str)
    parser.add_argument("--start-index",type=int)
    parser.add_argument("--end-index",type=int)
    args=parser.parse_args()

    main(args)
