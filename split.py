import argparse
import shutil
import yaml
from logging import getLogger,config
from pathlib import Path
from tqdm import tqdm

def main(args):
    input_dirname:str=args.input_dirname
    output_dirname:str=args.output_dirname
    num_lines_per_split:int=args.num_lines_per_split
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

    #Set start and end indices
    start_index=start_index if start_index is not None else 0
    end_index=end_index if end_index is not None else len(input_files)

    #Split
    logger.info("Start splitting the files...")
    for input_file in tqdm(input_files):
        lines=[]
        with input_file.open("r",encoding="utf-8",errors="replace") as r:
            lines=r.read().splitlines()

        #If the file has fewer lines than specified,
        #just copy it to the output directory
        if len(lines)<=num_lines_per_split:
            output_file=output_dir.joinpath(input_file.name)
            shutil.copy(input_file,output_file)
        #Otherwise split it into chunks
        else:
            num_chunks=len(lines)//num_lines_per_split
            if len(lines)%num_lines_per_split!=0:
                num_chunks+=1

            for i in range(num_chunks):
                chunk_lines=lines[i*num_lines_per_split:(i+1)*num_lines_per_split]

                output_file=output_dir.joinpath(f"{input_file.stem}-{i}.txt")
                with output_file.open("w",encoding="utf-8") as w:
                    for line in chunk_lines:
                        w.write(f"{line}\n")

    logger.info("Finished splitting the files")

if __name__=="__main__":
    parser=argparse.ArgumentParser()
    parser.add_argument("-i","--input-dirname",type=str)
    parser.add_argument("-o","--output-dirname",type=str)
    parser.add_argument("-n","--num-lines-per-split",type=int)
    parser.add_argument("--start-index",type=int)
    parser.add_argument("--end-index",type=int)
    args=parser.parse_args()

    main(args)
