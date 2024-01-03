import argparse
import gzip
import subprocess
import uuid
import yaml
from logging import getLogger,config
from pathlib import Path

def main(args):
    input_dirname:str=args.input_dirname
    concatenation_filepath:str=args.concatenation_filepath
    output_filepath:str=args.output_filepath
    remove_concatenation_file:bool=args.remove_concatenation_file

    #Set up logger
    with open("./logging_config.yaml","r",encoding="utf-8") as r:
        logging_config=yaml.safe_load(r)
    
    config.dictConfig(logging_config)

    logger=getLogger(__name__)
    logger.debug(args)

    #Create output directory if not exist
    output_file=Path(output_filepath)
    output_dir=output_file.parent
    if not output_dir.exists():
        output_dir.mkdir(parents=True)

    concatenation_file=None
    if concatenation_filepath is not None:
        concatenation_file=Path(concatenation_filepath)
    else:
        #Get all gzip files in the input directory
        input_dir=Path(input_dirname)
        input_files=list(input_dir.glob("*.txt.gz"))

        logger.info(f"{len(input_files)} files exist in the input directory")
    
        #Concatenate all input files to a text file
        logger.info("Concatenating all input files to a text file...")

        concatenation_filename=f"{str(uuid.uuid4())}.txt"
        logger.info(f"'{concatenation_filename}' will be created")

        concatenation_file=output_dir.joinpath(concatenation_filename)
        with concatenation_file.open("w",encoding="utf-8") as w:
            for input_file in input_files:
                logger.info(f"Processing '{input_file.name}'")

                with gzip.open(input_file,"rt",encoding="utf-8") as rt:
                    for line in rt:
                        line=line.strip()
                        w.write(f"{line}\n")

        logger.info("Finished concatenating all input files")

    #Run "sort -u"
    logger.info("Running 'sort -u'...")

    fp=output_file.open("w",encoding="utf-8")
    ret=subprocess.run(
        ["sort","-u",str(concatenation_file)],
        stdout=fp,
        stderr=subprocess.PIPE,
        text=True
    )
    fp.close()

    logger.info(f"Return code of 'sort' is {ret.returncode}")
    if len(ret.stderr)!=0:
        logger.error(ret.stderr)

    logger.info("Finished running 'sort -u'")

    if remove_concatenation_file:
        logger.info(f"Concatenation file '{concatenation_file.name}' will be deleted")
        concatenation_file.unlink()

if __name__=="__main__":
    parser=argparse.ArgumentParser()

    #Either --input-dirname or --concatenation-filepath must be provided
    parser.add_argument("-i","--input-dirname",type=str)
    parser.add_argument("-c","--concatenation-filepath",type=str)

    parser.add_argument("-o","--output-filepath",type=str)
    parser.add_argument("--remove-concatenation-file",action="store_true")
    args=parser.parse_args()

    main(args)
