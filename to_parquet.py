import argparse
import csv
import yaml
import pandas as pd
from logging import getLogger, config
from pathlib import Path


def main(args):
    input_root_dirname: str = args.input_root_dirname
    output_dirname: str = args.output_dirname
    start_index: int = args.start_index
    end_index: int = args.end_index

    # Set up logger
    with open("./logging_config.yaml", "r", encoding="utf-8") as r:
        logging_config = yaml.safe_load(r)

    config.dictConfig(logging_config)

    logger = getLogger(__name__)
    logger.debug(args)

    # Get the number of folders in the input directory
    input_root_dir = Path(input_root_dirname)
    input_dirs = input_root_dir.glob("*")
    input_dirs = [f for f in input_dirs if f.is_dir()]
    input_dirs.sort()

    logger.info(f"{len(input_dirs)} folders exist in the input directory")

    # Create output directory
    output_dir = Path(output_dirname)
    output_dir.mkdir(exist_ok=True, parents=True)

    # Create a subset of the list if either the start or the end index is specified
    start_index = start_index if start_index is not None else 0
    end_index = end_index if end_index is not None else len(input_dirs)

    input_dirs = input_dirs[start_index:end_index]

    # Convert to parquet
    logger.info("Start converting to parquet...")
    for input_dir in input_dirs:
        logger.info(f"Processing '{input_dir.name}'")

        input_file = input_dir.joinpath("records.tsv")
        df = pd.read_table(
            input_file,
            encoding="utf-8",
            quoting=csv.QUOTE_NONE,
            names=["email", "poh"],
            dtype={"email": str, "poh": str},
        )

        output_file = output_dir.joinpath(f"{input_dir.name}.parquet")
        df.to_parquet(output_file, index=False)

    logger.info("Finished converting to parquet")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-i", "--input-root-dirname", type=str)
    parser.add_argument("-o", "--output-dirname", type=str)
    parser.add_argument("--start-index", type=int)
    parser.add_argument("--end-index", type=int)
    args = parser.parse_args()

    main(args)
