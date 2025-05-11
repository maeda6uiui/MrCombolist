import csv
import pandas as pd
from logging import Logger
from pathlib import Path

class MCToParquet:
    def __init__(
        self,
        input_root_dirname: str,
        output_dirname: str,
        start_index: int,
        end_index: int,
        logger:Logger):
        self.__input_root_dirname=input_root_dirname
        self.__output_dirname=output_dirname
        self.__start_index=start_index
        self.__end_index=end_index
        self.__logger=logger

    def run(self):
        # Get the number of folders in the input directory
        input_root_dir = Path(self.__input_root_dirname)
        input_dirs = input_root_dir.glob("*")
        input_dirs = [f for f in input_dirs if f.is_dir()]
        input_dirs.sort()

        self.__logger.info(f"{len(input_dirs)} folders exist in the input directory")

        # Create output directory
        output_dir = Path(self.__output_dirname)
        output_dir.mkdir(exist_ok=True, parents=True)

        # Create a subset of the list if either the start or the end index is specified
        start_index = self.__start_index if self.__start_index is not None else 0
        end_index = self.__end_index if self.__end_index is not None else len(input_dirs)

        input_dirs = input_dirs[start_index:end_index]

        # Convert to parquet
        self.__logger.info("Start converting to parquet...")
        for input_dir in input_dirs:
            self.__logger.info(f"Processing '{input_dir.name}'")

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

        self.__logger.info("Finished converting to parquet")
