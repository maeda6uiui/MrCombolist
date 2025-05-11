import argparse
import gzip
import yaml
from logging import Logger
from pathlib import Path

class MCRearchive:
    def __init__(
        self,
        input_dirname:str,
        output_dirname:str,
        start_index:int,
        end_index:int,
        logger:Logger):
        self.input_dirname=input_dirname
        self.output_dirname=output_dirname
        self.start_index=start_index
        self.end_index=end_index
        self.logger=logger

    def run(self):
        # Get all text files in the input directory
        input_dir = Path(self.input_dirname)
        input_files = list(input_dir.glob("*.txt"))
        input_files.sort()

        self.logger.info(f"{len(input_files)} files exist in the input directory")

        # Create output directory
        output_dir = Path(self.output_dirname)
        output_dir.mkdir(exist_ok=True, parents=True)

        # Create a subset of the list if either the start or the end index is specified
        start_index = start_index if start_index is not None else 0
        end_index = end_index if end_index is not None else len(input_files)

        input_files = input_files[start_index:end_index]

        # Rearchive
        self.logger.info("Start rearchiving the files...")
        for input_file in input_files:
            self.logger.info(f"Creating archive of {input_file.name}")

            output_file = output_dir.joinpath(f"{input_file.name}.gz")
            with gzip.open(output_file, "wt", encoding="utf-8") as wt:
                with input_file.open("r", encoding="utf-8") as r:
                    for line in r:
                        line = line.strip()
                        wt.write(f"{line}\n")

        self.logger.info("Finished rearchiving the files")
