import argparse
import shutil
from logging import Logger
from pathlib import Path
from tqdm import tqdm

class MCSplit:
    def __init__(
        self,
        input_dirname:str,
        output_dirname:str,
        num_lines_per_split:int,
        start_index:int,
        end_index:int,
        logger:Logger):
        self.input_dirname=input_dirname
        self.output_dirname=output_dirname
        self.num_lines_per_split=num_lines_per_split
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

        # Set start and end indices
        start_index = start_index if start_index is not None else 0
        end_index = end_index if end_index is not None else len(input_files)

        # Split
        self.logger.info("Start splitting the files...")
        for input_file in tqdm(input_files):
            lines = []
            with input_file.open("r", encoding="utf-8", errors="replace") as r:
                lines = r.read().splitlines()

            # If the file has fewer lines than specified,
            # just copy it to the output directory
            if len(lines) <= self.num_lines_per_split:
                output_file = output_dir.joinpath(input_file.name)
                shutil.copy(input_file, output_file)
            # Otherwise split it into chunks
            else:
                num_chunks = len(lines) // self.num_lines_per_split
                if len(lines) % self.num_lines_per_split != 0:
                    num_chunks += 1

                for i in range(num_chunks):
                    chunk_lines = lines[
                        i * self.num_lines_per_split : (i + 1) * self.num_lines_per_split
                    ]

                    output_file = output_dir.joinpath(f"{input_file.stem}-{i}.txt")
                    with output_file.open("w", encoding="utf-8") as w:
                        for line in chunk_lines:
                            w.write(f"{line}\n")

        self.logger.info("Finished splitting the files")
