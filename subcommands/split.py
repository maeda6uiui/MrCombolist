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
        logger:Logger):
        self.__input_dirname=input_dirname
        self.__output_dirname=output_dirname
        self.__num_lines_per_split=num_lines_per_split
        self.__logger=logger

    def run(self):
        # Get all text files in the input directory
        input_dir = Path(self.__input_dirname)
        input_files = list(input_dir.glob("*.txt"))
        input_files.sort()

        self.__logger.info(f"{len(input_files)} files exist in the input directory")

        # Create output directory
        output_dir = Path(self.__output_dirname)
        output_dir.mkdir(exist_ok=True, parents=True)

        # Split
        self.__logger.info("Start splitting the files...")
        for input_file in tqdm(input_files):
            lines = []
            with input_file.open("r", encoding="utf-8", errors="replace") as r:
                lines = r.read().splitlines()

            # If the file has fewer lines than specified,
            # just copy it to the output directory
            if len(lines) <= self.__num_lines_per_split:
                output_file = output_dir.joinpath(input_file.name)
                shutil.copy(input_file, output_file)
            # Otherwise split it into chunks
            else:
                num_chunks = len(lines) // self.__num_lines_per_split
                if len(lines) % self.__num_lines_per_split != 0:
                    num_chunks += 1

                for i in range(num_chunks):
                    chunk_lines = lines[
                        i * self.__num_lines_per_split : (i + 1) * self.__num_lines_per_split
                    ]

                    output_file = output_dir.joinpath(f"{input_file.stem}-{i}.txt")
                    with output_file.open("w", encoding="utf-8") as w:
                        for line in chunk_lines:
                            w.write(f"{line}\n")

        self.__logger.info("Finished splitting the files")
