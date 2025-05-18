import pgzip
from logging import Logger
from pathlib import Path


class MCRearchive:
    def __init__(
        self,
        input_dirname: str,
        output_dirname: str,
        start_index: int,
        end_index: int,
        logger: Logger,
    ):
        self.__input_dirname = input_dirname
        self.__output_dirname = output_dirname
        self.__start_index = start_index
        self.__end_index = end_index
        self.__logger = logger

    def run(self):
        # Get all text files in the input directory
        input_dir = Path(self.__input_dirname)
        input_files = list(input_dir.glob("*.txt"))
        input_files.sort()

        self.__logger.info(f"{len(input_files)} files exist in the input directory")

        # Create output directory
        output_dir = Path(self.__output_dirname)
        output_dir.mkdir(exist_ok=True, parents=True)

        # Create a subset of the list if either the start or the end index is specified
        start_index = self.__start_index if self.__start_index is not None else 0
        end_index = (
            self.__end_index if self.__end_index is not None else len(input_files)
        )

        input_files = input_files[start_index:end_index]

        # Rearchive
        self.__logger.info("Start rearchiving the files...")
        for input_file in input_files:
            self.__logger.info(f"Creating archive of {input_file.name}")

            output_file = output_dir.joinpath(f"{input_file.name}.gz")
            with pgzip.open(output_file, "wt", encoding="utf-8") as wt:
                with input_file.open("r", encoding="utf-8") as r:
                    for line in r:
                        line = line.strip()
                        wt.write(f"{line}\n")

        self.__logger.info("Finished rearchiving the files")
