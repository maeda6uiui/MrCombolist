import gzip
from logging import Logger
from pathlib import Path
from tqdm import tqdm


class MCCollectParsingErrorRecords:
    def __init__(
        self,
        rearchive_dirname: str,
        parsing_root_dirname: str,
        output_dirname: str,
        logger: Logger,
    ):
        self.__rearchive_dirname = rearchive_dirname
        self.__parsing_root_dirname = parsing_root_dirname
        self.__output_dirname = output_dirname
        self.__logger = logger

    def run(self):
        # Create path of the rearchive directory
        rearchive_dir = Path(self.__rearchive_dirname)

        # Get all folders in the parsing directory
        parsing_root_dir = Path(self.__parsing_root_dirname)
        parsing_dirs = parsing_root_dir.glob("*")
        parsing_dirs = [f for f in parsing_dirs if f.is_dir()]
        parsing_dirs.sort()

        self.__logger.info(
            f"{len(parsing_dirs)} folders exist in the parsing directory"
        )

        # Create output directory
        output_dir = Path(self.__output_dirname)
        output_dir.mkdir(exist_ok=True, parents=True)

        # Collect error records of the parsing process
        self.__logger.info(
            "Starting to collect error records of the parsing process..."
        )
        for parsing_dir in tqdm(parsing_dirs):
            # Get error indices
            error_indices: list[int] = []
            error_indices_file = parsing_dir.joinpath("error_indices.txt")
            with error_indices_file.open("r", encoding="utf-8") as r:
                for line in r:
                    line = line.strip()
                    error_index = int(line)
                    error_indices.append(error_index)

            # Collect error records
            rearchive_file = rearchive_dir.joinpath(f"{parsing_dir.name}.txt.gz")
            with gzip.open(rearchive_file, "rt", encoding="utf-8") as rt:
                records = rt.read().splitlines()

            error_records = list(map(lambda i: records[i], error_indices))

            # Output error records to a text file
            output_file = output_dir.joinpath(f"{parsing_dir.name}.txt")
            with output_file.open("w", encoding="utf-8") as w:
                for error_record in error_records:
                    w.write(f"{error_record}\n")

        self.__logger.info("Finished collecting error records of the parsing process")
