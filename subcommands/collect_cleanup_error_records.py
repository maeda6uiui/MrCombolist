from logging import Logger
from pathlib import Path
from tqdm import tqdm


class MCCollectCleanupErrorRecords:
    def __init__(
        self,
        parsing_root_dirname: str,
        cleanup_root_dirname: str,
        output_dirname: str,
        logger: Logger,
    ):
        self.__parsing_root_dirname = parsing_root_dirname
        self.__cleanup_root_dirname = cleanup_root_dirname
        self.__output_dirname = output_dirname
        self.__logger = logger

    def run(self):
        # Get all folders in the parsing directory
        parsing_root_dir = Path(self.__parsing_root_dirname)
        parsing_dirs = parsing_root_dir.glob("*")
        parsing_dirs = [f for f in parsing_dirs if f.is_dir()]

        self.__logger.info(
            f"{len(parsing_dirs)} folders exist in the parsing directory"
        )

        # Get all folders in the cleanup directory
        cleanup_root_dir = Path(self.__cleanup_root_dirname)
        cleanup_dirs = cleanup_root_dir.glob("*")
        cleanup_dirs = [f for f in cleanup_dirs if f.is_dir()]
        cleanup_dirs.sort()

        self.__logger.info(
            f"{len(cleanup_dirs)} folders exist in the cleanup directory"
        )

        # Create output directory
        output_dir = Path(self.__output_dirname)
        output_dir.mkdir(exist_ok=True, parents=True)

        # Collect error records of the cleanup process
        self.__logger.info(
            "Starting to collect error records of the cleanup process..."
        )
        for cleanup_dir in tqdm(cleanup_dirs):
            # Get error line indices
            error_indices: list[int] = []
            error_indices_file = cleanup_dir.joinpath("error_indices.txt")
            with error_indices_file.open("r", encoding="utf-8") as r:
                for line in r:
                    line = line.strip()
                    error_index = int(line)
                    error_indices.append(error_index)

            # Collect error records
            parsing_file = parsing_root_dir.joinpath(cleanup_dir.name, "records.tsv")
            with parsing_file.open("r", encoding="utf-8") as r:
                records = r.read().splitlines()

            error_records = list(map(lambda i: records[i], error_indices))

            # Output error records to a text file
            output_file = output_dir.joinpath(f"{cleanup_dir.name}.txt")
            with output_file.open("w", encoding="utf-8") as w:
                for error_record in error_records:
                    w.write(f"{error_record}\n")

        self.__logger.info("Finished collecting error records of the cleanup process")
