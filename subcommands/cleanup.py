from logging import Logger
from pathlib import Path


class MCCleanup:
    def __init__(
        self,
        input_root_dirname: str,
        output_root_dirname: str,
        start_index: int,
        end_index: int,
        logger: Logger,
    ):
        self.__input_root_dirname = input_root_dirname
        self.__output_root_dirname = output_root_dirname
        self.__start_index = start_index
        self.__end_index = end_index
        self.__logger = logger

    def run(self):
        # Get the number of folders in the input directory
        input_root_dir = Path(self.__input_root_dirname)
        input_dirs = input_root_dir.glob("*")
        input_dirs = [f for f in input_dirs if f.is_dir()]
        input_dirs.sort()

        self.__logger.info(f"{len(input_dirs)} folders exist in the input directory")

        # Create output directory
        output_root_dir = Path(self.__output_root_dirname)
        output_root_dir.mkdir(exist_ok=True, parents=True)

        # Create a subset of the list if either the start or the end index is specified
        start_index = self.__start_index if self.__start_index is not None else 0
        end_index = (
            self.__end_index if self.__end_index is not None else len(input_dirs)
        )

        input_dirs = input_dirs[start_index:end_index]

        # Cleanup
        self.__logger.info("Start cleaning up the files...")
        for input_dir in input_dirs:
            self.__logger.info(f"Processing '{input_dir.name}'")

            # Create output directory
            output_dir = output_root_dir.joinpath(input_dir.name)
            output_dir.mkdir(exist_ok=True)

            # Read input file and write valid lines to output file
            input_file = input_dir.joinpath("records.tsv")
            output_file = output_dir.joinpath("records.tsv")
            error_indices: list[int] = []

            with output_file.open("w", encoding="utf-8") as w:
                with input_file.open("r", encoding="utf-8") as r:
                    for idx, line in enumerate(r):
                        line = line.strip()

                        # Each line must have two columns
                        # Otherwise it's an invalid record
                        splits = line.split("\t")
                        if len(splits) == 2:
                            w.write(f"{line}\n")
                        else:
                            error_indices.append(idx)

            # Output error indices to a text file
            error_indices_file = output_dir.joinpath("error_indices.txt")
            with error_indices_file.open("w", encoding="utf-8") as w:
                for error_index in error_indices:
                    w.write(f"{error_index}\n")

        self.__logger.info("Finished cleaning up the files")
