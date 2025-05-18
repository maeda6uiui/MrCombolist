import json
import shutil
from logging import Logger
from pathlib import Path
from tqdm import tqdm


class MCFlatten:
    def __init__(
        self,
        input_root_dirname: str,
        output_dirname: str,
        log_filepath: str,
        logger: Logger,
    ):
        self.__input_root_dirname = input_root_dirname
        self.__output_dirname = output_dirname
        self.__log_filepath = log_filepath
        self.__logger = logger

    def run(self):
        # Get list of all "text" files in the input directory
        # AFAIK, most combolists are comprised of text files,
        # but you may want some extra processing in case the combolist
        # contains other formats of data, such as JSON and SQL
        input_root_dir = Path(self.__input_root_dirname)
        input_files = input_root_dir.glob("**/*.txt")
        input_files = [f for f in input_files if f.is_file()]
        input_files.sort()

        self.__logger.info(f"{len(input_files)} files exist in the input directory")

        # Create output directory
        output_dir = Path(self.__output_dirname)
        output_dir.mkdir(exist_ok=True, parents=True)

        # Flatten
        self.__logger.info("Start flattening the files...")

        flattening_results: list[dict] = []

        for idx, input_file in enumerate(tqdm(input_files)):
            # Copy the file
            output_file = output_dir.joinpath(f"{idx}.txt")
            shutil.copy(input_file, output_file)

            # Store the correspondence of the input and the output files
            flatten_result = {"index": idx, "input_filepath": str(input_file)}
            flattening_results.append(flatten_result)

        self.__logger.info("Finished flattening the files")

        # Output flatten results to a log file
        log_file = Path(self.__log_filepath)
        with log_file.open("w", encoding="utf-8") as w:
            json.dump(flattening_results, w, ensure_ascii=False)
