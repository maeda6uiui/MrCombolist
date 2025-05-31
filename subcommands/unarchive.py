import json
import rarfile
import shutil
import uuid
from logging import Logger
from pathlib import Path

SHUTIL_SUPPORTED_EXTENSIONS = ["zip", "tar", "tar.gz", "tar.bz", "tar.xz"]
UNRAR_SUPPORTED_EXTENSIONS = ["rar"]


class MCUnarchive:
    def __init__(
        self,
        input_root_dirname: str,
        output_root_dirname: str,
        log_dirname: str,
        start_index: int,
        end_index: int,
        unrar_tool_filepath: str,
        logger: Logger,
    ):
        self.__input_root_dirname = input_root_dirname
        self.__output_root_dirname = output_root_dirname
        self.__log_dirname = log_dirname
        self.__start_index = start_index
        self.__end_index = end_index
        self.__unrar_tool_filepath = unrar_tool_filepath
        self.__logger = logger

    def __str_endswith_any(self, given: str, candidates: list[str]) -> bool:
        for candidate in candidates:
            if given.endswith(candidate):
                return True

        return False

    def run(self):
        # Get all files in the input directory
        self.__logger.info("Start searching input files...")

        input_root_dir = Path(self.__input_root_dirname)
        all_files = input_root_dir.glob("**/*")

        # Get list of files that can be unarchived with this script
        input_files: list[Path] = []
        excluded_files: list[Path] = []
        supported_extensions = SHUTIL_SUPPORTED_EXTENSIONS + UNRAR_SUPPORTED_EXTENSIONS
        for f in all_files:
            # Check if the filename ends with supported extensions
            if f.is_file():
                if self.__str_endswith_any(f.name.lower(), supported_extensions):
                    input_files.append(f)
                else:
                    excluded_files.append(f)

        self.__logger.info(
            f"{len(input_files)} files will be unarchived, and {len(excluded_files)} files are excluded as not supported"
        )

        input_files.sort()
        excluded_files.sort()

        # Create output directory
        output_root_dir = Path(self.__output_root_dirname)
        output_root_dir.mkdir(exist_ok=True, parents=True)

        # Create a directory for unarchive logs
        log_dir = Path(self.__log_dirname)
        log_dir.mkdir(exist_ok=True, parents=True)

        # Output paths of input files to a text file
        input_filepaths_log_file = log_dir.joinpath("input_filepaths.txt")
        with input_filepaths_log_file.open("w", encoding="utf-8") as w:
            for input_file in input_files:
                w.write(f"{str(input_file)}\n")

        # Output paths of excluded files to a text file
        excluded_filepaths_log_file = log_dir.joinpath("excluded_filepaths.txt")
        with excluded_filepaths_log_file.open("w", encoding="utf-8") as w:
            for excluded_file in excluded_files:
                w.write(f"{str(excluded_file)}\n")

        # Create a subset of the list if either the start or the end index is specified
        start_index = self.__start_index if self.__start_index is not None else 0
        end_index = (
            self.__end_index if self.__end_index is not None else len(input_files)
        )

        input_files = input_files[start_index:end_index]

        # Set variables for the unrar tool
        rarfile.UNRAR_TOOL = self.__unrar_tool_filepath
        rarfile.PATH_SEP = "/"

        # Unarchive the files
        self.__logger.info("Start unarchiving the files...")
        for input_file in input_files:
            self.__logger.info(f"Processing '{str(input_file)}'")

            # Create a directory to unarchive the file to
            uuid4_str = str(uuid.uuid4())
            output_dir = output_root_dir.joinpath(uuid4_str)
            output_dir.mkdir(exist_ok=False, parents=False)

            unarchive_result = {}
            try:
                # Files that can be unarchived with shutil
                if self.__str_endswith_any(
                    input_file.name.lower(), SHUTIL_SUPPORTED_EXTENSIONS
                ):
                    shutil.unpack_archive(input_file, output_dir, filter="data")
                # Files that can be unarchived with the unrar tool
                elif self.__str_endswith_any(
                    input_file.name.lower(), UNRAR_SUPPORTED_EXTENSIONS
                ):
                    with rarfile.RarFile(input_file) as rf:
                        rf.extractall(path=output_dir)
            except Exception as e:
                unarchive_result = {
                    "filepath": str(input_file),
                    "error": True,
                    "description": str(e),
                }
                self.__logger.error(f"Failed to unarchive file: '{str(input_file)}'")
            else:
                unarchive_result = {
                    "filepath": str(input_file),
                    "error": False,
                    "description": "",
                }

            # Output unarchive result to the log file
            unarchive_log_file = log_dir.joinpath(f"{uuid4_str}.json")
            with unarchive_log_file.open("w", encoding="utf-8") as w:
                json.dump(unarchive_result, w, ensure_ascii=False)

        self.__logger.info("Finished unarchiving the files")
