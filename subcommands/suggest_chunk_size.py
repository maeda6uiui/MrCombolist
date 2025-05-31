import random
from logging import Logger
from pathlib import Path
from tqdm import tqdm


class MCSuggestChunkSize:
    def __init__(self, input_dirname: str, num_files_to_check: int, logger: Logger):
        self.__input_dirname = input_dirname
        self.__num_files_to_check = num_files_to_check
        self.__logger = logger

    def __get_avg_num_lines(self) -> int:
        # Get all text files in the input directory
        input_dir = Path(self.__input_dirname)
        input_files = list(input_dir.glob("*.txt"))
        input_files.sort()

        if len(input_files) == 0:
            self.__logger.info("No files exist in the input directory")
            return 0

        self.__logger.info(f"{len(input_files)} files exist in the input directory")

        # Pick up files at random
        num_files_to_check_clamped = min(self.__num_files_to_check, len(input_files))
        files_to_check = random.sample(input_files, num_files_to_check_clamped)

        # Get number of lines for each file
        self.__logger.info("Start getting number of lines for each file")
        num_lines: list[int] = []
        for file in tqdm(files_to_check):
            with file.open("r", encoding="utf-8", errors="replace") as r:
                lines = r.read().splitlines()
                num_lines.append(len(lines))

        # Return average number of lines
        return round(sum(num_lines) / len(num_lines))

    def run(self):
        avg_num_lines = self.__get_avg_num_lines()
        print(f"{avg_num_lines} lines")

    def get(self) -> int:
        return self.__get_avg_num_lines()
