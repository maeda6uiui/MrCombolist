import json
from logging import Logger
from pathlib import Path


class PartitionInfo:
    def __init__(self):
        self.filename = ""
        self.start_line_index = -1  # Inclusive
        self.end_line_index = -1  # Exclusive

    def to_dict(self) -> dict:
        return {
            "filename": self.filename,
            "range": {"start": self.start_line_index, "end": self.end_line_index},
        }


class GroupInfo:
    def __init__(self):
        self.num_lines = -1
        self.partitions: list[PartitionInfo] = []

    def to_dict(self) -> dict:
        return {
            "num_lines": self.num_lines,
            "partitions": [v.to_dict() for v in self.partitions],
        }


class MCRegroup:
    def __init__(
        self,
        input_dirname: str,
        output_dirname: str,
        num_files_per_group: int,
        start_index: int,
        end_index: int,
        logger: Logger,
    ):
        self.__input_dirname = input_dirname
        self.__output_dirname = output_dirname
        self.__num_files_per_group = num_files_per_group
        self.__start_index = start_index
        self.__end_index = end_index
        self.__logger = logger

    def run(self):
        # Get all text files in the input directory
        input_dir = Path(self.__input_dirname)
        input_files = list(input_dir.glob("*.txt"))
        input_files.sort()

        self.__logger.info(f"{len(input_files)} files exist in the input directory")

        # Calculate the number of groups to be created
        num_groups = len(input_files) // self.__num_files_per_group
        if len(input_files) % self.__num_files_per_group != 0:
            num_groups += 1

        self.__logger.info(f"Total {num_groups} groups will be created")

        # Create output directory
        output_dir = Path(self.__output_dirname)
        output_dir.mkdir(exist_ok=True, parents=True)

        # Create a directory for regrouping logs
        log_dir = Path(self.__log_dirname)
        log_dir.mkdir(exist_ok=True, parents=True)

        # Set start and end indices
        start_index = self.__start_index if self.__start_index is not None else 0
        end_index = self.__end_index if self.__end_index is not None else num_groups

        # Regroup
        self.__logger.info("Start regrouping files...")
        for i in range(start_index, end_index):
            target_files = input_files[
                i * self.__num_files_per_group : (i + 1) * self.__num_files_per_group
            ]

            self.__logger.info(
                f"Creating group {i+1}/{num_groups} with {len(target_files)} files"
            )

            # Concatenate the lines of the input files
            lines = []
            group_info = GroupInfo()

            for target_file in target_files:
                partition_info = PartitionInfo()
                partition_info.filename = target_file.name
                partition_info.start_line_index = len(lines)

                with target_file.open("r", encoding="utf-8", errors="replace") as r:
                    lines += r.read().splitlines()

                partition_info.end_line_index = len(lines)

                group_info.partitions.append(partition_info)

            group_info.num_lines = len(lines)

            # Output concatenated lines to a text file
            output_file = output_dir.joinpath(f"{i}.txt")
            with output_file.open("w", encoding="utf-8") as w:
                for line in lines:
                    w.write(f"{line}\n")

            # Output group info to a log file
            log_file = log_dir.joinpath(f"{i}.json")
            with log_file.open("w", encoding="utf-8") as w:
                json.dump(group_info.to_dict(), w, ensure_ascii=False)

        self.__logger.info("Finished regrouping the files")
