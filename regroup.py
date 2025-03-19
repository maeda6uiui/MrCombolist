import argparse
import json
import yaml
from logging import getLogger, config
from pathlib import Path


class PartitionInfo(object):
    def __init__(self):
        self.filename = ""
        self.start_line_index = -1  # Inclusive
        self.end_line_index = -1  # Exclusive

    def to_dict(self) -> dict:
        return {
            "filename": self.filename,
            "range": {"start": self.start_line_index, "end": self.end_line_index},
        }


class GroupInfo(object):
    def __init__(self):
        self.num_lines = -1
        self.partitions: list[PartitionInfo] = []

    def to_dict(self) -> dict:
        return {
            "num_lines": self.num_lines,
            "partitions": [v.to_dict() for v in self.partitions],
        }


def main(args):
    input_dirname: str = args.input_dirname
    output_dirname: str = args.output_dirname
    log_dirname: str = args.log_dirname
    num_files_per_group: int = args.num_files_per_group
    start_index: int = args.start_index
    end_index: int = args.end_index

    # Set up logger
    with open("./logging_config.yaml", "r", encoding="utf-8") as r:
        logging_config = yaml.safe_load(r)

    config.dictConfig(logging_config)

    logger = getLogger(__name__)
    logger.debug(args)

    # Get all text files in the input directory
    input_dir = Path(input_dirname)
    input_files = list(input_dir.glob("*.txt"))
    input_files.sort()

    logger.info(f"{len(input_files)} files exist in the input directory")

    # Calculate the number of groups to be created
    num_groups = len(input_files) // num_files_per_group
    if len(input_files) % num_files_per_group != 0:
        num_groups += 1

    logger.info(f"Total {num_groups} groups will be created")

    # Create output directory
    output_dir = Path(output_dirname)
    output_dir.mkdir(exist_ok=True, parents=True)

    # Create a directory for regrouping logs
    log_dir = Path(log_dirname)
    log_dir.mkdir(exist_ok=True, parents=True)

    # Set start and end indices
    start_index = start_index if start_index is not None else 0
    end_index = end_index if end_index is not None else num_groups

    # Regroup
    logger.info("Start regrouping files...")
    for i in range(start_index, end_index):
        target_files = input_files[
            i * num_files_per_group : (i + 1) * num_files_per_group
        ]

        logger.info(f"Creating group {i+1}/{num_groups} with {len(target_files)} files")

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

    logger.info("Finished regrouping the files")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-i", "--input-dirname", type=str)
    parser.add_argument("-o", "--output-dirname", type=str)
    parser.add_argument("-l", "--log-dirname", type=str)
    parser.add_argument("-n", "--num-files-per-group", type=int)
    parser.add_argument("--start-index", type=int)
    parser.add_argument("--end-index", type=int)
    args = parser.parse_args()

    main(args)
