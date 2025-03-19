import argparse
import gzip
import json
import re
import yaml
from collections import Counter
from logging import getLogger, config
from pathlib import Path


def detect_schema(
    lines: list[str],
    r_email_head: re.Pattern,
    r_email_tail: re.Pattern,
    r_email_middle: re.Pattern,
    r_email_only: re.Pattern,
) -> dict[str, str]:
    schema = {"placement": "n/a", "delimiter": "n/a"}

    if len(lines) == 0:
        return schema

    possible_placements = []
    possible_delimiters = []
    for line in lines:
        possible_placement = ""
        possible_delimiter = ""

        m_head = r_email_head.search(line)
        m_tail = r_email_tail.search(line)
        m_middle = r_email_middle.search(line)
        m_only = r_email_only.search(line)

        if m_head is not None:
            possible_placement = "email:poh"
            possible_delimiter = line[m_head.end() - 1]
        elif m_tail is not None:
            possible_placement = "poh:email"
            possible_delimiter = line[m_tail.start()]
        elif m_middle is not None:
            possible_placement = "unknown:email:unknown"
            possible_delimiter = line[m_middle.start()]
        elif m_only is not None:
            possible_placement = "email"
            possible_delimiter = "n/a"
        else:
            possible_placement = "n/a"
            possible_delimiter = "n/a"

        possible_placements.append(possible_placement)
        possible_delimiters.append(possible_delimiter)

    # Get the most frequent placement and delimiter
    ct_placements = Counter(possible_placements)
    ct_delimiters = Counter(possible_delimiters)

    placement = ct_placements.most_common(1)[0][0]
    delimiter = ct_delimiters.most_common(1)[0][0]

    schema = {"placement": placement, "delimiter": delimiter}
    return schema


def main(args):
    input_dirname: str = args.input_dirname
    log_dirname: str = args.log_dirname
    delimiter_candidates: str = args.delimiter_candidates
    max_num_lines: int = args.max_num_lines
    max_line_length: int = args.max_line_length
    start_index: int = args.start_index
    end_index: int = args.end_index

    # Set up logger
    with open("./logging_config.yaml", "r", encoding="utf-8") as r:
        logging_config = yaml.safe_load(r)

    config.dictConfig(logging_config)

    logger = getLogger(__name__)
    logger.debug(args)

    # Get all gzip files in the input directory
    input_dir = Path(input_dirname)
    input_files = list(input_dir.glob("*.txt.gz"))
    input_files.sort()

    logger.info(f"{len(input_files)} files exist in the input directory")

    # Create a directory for schema detection logs
    log_dir = Path(log_dirname)
    log_dir.mkdir(exist_ok=True, parents=True)

    # Set up regular expressions
    r_email_head = re.compile(r"^\S+@\S+\.\S+" + f"[{delimiter_candidates}]")
    r_email_tail = re.compile(f"[{delimiter_candidates}]+" + r"\S+@\S+\.\S+$")
    r_email_middle = re.compile(
        f"[{delimiter_candidates}]+" + r"\S+@\S+\.\S+" + f"[{delimiter_candidates}]"
    )
    r_email_only = re.compile(r"^\S+@\S+\.\S+$")

    # Create a subset of the list if either the start or the end index is specified
    start_index = start_index if start_index is not None else 0
    end_index = end_index if end_index is not None else len(input_files)

    input_files = input_files[start_index:end_index]

    # Detect schema
    logger.info("Starting schema detection...")
    for input_file in input_files:
        logger.info(f"Processing '{input_file.name}'")

        lines = []
        with gzip.open(input_file, "rt", encoding="utf-8") as rt:
            for idx, line in enumerate(rt):
                if idx > max_num_lines:
                    break

                line = line[:max_line_length].strip()
                lines.append(line)

        schema = detect_schema(
            lines, r_email_head, r_email_tail, r_email_middle, r_email_only
        )

        # Output schema detection result to a log file
        schema_detection_result = {"filename": input_file.name, "schema": schema}

        log_file = log_dir.joinpath(f"{input_file.stem}.json")
        with log_file.open("w", encoding="utf-8") as w:
            json.dump(schema_detection_result, w, ensure_ascii=False)

    logger.info("Finished schema detection")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-i", "--input-dirname", type=str)
    parser.add_argument("-l", "--log-dirname", type=str)
    parser.add_argument("-d", "--delimiter-candidates", type=str, default=":;|, \t")
    parser.add_argument("--max-num-lines", type=int, default=10000000)
    parser.add_argument("--max-line-length", type=int, default=200)
    parser.add_argument("--start-index", type=int)
    parser.add_argument("--end-index", type=int)
    args = parser.parse_args()

    main(args)
