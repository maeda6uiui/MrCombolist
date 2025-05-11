import gzip
import json
import re
from collections import Counter
from logging import Logger
from pathlib import Path

class MCDetectSchema:
    def __init__(
        self,
        input_dirname: str,
        log_dirname: str,
        delimiter_candidates: str,
        max_num_lines: int,
        max_line_length: int,
        start_index: int,
        end_index: int,
        logger:Logger):
        self.__input_dirname=input_dirname
        self.__log_dirname=log_dirname
        self.__delimiter_candidates=delimiter_candidates
        self.__max_num_lines=max_num_lines
        self.__max_line_length=max_line_length
        self.__start_index=start_index
        self.__end_index=end_index
        self.__logger=logger

    def __detect_schema(
        self,
        lines: list[str],
        r_email_head: re.Pattern,
        r_email_tail: re.Pattern,
        r_email_middle: re.Pattern,
        r_email_only: re.Pattern) -> dict[str, str]:
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

    def run(self):
        # Get all gzip files in the input directory
        input_dir = Path(self.__input_dirname)
        input_files = list(input_dir.glob("*.txt.gz"))
        input_files.sort()

        self.__logger.info(f"{len(input_files)} files exist in the input directory")

        # Create a directory for schema detection logs
        log_dir = Path(self.__log_dirname)
        log_dir.mkdir(exist_ok=True, parents=True)

        # Set up regular expressions
        r_email_head = re.compile(r"^\S+@\S+\.\S+" + f"[{self.__delimiter_candidates}]")
        r_email_tail = re.compile(f"[{self.__delimiter_candidates}]+" + r"\S+@\S+\.\S+$")
        r_email_middle = re.compile(
            f"[{self.__delimiter_candidates}]+" + r"\S+@\S+\.\S+" + f"[{self.__delimiter_candidates}]"
        )
        r_email_only = re.compile(r"^\S+@\S+\.\S+$")

        # Create a subset of the list if either the start or the end index is specified
        start_index = self.__start_index if self.__start_index is not None else 0
        end_index = self.__end_index if self.__end_index is not None else len(input_files)

        input_files = input_files[start_index:end_index]

        # Detect schema
        self.__logger.info("Starting schema detection...")
        for input_file in input_files:
            self.__logger.info(f"Processing '{input_file.name}'")

            lines = []
            with gzip.open(input_file, "rt", encoding="utf-8") as rt:
                for idx, line in enumerate(rt):
                    if idx > self.__max_num_lines:
                        break

                    line = line[:self.__max_line_length].strip()
                    lines.append(line)

            schema = self.__detect_schema(
                lines, r_email_head, r_email_tail, r_email_middle, r_email_only
            )

            # Output schema detection result to a log file
            schema_detection_result = {"filename": input_file.name, "schema": schema}

            log_file = log_dir.joinpath(f"{input_file.stem}.json")
            with log_file.open("w", encoding="utf-8") as w:
                json.dump(schema_detection_result, w, ensure_ascii=False)

        self.__logger.info("Finished schema detection")
