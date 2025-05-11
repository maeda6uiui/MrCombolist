import gzip
import json
import re
from logging import Logger
from pathlib import Path


class ParseResult:
    def __init__(self):
        self.error = False
        self.email = ""
        self.poh = ""

    def strip(self):
        self.email = self.email.strip()
        self.poh = self.poh.strip()

    def to_dict(self) -> dict:
        return {"error": self.error, "email": self.email, "poh": self.poh}


class MCParse:
    def __init__(
        self,
        input_dirname: str,
        output_root_dirname: str,
        schema_detection_log_dirname: str,
        max_line_length: int,
        start_index: int,
        end_index: int,
        logger: Logger,
    ):
        self.__input_dirname = input_dirname
        self.__output_root_dirname = output_root_dirname
        self.__schema_detection_log_dirname = schema_detection_log_dirname
        self.__max_line_length = max_line_length
        self.__start_index = start_index
        self.__end_index = end_index
        self.__logger = logger

    def __parse_email_poh(self, line: str, delimiter: str) -> ParseResult:
        result = ParseResult()

        splits = line.split(delimiter, maxsplit=1)
        if len(splits) != 2:
            result.error = True
        else:
            result.error = False
            result.email = splits[0]
            result.poh = splits[1]

        return result

    def __parse_poh_email(self, line: str, delimiter: str) -> ParseResult:
        result = ParseResult()

        splits = line.split(delimiter, maxsplit=1)
        if len(splits) != 2:
            result.error = True
        else:
            result.error = False
            result.email = splits[1]
            result.poh = splits[0]

        return result

    def __parse_unknown_email_unknown(
        self, line: str, delimiter: str, r_email_middle: re.Pattern
    ) -> ParseResult:
        result = ParseResult()

        m = r_email_middle.search(line)
        if m is None:
            result.error = True
        else:
            result.error = False
            result.email = m.group().replace(delimiter, "")

        return result

    def run(self):
        # Get all gzip files in the input directory
        input_dir = Path(self.__input_dirname)
        input_files = list(input_dir.glob("*.txt.gz"))
        input_files.sort()

        self.__logger.info(f"{len(input_files)} files exist in the input directory")

        # Create output directory
        output_root_dir = Path(self.__output_root_dirname)
        output_root_dir.mkdir(exist_ok=True, parents=True)

        # Open log directory of schema detection
        schema_detection_log_dir = Path(self.__schema_detection_log_dirname)

        # Create a subset of the list if either the start or the end index is specified
        start_index = self.__start_index if self.__start_index is not None else 0
        end_index = (
            self.__end_index if self.__end_index is not None else len(input_files)
        )

        input_files = input_files[start_index:end_index]

        # Parse
        self.__logger.info("Start parsing the files...")
        for input_file in input_files:
            self.__logger.info(f"Processing '{input_file.name}'")

            # Get scheme detection result
            schema_detection_log_file = schema_detection_log_dir.joinpath(
                f"{input_file.stem}.json"
            )
            with schema_detection_log_file.open("r", encoding="utf-8") as r:
                schema_detection_result: dict = json.load(r)

            schema: dict = schema_detection_result["schema"]
            placement: str = schema["placement"]
            delimiter: str = schema["delimiter"]

            r_email_middle = re.compile(
                f"[{delimiter}]+" + r"\S+@\S+\.\S+" + f"[{delimiter}]"
            )

            # Parse each line in the input file
            parse_results: list[ParseResult] = []
            with gzip.open(input_file, "rt", encoding="utf-8") as rt:
                for line in rt:
                    line = line[: self.__max_line_length].strip()

                    parse_result = ParseResult()
                    if placement == "email:poh":
                        parse_result = self.__parse_email_poh(line, delimiter)
                    elif placement == "poh:email":
                        parse_result = self.__parse_poh_email(line, delimiter)
                    elif placement == "unknown:email:unknown":
                        parse_result = self.__parse_unknown_email_unknown(
                            line, delimiter, r_email_middle
                        )
                    elif placement == "email":
                        parse_result.error = False
                        parse_result.email = line
                    else:
                        parse_result.error = True

                    parse_result.strip()
                    parse_results.append(parse_result)

            # Create a directory to output parsing results
            input_basename = input_file.name.split(".")[0]
            output_dir = output_root_dir.joinpath(input_basename)
            output_dir.mkdir(exist_ok=True)

            # Output parsed records to as a TSV file
            parsed_records_file = output_dir.joinpath("records.tsv")
            with parsed_records_file.open("w", encoding="utf-8") as w:
                for parse_result in parse_results:
                    if not parse_result.error:
                        w.write(f"{parse_result.email}\t{parse_result.poh}\n")

            # Output error line indices to a text file
            error_indices_file = output_dir.joinpath("error_indices.txt")
            with error_indices_file.open("w", encoding="utf-8") as w:
                for idx, parse_result in enumerate(parse_results):
                    if parse_result.error:
                        w.write(f"{idx}\n")

        self.__logger.info("Finished parsing the files")
