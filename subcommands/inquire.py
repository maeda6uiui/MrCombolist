import gzip
import sqlite3
from logging import Logger
from pathlib import Path

class MCInquire:
    def __init__(
        self,
        db_filepath: str,
        output_filepath: str,
        query: str,
        max_num_records_to_print: int,
        num_records_for_logging: int,
        logger:Logger):
        self.__db_filepath=db_filepath
        self.__output_filepath=output_filepath
        self.__query=query
        self.__max_num_records_to_print=max_num_records_to_print
        self.__num_records_for_logging=num_records_for_logging
        self.__logger=logger

    def __print_records(self,db_filepath: str, query: str, max_num_records_to_print: int):
        with sqlite3.connect(db_filepath) as conn:
            cur = conn.cursor()
            cur.execute(query)
            rows = cur.fetchmany(max_num_records_to_print)
            for row in rows:
                print(row)

    def __export_records(
        self,
        db_filepath: str,
        query: str,
        output_filepath: str,
        num_records_for_logging: int,
        logger: Logger,
    ):
        # Create output directory
        logger.info("Creating output directory...")

        output_file = Path(output_filepath)
        output_dir = output_file.parent
        output_dir.mkdir(exist_ok=True, parents=True)

        # Execute query
        logger.info("Executing query...")

        conn = sqlite3.connect(db_filepath)
        cur = conn.cursor()
        cur.execute(query)

        # Fetch one row at a time and write it to a gzipped TSV file
        logger.info("Start exporting records to a file...")
        with gzip.open(output_filepath, "wt", encoding="utf-8") as wt:
            row = cur.fetchone()
            num_records_exported = 0
            while row is not None:
                line = "\t".join(map(str, row))
                wt.write(f"{line}\n")

                num_records_exported += 1
                if num_records_exported % num_records_for_logging == 0:
                    logger.info(f"{num_records_exported/1000000}M records exported")

                row = cur.fetchone()

        conn.close()

        logger.info("Finished exporting records to a file")

    def run(self):
        # Check if DB file exists
        db_file = Path(self.__db_filepath)
        if not db_file.exists():
            self.__logger.error(f"DB file '{db_file.name}' does not exist")
            return

        # Print query result
        if self.__output_filepath is None:
            self.__print_records(self.__db_filepath, self.__query, self.__max_num_records_to_print)
        # Export to a gzipped TSV file
        else:
            self.__export_records(
                self.__db_filepath, self.__query, self.__output_filepath, self.__num_records_for_logging, self.__logger
            )
