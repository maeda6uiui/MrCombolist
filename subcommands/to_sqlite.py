import pandas as pd
import sqlite3
from logging import Logger
from pathlib import Path


class MCToSQLite:
    def __init__(
        self,
        input_dirname: str,
        output_dirname: str,
        email_freqs_db: bool,
        poh_freqs_db: bool,
        personae_db: bool,
        start_index: int,
        end_index: int,
        logger: Logger,
    ):
        self.__input_dirname = input_dirname
        self.__output_dirname = output_dirname
        self.__email_freqs_db = email_freqs_db
        self.__poh_freqs_db = poh_freqs_db
        self.__personae_db = personae_db
        self.__start_index = start_index
        self.__end_index = end_index
        self.__logger = logger

    def run(self):
        # Get all parquet files in the input directory
        input_dir = Path(self.__input_dirname)
        input_files = list(input_dir.glob("*.parquet"))
        input_files.sort()

        self.__logger.info(f"{len(input_files)} files exist in the input directory")

        # Create output directory
        output_dir = Path(self.__output_dirname)
        output_dir.mkdir(exist_ok=True, parents=True)

        # Create a subset of the list if either the start or the end index is specified
        start_index = self.__start_index if self.__start_index is not None else 0
        end_index = (
            self.__end_index if self.__end_index is not None else len(input_files)
        )

        input_files = input_files[start_index:end_index]

        # Determine value column name and table name
        val_column_name = ""
        table_name = ""

        if self.__email_freqs_db:
            val_column_name = "email"
            table_name = "freqs"
        elif self.__poh_freqs_db:
            val_column_name = "poh"
            table_name = "freqs"
        elif self.__personae_db:
            table_name = "personae"
        else:
            self.__logger.error("Must specify the type of DB to be created")
            return

        # Convert to SQLite DB
        self.__logger.info("Start converting to SQLite DB...")
        for input_file in input_files:
            self.__logger.info(f"Processing '{input_file.name}'")

            # Load input file
            df = pd.read_parquet(input_file)

            # Rename value column to "word"
            if val_column_name != "":
                df.rename(columns={val_column_name: "word"}, inplace=True)

            # Create connection and output to SQLite DB
            db_file = output_dir.joinpath(f"{input_file.stem}.db")
            with sqlite3.connect(db_file) as conn:
                df.to_sql(table_name, conn, if_exists="replace")

        self.__logger.info("Finished converting to SQLite DB")
