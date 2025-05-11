import pandas as pd
import sqlite3
from logging import Logger
from pathlib import Path

class MCToSQL:
    def __init__(
        self,
        input_dirname: str,
        output_dirname: str,
        email_freqs_db: bool,
        poh_freqs_db: bool,
        personae_db: bool,
        start_index: int,
        end_index: int,
        logger:Logger):
        self.input_dirname=input_dirname
        self.output_dirname=output_dirname
        self.email_freqs_db=email_freqs_db
        self.poh_freqs_db=poh_freqs_db
        self.personae_db=personae_db
        self.start_index=start_index
        self.end_index=end_index
        self.logger=logger

    def run(self):
        # Get all parquet files in the input directory
        input_dir = Path(self.input_dirname)
        input_files = list(input_dir.glob("*.parquet"))
        input_files.sort()

        self.logger.info(f"{len(input_files)} files exist in the input directory")

        # Create output directory
        output_dir = Path(self.output_dirname)
        output_dir.mkdir(exist_ok=True, parents=True)

        # Create a subset of the list if either the start or the end index is specified
        start_index = start_index if start_index is not None else 0
        end_index = end_index if end_index is not None else len(input_files)

        input_files = input_files[start_index:end_index]

        # Determine value column name and table name
        val_column_name = ""
        table_name = ""

        if self.email_freqs_db:
            val_column_name = "email"
            table_name = "freqs"
        elif self.poh_freqs_db:
            val_column_name = "poh"
            table_name = "freqs"
        elif self.personae_db:
            table_name = "personae"
        else:
            self.logger.error("Must specify the type of DB to be created")
            return

        # Convert to sql
        self.logger.info("Start converting to sql...")
        for input_file in input_files:
            self.logger.info(f"Processing '{input_file.name}'")

            # Load input file
            df = pd.read_parquet(input_file)

            # Rename value column to "word"
            if val_column_name != "":
                df.rename(columns={val_column_name: "word"}, inplace=True)

            # Create connection and output to sql
            db_file = output_dir.joinpath(f"{input_file.stem}.db")
            with sqlite3.connect(db_file) as conn:
                df.to_sql(table_name, conn, if_exists="replace")

        self.logger.info("Finished converting to sql")
