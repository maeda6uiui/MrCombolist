import sqlite3
from logging import Logger
from pathlib import Path

class MCMergeFreqs:
    def __init__(
        self,
        input_dirname: str,
        output_filepath: str,
        gather_local: bool,
        merge_local: bool,
        logger:Logger):
        self.input_dirname=input_dirname
        self.output_filepath=output_filepath
        self.gather_local=gather_local
        self.merge_local=merge_local
        self.logger=logger

    def __fn_gather_local(self,input_dirname: str, output_filepath: str):
        # Get input files
        input_dir = Path(input_dirname)
        input_files = list(input_dir.glob("*.db"))
        input_files.sort()

        self.logger.info(f"{len(input_files)} files exist in the input directory")

        # Create table to gather local frequencies
        self.logger.info("Creating table to gather local frequencies...")
        with sqlite3.connect(output_filepath) as conn:
            cur = conn.cursor()

            cur.execute(
                """
                CREATE TABLE local_freqs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    word STRING NOT NULL,
                    freq INTEGER NOT NULL
                );
                """
            )
            conn.commit()

        # Insert all records of the local frequency tables into the gathering table
        self.logger.info("Start gathering records from local frequency tables...")
        with sqlite3.connect(output_filepath) as conn:
            cur = conn.cursor()

            for input_file in input_files:
                self.logger.info(f"Processing '{input_file.name}'")

                # Attach temp DB
                cur.execute(f"ATTACH DATABASE '{str(input_file)}' AS tmpdb;")

                # Insert records
                cur.execute(
                    """
                    INSERT INTO local_freqs (word,freq)
                    SELECT word,freq
                    FROM tmpdb.freqs;
                    """
                )
                conn.commit()

                # Detach temp DB
                cur.execute("DETACH tmpdb;")

        self.logger.info("Finished gathering records from local frequency tables")

    def __fn_merge_local(self,output_filepath: str):
        # Merge local frequencies
        self.logger.info("Start merging local frequencies...")
        with sqlite3.connect(output_filepath) as conn:
            cur = conn.cursor()

            # Create table to merge frequencies
            cur.execute(
                """
                CREATE TABLE freqs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    word STRING NOT NULL,
                    freq INTEGER NOT NULL
                );
                """
            )
            conn.commit()

            # Consolidate local frequencies
            cur.execute(
                """
                INSERT INTO freqs (word,freq)
                SELECT word,SUM(freq)
                FROM local_freqs
                GROUP BY word;
                """
            )
            conn.commit()

            # Remove table for local frequencies
            cur.execute("DROP TABLE local_freqs;")
            conn.commit()

        self.logger.info("Finished merging local frequencies")

    def run(self):
        if self.gather_local:
            self.__fn_gather_local(self.input_dirname, self.output_filepath)
        if self.merge_local:
            self.__fn_merge_local(self.output_filepath)
