import sqlite3
from logging import Logger
from pathlib import Path

class MCConcatPersonae:
    def __init__(
        self,
        input_dirname: str,
        output_filepath: str,
        logger:Logger):
        self.__input_dirname=input_dirname
        self.__output_filepath=output_filepath
        self.__logger=logger

    def run(self):
        # Get input files
        input_dir = Path(self.__input_dirname)
        input_files = list(input_dir.glob("*.db"))
        input_files.sort()

        self.__logger.info(f"{len(input_files)} files exist in the input directory")

        # Concatenate personae
        self.__logger.info("Start concatenating personae...")
        with sqlite3.connect(self.__output_filepath) as conn:
            cur = conn.cursor()

            # Create table to concatenate personae
            cur.execute(
                """
                CREATE TABLE personae (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    email STRING NOT NULL,
                    poh STRING NOT NULL
                );
                """
            )
            conn.commit()

            # Insert records from each DB
            for input_file in input_files:
                self.__logger.info(f"Processing '{input_file.name}'")

                # Attach as temp DB
                cur.execute(f"ATTACH DATABASE '{str(input_file)}' AS tmpdb;")

                # Insert records
                cur.execute(
                    """
                    INSERT INTO personae (email,poh)
                    SELECT email,poh
                    FROM tmpdb.personae
                    WHERE email IS NOT NULL AND poh IS NOT NULL;
                    """
                )
                conn.commit()

                # Detach temp DB
                cur.execute("DETACH tmpdb;")

        self.__logger.info("Finished concatenating personae")
