import pandas as pd
from logging import Logger
from pathlib import Path

class MCCountLocalFreqs:
    def __init__(
        self,
        input_dirname: str,
        output_root_dirname: str,
        start_index: int,
        end_index: int,
        logger:Logger):
        self.input_dirname=input_dirname
        self.output_root_dirname=output_root_dirname
        self.start_index=start_index
        self.end_index=end_index
        self.logger=logger

    def run(self):
        # Get all parquet files in the input directory
        input_dir = Path(self.input_dirname)
        input_files = list(input_dir.glob("*.parquet"))
        input_files.sort()

        self.logger.info(f"{len(input_files)} files exist in the input directory")

        # Create output directories
        output_root_dir = Path(self.output_root_dirname)
        output_root_dir.mkdir(exist_ok=True, parents=True)

        email_output_dir = output_root_dir.joinpath("Email")
        email_output_dir.mkdir(exist_ok=True)

        poh_output_dir = output_root_dir.joinpath("PoH")
        poh_output_dir.mkdir(exist_ok=True)

        # Create a subset of the list if either the start or the end index is specified
        start_index = start_index if start_index is not None else 0
        end_index = end_index if end_index is not None else len(input_files)

        input_files = input_files[start_index:end_index]

        # Count local frequencies
        self.logger.info("Start counting local frequencies...")
        for input_file in input_files:
            self.logger.info(f"Processing '{input_file.name}'")

            # Load input file
            df = pd.read_parquet(input_file)

            # Count value frequencies of "email" column and save it to a file
            df_freq_email = (
                df["email"]
                .value_counts()
                .to_frame(name="freq")
                .reset_index(names=["email"])
            )
            email_output_file = email_output_dir.joinpath(input_file.name)
            df_freq_email.to_parquet(email_output_file, index=False)

            # Count value frequencies of "poh" column and save it to a file
            df_freq_poh = (
                df["poh"].value_counts().to_frame(name="freq").reset_index(names=["poh"])
            )
            poh_output_file = poh_output_dir.joinpath(input_file.name)
            df_freq_poh.to_parquet(poh_output_file, index=False)

        self.logger.info("Finished counting local frequencies")
