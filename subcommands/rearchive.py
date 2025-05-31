import subprocess
from logging import Logger
from pathlib import Path


class MCRearchive:
    def __init__(
        self,
        input_dirname: str,
        output_dirname: str,
        start_index: int,
        end_index: int,
        pigz_filepath:str,
        logger: Logger,
    ):
        self.__input_dirname = input_dirname
        self.__output_dirname = output_dirname
        self.__start_index = start_index
        self.__end_index = end_index
        self.__pigz_filepath=pigz_filepath
        self.__logger = logger


    def __compress_with_pigz(self,input_file:Path,output_file:Path):
        #First, compress the file with pigz
        #and output it in the same directory as the src file
        result=subprocess.run([
            self.__pigz_filepath,
            "-k",
            str(input_file)
        ])
        if result.returncode!=0:
            self.__logger.error(f"pigz returned code {result.returncode}")
            raise RuntimeError("pigz error")
        
        #Move the compressed file to the output directory
        tmp_output_file=input_file.with_suffix(".txt.gz")
        tmp_output_file.rename(output_file)

    def run(self):
        # Get all text files in the input directory
        input_dir = Path(self.__input_dirname)
        input_files = list(input_dir.glob("*.txt"))
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

        # Rearchive
        self.__logger.info("Start rearchiving the files...")
        for input_file in input_files:
            self.__logger.info(f"Creating archive of {input_file.name}")

            output_file = output_dir.joinpath(f"{input_file.name}.gz")
            self.__compress_with_pigz(input_file,output_file)

        self.__logger.info("Finished rearchiving the files")
