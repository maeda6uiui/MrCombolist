import argparse
import json
import shutil
import yaml
from logging import getLogger, config
from pathlib import Path
from tqdm import tqdm


def main(args):
    input_root_dirname: str = args.input_root_dirname
    output_dirname: str = args.output_dirname
    log_filepath: str = args.log_filepath

    # Set up logger
    with open("./logging_config.yaml", "r", encoding="utf-8") as r:
        logging_config = yaml.safe_load(r)

    config.dictConfig(logging_config)

    logger = getLogger(__name__)
    logger.debug(args)

    # Get list of all "text" files in the input directory
    # AFAIK, most combolists are comprised of text files,
    # but you may want some extra processing in case the combolist
    # contains other formats of data, such as JSON and SQL
    input_root_dir = Path(input_root_dirname)
    input_files = input_root_dir.glob("**/*.txt")
    input_files = [f for f in input_files if f.is_file()]
    input_files.sort()

    logger.info(f"{len(input_files)} files exist in the input directory")

    # Create output directory
    output_dir = Path(output_dirname)
    output_dir.mkdir(exist_ok=True, parents=True)

    # Flatten
    logger.info("Start flattening the files...")

    flattening_results: list[dict] = []

    for idx, input_file in enumerate(tqdm(input_files)):
        # Copy the file
        output_file = output_dir.joinpath(f"{idx}.txt")
        shutil.copy(input_file, output_file)

        # Store the correspondence of the input and the output files
        flatten_result = {"index": idx, "input_filepath": str(input_file)}
        flattening_results.append(flatten_result)

    logger.info("Finished flattening the files")

    # Output flatten results to a log file
    log_file = Path(log_filepath)
    with log_file.open("w", encoding="utf-8") as w:
        json.dump(flattening_results, w, ensure_ascii=False)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-i", "--input-root-dirname", type=str)
    parser.add_argument("-o", "--output-dirname", type=str)
    parser.add_argument("-l", "--log-filepath", type=str)
    args = parser.parse_args()

    main(args)
