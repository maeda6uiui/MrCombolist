import argparse
import csv
import pandas as pd
from pathlib import Path
from tqdm import tqdm

def main(args):
    concat_dirname:str=args.concat_dirname
    output_dirname:str=args.output_dirname

    concat_dir=Path(concat_dirname)
    tsv_files=list(concat_dir.glob("*.tsv"))

    output_dir=Path(output_dirname)
    output_dir.mkdir(exist_ok=True,parents=True)

    for tsv_file in tqdm(tsv_files):
        df=pd.read_table(tsv_file,encoding="utf-8",quoting=csv.QUOTE_NONE)

        parquet_file=output_dir.joinpath(f"{tsv_file.stem}.parquet")
        df.to_parquet(parquet_file,index=False)

if __name__=="__main__":
    parser=argparse.ArgumentParser()
    parser.add_argument("-i","--concat-dirname",type=str)
    parser.add_argument("-o","--output-dirname",type=str)
    args=parser.parse_args()

    main(args)
