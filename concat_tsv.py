import argparse
import json
from pathlib import Path
from tqdm import tqdm

def main(args):
    parse_info_filepath:str=args.parse_info_filepath
    output_dirname:str=args.output_dirname
    num_tsv_per_concat:int=args.num_tsv_per_concat
    start_index:int=args.start_index

    output_dir=Path(output_dirname)
    output_dir.mkdir(exist_ok=True,parents=True)

    with open(parse_info_filepath,"r",encoding="utf-8") as r:
        parse_info_list=json.load(r)

    num_input_files=len(parse_info_list)
    num_output_files=num_input_files//num_tsv_per_concat
    if num_input_files%num_tsv_per_concat!=0:
        num_output_files+=1

    if start_index is None:
        start_index=0

    for i in range(start_index,num_output_files):
        print(f"{i}/{num_output_files-1}")

        concat_lines=[]

        header="email\tpoh\tinput_filepath_hash\n"
        concat_lines.append(header)

        num_tsv_to_concat=num_tsv_per_concat if i<num_output_files-1 else num_input_files%num_tsv_per_concat
        for j in tqdm(range(num_tsv_to_concat)):
            parse_info=parse_info_list[i*num_tsv_per_concat+j]
            records_filepath:str=parse_info["records_filepath"]
            input_filepath_hash:str=parse_info["input_filepath_hash"]

            with open(records_filepath,"r",encoding="utf-8") as r:
                for idx,line in enumerate(r):
                    if idx==0:
                        continue

                    line=line.strip()
                    line+=f"\t{input_filepath_hash}\n"
                    concat_lines.append(line)

        output_file=output_dir.joinpath(f"{i}.tsv")
        with output_file.open("w",encoding="utf-8") as w:
            w.writelines(concat_lines)

if __name__=="__main__":
    parser=argparse.ArgumentParser()
    parser.add_argument("-i","--parse-info-filepath",type=str)
    parser.add_argument("-o","--output-dirname",type=str)
    parser.add_argument("-n","--num-tsv-per-concat",type=int,default=200)
    parser.add_argument("-s","--start-index",type=int)
    args=parser.parse_args()

    main(args)
