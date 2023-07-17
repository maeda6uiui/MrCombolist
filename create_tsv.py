import argparse
import hashlib
import json
import re
from pathlib import Path
from tqdm import tqdm

def get_md5_hash(value:str)->str:
    return hashlib.md5(value.encode()).hexdigest()

def parse_email_poh(line:str,delimiter:str)->dict[str,str]:
    parse_result={
        "email": "",
        "poh": "",
        "result": "",
        "error": ""
    }

    splits=line.split(delimiter,maxsplit=1)
    if len(splits)<2:
        parse_result["result"]="Failure"
        parse_result["error"]="Invalid record"
    else:
        parse_result["email"]=splits[0]
        parse_result["poh"]=splits[1]
        parse_result["result"]="Success"

    return parse_result

def parse_poh_email(line:str,delimiter:str)->dict[str,str]:
    parse_result={
        "email": "",
        "poh": "",
        "result": "",
        "error": ""
    }

    splits=line.split(delimiter,maxsplit=1)
    if len(splits)<2:
        parse_result["result"]="Failure"
        parse_result["error"]="Invalid record"
    else:
        parse_result["email"]=splits[1]
        parse_result["poh"]=splits[0]
        parse_result["result"]="Success"

    return parse_result

def parse_unknown_email_unknown(line:str,delimiter:str,r_email_middle:re.Pattern)->dict[str,str]:
    parse_result={
        "email": "",
        "poh": "",
        "result": "",
        "error": ""
    }

    m=r_email_middle.search(line)
    if m is None:
        parse_result["result"]="Failure"
        parse_result["error"]="Invalid record"
    else:
        email=m.group().replace(delimiter,"")
        parse_result["email"]=email
        parse_result["result"]="Success"

    return parse_result

def parse_email(line:str)->dict[str,str]:
    return {
        "email": line,
        "poh": "",
        "result": "Success",
        "error": ""
    }

def main(args):
    schema_detection_results_filepath:str=args.schema_detection_results_filepath
    output_root_dirname:str=args.output_root_dirname

    output_root_dir=Path(output_root_dirname)
    output_root_dir.mkdir(exist_ok=True,parents=True)

    with open(schema_detection_results_filepath,"r",encoding="utf-8") as r:
        schema_detection_results=json.load(r)

    parse_info_list=[]
    for schema_detection_result in tqdm(schema_detection_results):
        input_filepath=schema_detection_result["filepath"]
        input_file=Path(input_filepath)

        schema=schema_detection_result["schema"]
        placement:str=schema["placement"]
        delimiter:str=schema["delimiter"]

        r_email_middle=re.compile(f"[{delimiter}]+"+r"\S+@\S+\.\S+"+f"[{delimiter}]")

        with input_file.open("r",encoding="utf-8",errors="replace") as r:
            lines=r.read().splitlines()

        #Remove extra spaces
        lines=[line.strip() for line in lines]

        #Get email and poh
        records=[]
        parse_errors=[]
        for idx,line in enumerate(lines):
            parse_result=None
            if placement=="email:poh":
                parse_result=parse_email_poh(line,delimiter)
            elif placement=="poh:email":
                parse_result=parse_poh_email(line,delimiter)
            elif placement=="unknown:email:unknown":
                parse_result=parse_unknown_email_unknown(line,delimiter,r_email_middle)
            elif placement=="email":
                parse_result=parse_email(line)
            else:
                continue

            if parse_result["result"]=="Failure":
                parse_error={
                    "index": idx,
                    "error": parse_result["error"]
                }
                parse_errors.append(parse_error)
            else:
                record={
                    "email": parse_result["email"],
                    "poh": parse_result["poh"]
                }
                records.append(record)

        #Output records and errors
        input_dirname=input_file.parent.name
        input_dirname_hash=get_md5_hash(input_dirname)
        output_dir=output_root_dir.joinpath(input_dirname_hash)
        output_dir.mkdir(exist_ok=True)

        input_filepath_hash=get_md5_hash(input_filepath)
        records_file=output_dir.joinpath(f"{input_filepath_hash}.tsv")
        parse_errors_file=output_dir.joinpath(f"{input_filepath_hash}_errors.json")

        with records_file.open("w",encoding="utf-8") as w:
            w.write("email\tpoh\n")

            for record in records:
                email=record["email"]
                poh=record["poh"]

                w.write(f"{email}\t{poh}\n")

        with parse_errors_file.open("w",encoding="utf-8") as w:
            json.dump(parse_errors,w,ensure_ascii=False,indent=4)

        parse_info={
            "input_filepath": input_filepath,
            "input_filepath_hash": input_filepath_hash,
            "input_dirname_hash": input_dirname_hash,
            "records_filepath": str(records_file),
            "parse_errors_filepath": str(parse_errors_file)
        }
        parse_info_list.append(parse_info)

    parse_info_file=output_root_dir.joinpath("parse_info.json")
    with parse_info_file.open("w",encoding="utf-8") as w:
        json.dump(parse_info_list,w,ensure_ascii=False,indent=4)

if __name__=="__main__":
    parser=argparse.ArgumentParser()
    parser.add_argument("-i","--schema-detection-results-filepath",type=str)
    parser.add_argument("-o","--output-root-dirname",type=str)
    args=parser.parse_args()

    main(args)
