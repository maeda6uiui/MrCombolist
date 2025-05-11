import argparse
import yaml
from logging import getLogger, config,Logger

if __name__=="__main__":
    #Set up logger
    with open("./logging_config.yaml", "r", encoding="utf-8") as r:
        logging_config = yaml.safe_load(r)

    config.dictConfig(logging_config)

    logger=getLogger(__name__)

    #Parse arguments =====
    parser=argparse.ArgumentParser()
    subparsers=parser.add_subparsers()

    #Unarchive
    parser_unarchive=subparsers.add_parser("unarchive")
    parser_unarchive.add_argument("-i", "--input-root-dirname", type=str)
    parser_unarchive.add_argument("-o", "--output-root-dirname", type=str)
    parser_unarchive.add_argument("-l", "--log-dirname", type=str)
    parser_unarchive.add_argument("--start-index", type=int)
    parser_unarchive.add_argument("--end-index", type=int)
    parser_unarchive.add_argument("--unrar-tool-filepath", type=str, default="./Bin/UnRAR.exe")

    #Flatten
    parser_flatten=subparsers.add_parser("flatten")
    parser_flatten.add_argument("-i", "--input-root-dirname", type=str)
    parser_flatten.add_argument("-o", "--output-dirname", type=str)
    parser_flatten.add_argument("-l", "--log-filepath", type=str)

    #Split
    parser_split=subparsers.add_parser("split")
    parser_split.add_argument("-i", "--input-dirname", type=str)
    parser_split.add_argument("-o", "--output-dirname", type=str)
    parser_split.add_argument("-n", "--num-lines-per-split", type=int)
    parser_split.add_argument("--start-index", type=int)
    parser_split.add_argument("--end-index", type=int)

    #Regroup
    parser_regroup=subparsers.add_parser("regroup")
    parser_regroup.add_argument("-i", "--input-dirname", type=str)
    parser_regroup.add_argument("-o", "--output-dirname", type=str)
    parser_regroup.add_argument("-l", "--log-dirname", type=str)
    parser_regroup.add_argument("-n", "--num-files-per-group", type=int)
    parser_regroup.add_argument("--start-index", type=int)
    parser_regroup.add_argument("--end-index", type=int)

    #Rearchive
    parser_rearchive=subparsers.add_parser("rearchive")
    parser_rearchive.add_argument("-i", "--input-dirname", type=str)
    parser_rearchive.add_argument("-o", "--output-dirname", type=str)
    parser_rearchive.add_argument("--start-index", type=int)
    parser_rearchive.add_argument("--end-index", type=int)

    #Detect schema
    parser_detect_schema=subparsers.add_parser("detect-schema")
    parser_detect_schema.add_argument("-i", "--input-dirname", type=str)
    parser_detect_schema.add_argument("-l", "--log-dirname", type=str)
    parser_detect_schema.add_argument("-d", "--delimiter-candidates", type=str, default=":;|, \t")
    parser_detect_schema.add_argument("--max-num-lines", type=int, default=10000000)
    parser_detect_schema.add_argument("--max-line-length", type=int, default=200)
    parser_detect_schema.add_argument("--start-index", type=int)
    parser_detect_schema.add_argument("--end-index", type=int)

    #Parser
    parser_parse=subparsers.add_parser("parse")
    parser_parse.add_argument("-i", "--input-dirname", type=str)
    parser_parse.add_argument("-o", "--output-root-dirname", type=str)
    parser_parse.add_argument("-l", "--schema-detection-log-dirname", type=str)
    parser_parse.add_argument("--max-line-length", type=int, default=200)
    parser_parse.add_argument("--start-index", type=int)
    parser_parse.add_argument("--end-index", type=int)

    #Cleanup
    parser_cleanup=subparsers.add_parser("cleanup")
    parser_cleanup.add_argument("-i", "--input-root-dirname", type=str)
    parser_cleanup.add_argument("-o", "--output-root-dirname", type=str)
    parser_cleanup.add_argument("--start-index", type=int)
    parser_cleanup.add_argument("--end-index", type=int)

    #Convert to parquet
    parser_to_parquet=subparsers.add_parser("to-parquet")
    parser_to_parquet.add_argument("-i", "--input-root-dirname", type=str)
    parser_to_parquet.add_argument("-o", "--output-dirname", type=str)
    parser_to_parquet.add_argument("--start-index", type=int)
    parser_to_parquet.add_argument("--end-index", type=int)

    #Count local frequencies
    parser_count_local_freqs=subparsers.add_parser("count-local-freqs")
    parser_count_local_freqs.add_argument("-i", "--input-dirname", type=str)
    parser_count_local_freqs.add_argument("-o", "--output-root-dirname", type=str)
    parser_count_local_freqs.add_argument("--start-index", type=int)
    parser_count_local_freqs.add_argument("--end-index", type=int)

    #Convert to SQLite DB
    parser_to_sql=subparsers.add_parser("to-sql")
    parser_to_sql.add_argument("-i", "--input-dirname", type=str)
    parser_to_sql.add_argument("-o", "--output-dirname", type=str)
    parser_to_sql.add_argument("--email-freqs-db", action="store_true")
    parser_to_sql.add_argument("--poh-freqs-db", action="store_true")
    parser_to_sql.add_argument("--personae-db", action="store_true")
    parser_to_sql.add_argument("--start-index", type=int)
    parser_to_sql.add_argument("--end-index", type=int)

    #Merge freqs
    parser_merge_freqs=subparsers.add_parser("merge-freqs")
    parser_merge_freqs.add_argument("-i", "--input-dirname", type=str)
    parser_merge_freqs.add_argument("-o", "--output-filepath", type=str)
    parser_merge_freqs.add_argument("--gather-local", action="store_true")
    parser_merge_freqs.add_argument("--merge-local", action="store_true")

    #Concat personae
    parser_concat_personae=subparsers.add_parser("concat-personae")
    parser_concat_personae.add_argument("-i", "--input-dirname", type=str)
    parser_concat_personae.add_argument("-o", "--output-filepath", type=str)

    #Collect cleanup error records
    parser_collect_cleanup_error_records=subparsers.add_parser("collect-cleanup-error-records")
    parser_collect_cleanup_error_records.add_argument("-ip", "--parsing-root-dirname", type=str)
    parser_collect_cleanup_error_records.add_argument("-ic", "--cleanup-root-dirname", type=str)
    parser_collect_cleanup_error_records.add_argument("-o", "--output-dirname", type=str)

    #Inquire
    parser_inquire=subparsers.add_parser("inquire")
    parser_inquire.add_argument("-i", "--db-filepath", type=str)
    parser_inquire.add_argument("-o", "--output-filepath", type=str)
    parser_inquire.add_argument("-q", "--query", type=str)
    parser_inquire.add_argument("--max-num-records-to-print", type=int, default=10)
    parser_inquire.add_argument("--num-records-for-logging", type=int, default=1000000)
    #==========

