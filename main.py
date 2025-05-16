import argparse
import yaml
from logging import getLogger, config, Logger
from subcommands.unarchive import MCUnarchive
from subcommands.flatten import MCFlatten
from subcommands.split import MCSplit
from subcommands.regroup import MCRegroup
from subcommands.rearchive import MCRearchive
from subcommands.detect_schema import MCDetectSchema
from subcommands.parse import MCParse
from subcommands.cleanup import MCCleanup
from subcommands.to_parquet import MCToParquet
from subcommands.count_local_freqs import MCCountLocalFreqs
from subcommands.to_sql import MCToSQL
from subcommands.merge_freqs import MCMergeFreqs
from subcommands.concat_personae import MCConcatPersonae
from subcommands.collect_cleanup_error_records import MCCollectCleanupErrorRecords
from subcommands.inquire import MCInquire


def unarchive(args, logger: Logger):
    runner = MCUnarchive(
        args.input_root_dirname,
        args.output_root_dirname,
        args.log_dirname,
        args.start_index,
        args.end_index,
        args.unrar_tool_filepath,
        logger,
    )
    runner.run()


def flatten(args, logger: Logger):
    runner = MCFlatten(
        args.input_root_dirname, args.output_dirname, args.log_filepath, logger
    )
    runner.run()


def split(args, logger: Logger):
    runner = MCSplit(
        args.input_dirname,
        args.output_dirname,
        args.num_lines_per_split,
        logger,
    )
    runner.run()


def regroup(args, logger: Logger):
    runner = MCRegroup(
        args.input_dirname,
        args.output_dirname,
        args.log_dirname,
        args.num_files_per_group,
        args.start_index,
        args.end_index,
        logger,
    )
    runner.run()


def rearchive(args, logger: Logger):
    runner = MCRearchive(
        args.input_dirname,
        args.output_dirname,
        args.start_index,
        args.end_index,
        logger,
    )
    runner.run()


def detect_schema(args, logger: Logger):
    runner = MCDetectSchema(
        args.input_dirname,
        args.log_dirname,
        args.delimiter_candidates,
        args.max_num_lines,
        args.max_line_length,
        args.start_index,
        args.end_index,
        logger,
    )
    runner.run()


def parse(args, logger: Logger):
    runner = MCParse(
        args.input_dirname,
        args.output_root_dirname,
        args.schema_detection_log_dirname,
        args.max_line_length,
        args.start_index,
        args.end_index,
        logger,
    )
    runner.run()


def cleanup(args, logger: Logger):
    runner = MCCleanup(
        args.input_root_dirname,
        args.output_root_dirname,
        args.start_index,
        args.end_index,
        logger,
    )
    runner.run()


def to_parquet(args, logger: Logger):
    runner = MCToParquet(
        args.input_root_dirname,
        args.output_dirname,
        args.start_index,
        args.end_index,
        logger,
    )
    runner.run()


def count_local_freqs(args, logger: Logger):
    runner = MCCountLocalFreqs(
        args.input_dirname,
        args.output_root_dirname,
        args.start_index,
        args.end_index,
        logger,
    )
    runner.run()


def to_sql(args, logger: Logger):
    runner = MCToSQL(
        args.input_dirname,
        args.output_dirname,
        args.email_freqs_db,
        args.poh_freqs_db,
        args.personae_db,
        args.start_index,
        args.end_index,
        logger,
    )
    runner.run()


def merge_freqs(args, logger: Logger):
    runner = MCMergeFreqs(
        args.input_dirname,
        args.output_filepath,
        args.gather_local,
        args.merge_local,
        logger,
    )
    runner.run()


def concat_personae(args, logger: Logger):
    runner = MCConcatPersonae(args.input_dirname, args.output_filepath, logger)
    runner.run()


def collect_cleanup_error_records(args, logger: Logger):
    runner = MCCollectCleanupErrorRecords(
        args.parsing_root_dirname,
        args.cleanup_root_dirname,
        args.output_dirname,
        logger,
    )
    runner.run()


def inquire(args, logger: Logger):
    runner = MCInquire(
        args.db_filepath,
        args.output_filepath,
        args.query,
        args.max_num_records_to_print,
        args.num_records_for_logging,
        logger,
    )
    runner.run()


if __name__ == "__main__":
    # Set up logger
    with open("./logging_config.yaml", "r", encoding="utf-8") as r:
        logging_config = yaml.safe_load(r)

    config.dictConfig(logging_config)

    logger = getLogger(__name__)

    # Create parsers =====
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers()

    # Unarchive
    parser_unarchive = subparsers.add_parser("unarchive")
    parser_unarchive.add_argument("-i", "--input-root-dirname", type=str)
    parser_unarchive.add_argument("-o", "--output-root-dirname", type=str)
    parser_unarchive.add_argument("-l", "--log-dirname", type=str)
    parser_unarchive.add_argument("--start-index", type=int)
    parser_unarchive.add_argument("--end-index", type=int)
    parser_unarchive.add_argument(
        "--unrar-tool-filepath", type=str, default="./Bin/UnRAR.exe"
    )
    parser_unarchive.set_defaults(handler=unarchive)

    # Flatten
    parser_flatten = subparsers.add_parser("flatten")
    parser_flatten.add_argument("-i", "--input-root-dirname", type=str)
    parser_flatten.add_argument("-o", "--output-dirname", type=str)
    parser_flatten.add_argument("-l", "--log-filepath", type=str)
    parser_flatten.set_defaults(handler=flatten)

    # Split
    parser_split = subparsers.add_parser("split")
    parser_split.add_argument("-i", "--input-dirname", type=str)
    parser_split.add_argument("-o", "--output-dirname", type=str)
    parser_split.add_argument("-n", "--num-lines-per-split", type=int)
    parser_split.set_defaults(handler=split)

    # Regroup
    parser_regroup = subparsers.add_parser("regroup")
    parser_regroup.add_argument("-i", "--input-dirname", type=str)
    parser_regroup.add_argument("-o", "--output-dirname", type=str)
    parser_regroup.add_argument("-l", "--log-dirname", type=str)
    parser_regroup.add_argument("-n", "--num-files-per-group", type=int)
    parser_regroup.add_argument("--start-index", type=int)
    parser_regroup.add_argument("--end-index", type=int)
    parser_regroup.set_defaults(handler=regroup)

    # Rearchive
    parser_rearchive = subparsers.add_parser("rearchive")
    parser_rearchive.add_argument("-i", "--input-dirname", type=str)
    parser_rearchive.add_argument("-o", "--output-dirname", type=str)
    parser_rearchive.add_argument("--start-index", type=int)
    parser_rearchive.add_argument("--end-index", type=int)
    parser_rearchive.set_defaults(handler=rearchive)

    # Detect schema
    parser_detect_schema = subparsers.add_parser("detect-schema")
    parser_detect_schema.add_argument("-i", "--input-dirname", type=str)
    parser_detect_schema.add_argument("-l", "--log-dirname", type=str)
    parser_detect_schema.add_argument(
        "-d", "--delimiter-candidates", type=str, default=":;|, \t"
    )
    parser_detect_schema.add_argument("--max-num-lines", type=int, default=10000000)
    parser_detect_schema.add_argument("--max-line-length", type=int, default=200)
    parser_detect_schema.add_argument("--start-index", type=int)
    parser_detect_schema.add_argument("--end-index", type=int)
    parser_detect_schema.set_defaults(handler=detect_schema)

    # Parse
    parser_parse = subparsers.add_parser("parse")
    parser_parse.add_argument("-i", "--input-dirname", type=str)
    parser_parse.add_argument("-o", "--output-root-dirname", type=str)
    parser_parse.add_argument("-l", "--schema-detection-log-dirname", type=str)
    parser_parse.add_argument("--max-line-length", type=int, default=200)
    parser_parse.add_argument("--start-index", type=int)
    parser_parse.add_argument("--end-index", type=int)
    parser_parse.set_defaults(handler=parse)

    # Cleanup
    parser_cleanup = subparsers.add_parser("cleanup")
    parser_cleanup.add_argument("-i", "--input-root-dirname", type=str)
    parser_cleanup.add_argument("-o", "--output-root-dirname", type=str)
    parser_cleanup.add_argument("--start-index", type=int)
    parser_cleanup.add_argument("--end-index", type=int)
    parser_cleanup.set_defaults(handler=cleanup)

    # Convert to parquet
    parser_to_parquet = subparsers.add_parser("to-parquet")
    parser_to_parquet.add_argument("-i", "--input-root-dirname", type=str)
    parser_to_parquet.add_argument("-o", "--output-dirname", type=str)
    parser_to_parquet.add_argument("--start-index", type=int)
    parser_to_parquet.add_argument("--end-index", type=int)
    parser_to_parquet.set_defaults(handler=to_parquet)

    # Count local frequencies
    parser_count_local_freqs = subparsers.add_parser("count-local-freqs")
    parser_count_local_freqs.add_argument("-i", "--input-dirname", type=str)
    parser_count_local_freqs.add_argument("-o", "--output-root-dirname", type=str)
    parser_count_local_freqs.add_argument("--start-index", type=int)
    parser_count_local_freqs.add_argument("--end-index", type=int)
    parser_count_local_freqs.set_defaults(handler=count_local_freqs)

    # Convert to SQLite DB
    parser_to_sql = subparsers.add_parser("to-sql")
    parser_to_sql.add_argument("-i", "--input-dirname", type=str)
    parser_to_sql.add_argument("-o", "--output-dirname", type=str)
    parser_to_sql.add_argument("--email-freqs-db", action="store_true")
    parser_to_sql.add_argument("--poh-freqs-db", action="store_true")
    parser_to_sql.add_argument("--personae-db", action="store_true")
    parser_to_sql.add_argument("--start-index", type=int)
    parser_to_sql.add_argument("--end-index", type=int)
    parser_to_sql.set_defaults(handler=to_sql)

    # Merge freqs
    parser_merge_freqs = subparsers.add_parser("merge-freqs")
    parser_merge_freqs.add_argument("-i", "--input-dirname", type=str)
    parser_merge_freqs.add_argument("-o", "--output-filepath", type=str)
    parser_merge_freqs.add_argument("--gather-local", action="store_true")
    parser_merge_freqs.add_argument("--merge-local", action="store_true")
    parser_merge_freqs.set_defaults(handler=merge_freqs)

    # Concat personae
    parser_concat_personae = subparsers.add_parser("concat-personae")
    parser_concat_personae.add_argument("-i", "--input-dirname", type=str)
    parser_concat_personae.add_argument("-o", "--output-filepath", type=str)
    parser_concat_personae.set_defaults(handler=concat_personae)

    # Collect cleanup error records
    parser_collect_cleanup_error_records = subparsers.add_parser(
        "collect-cleanup-error-records"
    )
    parser_collect_cleanup_error_records.add_argument(
        "-ip", "--parsing-root-dirname", type=str
    )
    parser_collect_cleanup_error_records.add_argument(
        "-ic", "--cleanup-root-dirname", type=str
    )
    parser_collect_cleanup_error_records.add_argument(
        "-o", "--output-dirname", type=str
    )
    parser_collect_cleanup_error_records.set_defaults(
        handler=collect_cleanup_error_records
    )

    # Inquire
    parser_inquire = subparsers.add_parser("inquire")
    parser_inquire.add_argument("-i", "--db-filepath", type=str)
    parser_inquire.add_argument("-o", "--output-filepath", type=str)
    parser_inquire.add_argument("-q", "--query", type=str)
    parser_inquire.add_argument("--max-num-records-to-print", type=int, default=10)
    parser_inquire.add_argument("--num-records-for-logging", type=int, default=1000000)
    parser_inquire.set_defaults(handler=inquire)
    # ==========

    # Run subcommand
    args = parser.parse_args()
    if hasattr(args, "handler"):
        args.handler(args, logger)
    else:
        parser.print_help()
