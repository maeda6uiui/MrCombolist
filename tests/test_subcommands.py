import pytest
import shutil
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
from subcommands.to_sqlite import MCToSQLite
from subcommands.merge_freqs import MCMergeFreqs
from subcommands.concat_personae import MCConcatPersonae
from subcommands.inquire import MCInquire
from subcommands.collect_parsing_error_records import MCCollectParsingErrorRecords
from subcommands.collect_cleanup_error_records import MCCollectCleanupErrorRecords
from subcommands.generate_pseudo_combos import MCGeneratePseudoCombos
from logging import getLogger
from pathlib import Path

logger = getLogger(__name__)


@pytest.fixture(scope="session", autouse=True)
def remove_test_artifacts():
    dirnames = [
        "Unarchive",
        "Flatten",
        "Split",
        "Regroup",
        "Rearchive",
        "DetectSchema",
        "Parse",
        "Cleanup",
        "Parquet",
        "CountLocalFreqs",
        "SQLite",
        "ParsingErrorRecords",
        "CleanupErrorRecords",
    ]
    for dirname in dirnames:
        dir_path = Path(f"./tests/Data/{dirname}")
        if dir_path.exists():
            shutil.rmtree(f"./tests/Data/{dirname}")

    Path("./tests/Data/pseudo_combos.txt").unlink(missing_ok=True)


@pytest.mark.order(0)
def test_unarchive():
    runner = MCUnarchive(
        "./tests/Data/Original",
        "./tests/Data/Unarchive",
        "./tests/Data/Log/Unarchive",
        None,
        None,
        "./Bin/UnRAR.exe",
        logger,
    )
    runner.run()


@pytest.mark.order(1)
def test_flatten():
    runner = MCFlatten(
        "./tests/Data/Unarchive",
        "./tests/Data/Flatten",
        "./tests/Data/Log/flatten.json",
        logger,
    )
    runner.run()


@pytest.mark.order(2)
def test_split():
    runner = MCSplit("./tests/Data/Flatten", "./tests/Data/Split", 50000, logger)
    runner.run()


@pytest.mark.order(3)
def test_regroup():
    runner = MCRegroup(
        "./tests/Data/Split",
        "./tests/Data/Regroup",
        "./tests/Data/Log/Regroup",
        4,
        None,
        None,
        logger,
    )
    runner.run()


@pytest.mark.order(4)
def test_rearchive():
    runner = MCRearchive(
        "./tests/Data/Regroup", "./tests/Data/Rearchive", None, None, logger
    )
    runner.run()


@pytest.mark.order(5)
def test_detect_schema():
    runner = MCDetectSchema(
        "./tests/Data/Rearchive",
        "./tests/Data/Log/DetectSchema",
        ":;|, \t",
        10000000,
        200,
        None,
        None,
        logger,
    )
    runner.run()


@pytest.mark.order(6)
def test_parse():
    runner = MCParse(
        "./tests/Data/Rearchive",
        "./tests/Data/Parse",
        "./tests/Data/Log/DetectSchema",
        200,
        None,
        None,
        logger,
    )
    runner.run()


@pytest.mark.order(7)
def test_cleanup():
    runner = MCCleanup("./tests/Data/Parse", "./tests/Data/Cleanup", None, None, logger)
    runner.run()


@pytest.mark.order(8)
def test_to_parquet():
    runner = MCToParquet(
        "./tests/Data/Cleanup", "./tests/Data/Parquet", None, None, logger
    )
    runner.run()


@pytest.mark.order(9)
def test_count_local_freqs():
    runner = MCCountLocalFreqs(
        "./tests/Data/Parquet", "./tests/Data/CountLocalFreqs", None, None, logger
    )
    runner.run()


@pytest.mark.order(10)
def test_to_sqlite():
    runner = MCToSQLite(
        "./tests/Data/CountLocalFreqs/Email",
        "./tests/Data/SQLite/Email",
        True,
        False,
        False,
        None,
        None,
        logger,
    )
    runner.run()

    runner = MCToSQLite(
        "./tests/Data/CountLocalFreqs/PoH",
        "./tests/Data/SQLite/PoH",
        False,
        True,
        False,
        None,
        None,
        logger,
    )
    runner.run()

    runner = MCToSQLite(
        "./tests/Data/Parquet",
        "./tests/Data/SQLite/Personae",
        False,
        False,
        True,
        None,
        None,
        logger,
    )
    runner.run()


@pytest.mark.order(11)
def test_merge_freqs():
    runner = MCMergeFreqs(
        "./tests/Data/SQLite/Email",
        "./tests/Data/SQLite/email_freqs.db",
        True,
        True,
        logger,
    )
    runner.run()

    runner = MCMergeFreqs(
        "./tests/Data/SQLite/PoH",
        "./tests/Data/SQLite/poh_freqs.db",
        True,
        True,
        logger,
    )
    runner.run()


@pytest.mark.order(12)
def test_concat_personae():
    runner = MCConcatPersonae(
        "./tests/Data/SQLite/Personae", "./tests/Data/SQLite/personae.db", logger
    )
    runner.run()


@pytest.mark.order(13)
def test_inquire():
    runner = MCInquire(
        "./tests/Data/SQLite/poh_freqs.db",
        None,
        "SELECT word,freq FROM freqs ORDER BY freq DESC LIMIT 5;",
        10,
        1000000,
        logger,
    )
    records = runner.get()

    assert len(records) == 5
    assert records[0] == (0, 177)
    assert records[1] == (147852369, 72)
    assert records[2] == (14789, 67)
    assert records[3] == (112233, 65)
    assert records[4] == (98765, 63) or records[4] == (123456, 63)


@pytest.mark.order(100)
def test_collect_parsing_error_records():
    runner = MCCollectParsingErrorRecords(
        "./tests/Data/Rearchive",
        "./tests/Data/Parse",
        "./tests/Data/ParsingErrorRecords",
        logger,
    )
    runner.run()


@pytest.mark.order(101)
def test_collect_cleanup_error_records():
    runner = MCCollectCleanupErrorRecords(
        "./tests/Data/Parse",
        "./tests/Data/Cleanup",
        "./tests/Data/CleanupErrorRecords",
        logger,
    )
    runner.run()

@pytest.mark.order(200)
def test_generate_pseudo_combos():
    runner = MCGeneratePseudoCombos(
        "./Data/rockyou_1M.txt.gz",
        -1,
        "example.com",
        4,
        ":",
        1000000,
        "./tests/Data/pseudo_combos.txt",
        logger,
    )
    runner.run()
