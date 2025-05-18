import gzip
import random
from logging import Logger
from pathlib import Path
from tqdm import tqdm


class MCGeneratePseudoCombos:
    def __init__(
        self,
        word_list_filepath: str,
        limit_num_words: int,
        email_domains: str,
        email_local_part_length: int,
        delimiter: str,
        num_combos: int,
        output_filepath: str,
        logger: Logger,
    ):
        self.__word_list_filepath = word_list_filepath
        self.__limit_num_words = limit_num_words
        self.__email_domains = email_domains
        self.__email_local_part_length = email_local_part_length
        self.__delimiter = delimiter
        self.__num_combos = num_combos
        self.__output_filepath = output_filepath
        self.__logger = logger

    def run(self):
        self.__logger.info("Loading word list...")

        words: list[str] = []
        with gzip.open(
            self.__word_list_filepath, "rt", encoding="utf-8", errors="replace"
        ) as rt:
            for word in rt:
                word = word.strip()
                words.append(word)

                if self.__limit_num_words > 0 and len(words) >= self.__limit_num_words:
                    break

        self.__logger.info("Start generating combos...")
        combos: list[str] = []

        rnd_max = 10**self.__email_local_part_length  # Exclusive
        lst_email_domains = self.__email_domains.split(",")

        for _ in tqdm(range(self.__num_combos)):
            rnd = random.randrange(0, rnd_max)
            email_local_part = f"{rnd:0{self.__email_local_part_length}d}"
            email_domain = random.choice(lst_email_domains)
            email_address = f"{email_local_part}@{email_domain}"

            password = random.choice(words)

            combo = f"{email_address}{self.__delimiter}{password}"
            combos.append(combo)

        self.__logger.info("Output generated combos to a text file")

        output_file = Path(self.__output_filepath)
        output_dir = output_file.parent
        output_dir.mkdir(exist_ok=True, parents=True)

        with output_file.open("wt", encoding="utf-8") as wt:
            for combo in combos:
                wt.write(f"{combo}\n")
