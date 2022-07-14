#!/usr/bin/env python

import re
import string
from typing import Any, Union

import click
import pandas as pd
from loguru import logger

ICD_10_VALID_CHARS = set(string.ascii_uppercase + string.digits + ".,; ")
ICD_3_CHAR = re.compile(r"^[A-Z]{1}[0-9]{2}[+*]{0,1}$")
ICD_4_CHAR = re.compile(r"^[A-Z]{1}[0-9]{2}.[0-9]{1,}[+*]{0,1}$")
ICD_RANGE = re.compile(r"^([A-Z]{1})[0-9]{2}-\1[0-9]{2}$")


def get_valid_icd10_codes() -> set[str]:
    codes = set()
    with open("scraped_codes_2019.tsv", "r") as f:
        for line in f:
            code = line.strip().split("\t")[0]
            code = "".join(i for i in code if i in ICD_10_VALID_CHARS)
            # add both complete code and 3 char code
            codes.add(code)
            codes.add(code[:3])
    return codes


ICD10_2019_CODES = get_valid_icd10_codes()


def standardise_icd_string(code: str) -> list[str]:
    new = "".join(char for char in code if char in ICD_10_VALID_CHARS)
    cleaned = [i.strip() for i in re.split(";|,| ", new) if i]
    return [i for i in cleaned if i]


def expand_icd_range(start: str, stop: str):
    start, stop = start.upper(), stop.upper()
    idx = string.ascii_uppercase.index(start[0])

    letter = start[0]
    number = int(start[1:])

    matches = set()
    finished = False

    while True:
        for i in range(number, 100):
            code = f"{letter}{str(i).zfill(2)}"
            matches.add(code)
            if code == stop:
                finished = True
                break

        if finished:
            break

        idx += 1
        letter = string.ascii_uppercase[idx]
        number = 0

    return matches


def parse_row(
    row: dict[str, Any],
    ignore_ranges: bool,
) -> Union[tuple[str, tuple[str, ...]], tuple[()]]:
    omim, icd_prev, icd_new = (row["OMIM"], row["ICD10"], row["REVISED mapping code"])

    try:
        omim = int(omim)
    except ValueError:
        logger.debug(f"omim ID {omim!r} could not be convered to integer")
        return ()

    # No mapping available
    if pd.isna(icd_prev) and pd.isna(icd_new):
        logger.debug(f"Could not get an ICD10 mapping for OMIM {omim!r}")
        return ()
    if pd.isna(omim):
        logger.debug("Row did not have OMIM code available")
        return ()

    # Prefer the revised mapping
    if pd.isna(icd_new):
        icd = icd_prev
    else:
        icd = icd_new

    icd_codes = set()

    for code in standardise_icd_string(icd):
        if ICD_3_CHAR.match(code):
            icd_codes.add(code)
        elif ICD_4_CHAR.match(code):
            icd_codes.add(code)
            icd_codes.add(code[:3])
        elif ICD_RANGE.match(code) and not ignore_ranges:
            start_code, stop_code = code.split("-")
            icd_codes |= expand_icd_range(start_code, stop_code)

    # filter out non-icd10 2019 codes
    icd_codes = icd_codes & ICD10_2019_CODES
    return f"omim.{int(omim)}", tuple(icd_codes)


@click.command()
@click.argument("fname", type=click.Path(exists=True))
@click.option("--ignore-ranges", is_flag=True, default=False)
def create_mapping(fname, ignore_ranges):
    df = pd.read_excel(fname, sheet_name=2)
    inputs = (row for _, row in df.iterrows())

    with open("repotrial_mappings.tsv", "w") as f:
        for row in inputs:
            result = parse_row(row, ignore_ranges=ignore_ranges)
            if result and result[1]:
                omim, icd10_codes = result
                f.write(f"{omim}\t{'|'.join(icd10_codes)}" "\n")


if __name__ == "__main__":
    create_mapping()
