from argparse import ArgumentParser
from pathlib import Path
from typing import Optional, Tuple

from modules.logger import get_logger

def parser() -> ArgumentParser:
    parser = ArgumentParser(description='CYP2D6 copy number extractor')
    parser.add_argument('-i', '--table_input', type=str, help='spreadsheet with init sample data')
    parser.add_argument('-s', '--sample_data', type=str, default='', help='sample data from previous run')
    return parser

def get_args() -> Tuple[Path, Optional[Path]]:
    args = parser().parse_args()
    input_xlsx = Path(args.table_input).resolve()
    sample_data = Path(args.sample_data).resolve() if args.sample_data else None
    logger = get_logger(__name__)
    logger.info(f"Args: table_input='{input_xlsx.as_posix()}'")
    return (input_xlsx, sample_data)