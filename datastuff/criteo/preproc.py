import csv
import pickle
from glob import glob
from multiprocessing import Pool
from pathlib import Path

# from typing import Dict, List, Optional, Tuple, Set
from typing import Optional
from dataclasses import dataclass

import click
from cprint import cprint


@dataclass
class SparseStats:
    colname: str
    uniq_toks: set[str]


@dataclass
class DenseStats:
    # Name of the dense column
    colname: str

    # Sum of all the values in the column.
    sum: float

    # Number of elements in the column, useful for calculating mean later on.
    count: int

    # \sum_i (x_i - \bar x)^2 for the column, useful for calculating the standard deviation later on.
    sumdiffsq: float


# TODO: Move this to utils
class GlobType(click.ParamType):
    name = "glob"

    def convert(self, value, param, ctx):
        if not list(glob(value)):
            self.fail(False, f"There are no files matching the pattern {value}!")
        return value


# def compute_meta(rank: int, filepath: Path) ->

# def get_uniq_toks(rank: int, filepath: Path) -> Dict[str, set]:
#     cprint(rank, f"Processing {filepath}")
#     uniq_toks: Dict[str, set] = {f"C{i}": set() for i in range(1, 27)}
#     with open(filepath, "rt") as f:
#         tsv = csv.reader(f, delimiter="\t")
#         for row in tsv:
#             for i in range(1, 27):
#                 uniq_toks[f"C{i}"].add(row[i + 13])
#     return uniq_toks


# def dense_stats(rank: int, filepath: Path) -> Dict[str, ]


def validate_user_input(inglob: str, outfile: str) -> Tuple[bool, Optional[str]]:
    if not list(glob(inglob)):
        return (False, f"There are no files matching the pattern {inglob}!")

    outpath = Path(outfile)
    outdir = outpath.parents[0]
    if not outdir.exists():
        return (False, f"There is no such directory as {outdir}!")
    if outpath.exists():
        return (False, f"{outfile} already exists!")
    return (True, None)


@click.command()
@click.option(
    "--inglob",
    type=GlobType,
    default="./*.tsv",
    help="The file pattern that all input files have.",
)
@click.option(
    "--nprocs", type=int, default=None, help="Number of processes to spin off."
)
@click.argument(
    "outdir",
    type=click.Path(
        exists=True, file_okay=False, dir_okay=True, writable=True, path_type=Path
    ),
)
def main(inglob: str, nprocs: Optional[int], outdir: str) -> int:
    """TODO"""

    nprocs = int(nprocs) if nprocs is not None else None
    infiles: List[str] = list(glob(inglob))
    with Pool(processes=nprocs) as pool:
        uniq_toks_from_files: List[Dict[str, set]] = pool.starmap(
            get_uniq_toks, zip(range(len(infiles)), infiles)
        )

    print("Gathered all tokens from indivdiual files. Writing them out.")
    all_uniq_toks: Dict[str, set] = {f"C{i}": set() for i in range(1, 27)}
    for uniq_toks_from_file in uniq_toks_from_files:
        for colname, uniq_toks_for_col in uniq_toks_from_file.items():
            all_uniq_toks[colname].union(uniq_toks_for_col)

    outfile = Path(outdir) / Path("uniq_toks.pkl")
    with open(outfile, "wb") as f:
        pickle.dump(all_uniq_toks, f)

    return 0


if __name__ == "__main__":
    main()
