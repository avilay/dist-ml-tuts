import csv
import pickle
from glob import glob
from multiprocessing import Pool
from pathlib import Path
from typing import Optional, Tuple

import click
from cprint import cprint, danger_print


def get_uniq_toks(rank: int, filepath: Path) -> set:
    cprint(rank, f"Processing {filepath}")
    uniq_toks = {f"C{i}": set() for i in range(1, 27)}
    with open(filepath, "rt") as f:
        tsv = csv.reader(f, delimiter="\t")
        for row in tsv:
            for i in range(1, 27):
                uniq_toks[f"C{i}"].add(row[i + 13])
    return uniq_toks


def validate_user_input(inglob: str, outfile: str) -> Tuple[bool, str]:
    if not list(glob(inglob)):
        return (False, f"There are no files matching the pattern {inglob}!")

    outfile = Path(outfile)
    outdir = outfile.parents[0]
    if not outdir.exists():
        return (False, f"There is no such directory as {outdir}!")
    if outfile.exists():
        return (False, f"{outfile} already exists!")
    return (True, None)


@click.command()
@click.option(
    "--inglob", default="./*.tsv", help="The file pattern that all input files have."
)
@click.option("--nprocs", default=None, help="Number of processes to spin off.")
@click.argument("outfile")
def main(inglob: str, nprocs: Optional[int], outfile: str):
    """Creates a token to index mapping and pickles this file in the OUTFILE location."""
    is_valid, err_msg = validate_user_input(inglob, outfile)
    if not is_valid:
        danger_print(err_msg)
        return -1

    nprocs = int(nprocs) if nprocs is not None else None
    infiles = list(glob(inglob))
    with Pool(processes=nprocs) as pool:
        uniq_toks_from_files = pool.starmap(
            get_uniq_toks, zip(range(len(infiles)), infiles)
        )

    print("Gathered all tokens from indivdiual files. Writing them out.")
    all_uniq_toks = {f"C{i}": set() for i in range(1, 27)}
    for uniq_toks_from_file in uniq_toks_from_files:
        for colname, uniq_toks_for_col in uniq_toks_from_file.items():
            all_uniq_toks[colname].union(uniq_toks_for_col)

    with open(outfile, "wb") as f:
        pickle.dump(all_uniq_toks, f)


if __name__ == "__main__":
    main()
