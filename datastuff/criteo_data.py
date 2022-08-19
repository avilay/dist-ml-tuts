from multiprocessing.sharedctypes import Value
import torch as t
from pathlib import Path
from dataclasses import dataclass


@dataclass
class CriteoRow:
    label: int
    int1: int
    int2: int
    int3: int
    int4: int
    int5: int
    int6: int
    int7: int
    int8: int
    int9: int
    int10: int
    int11: int
    cat1: str
    cat2: str
    cat3: str
    cat4: str
    cat5: str
    cat6: str
    cat7: str
    cat8: str
    cat9: str
    cat10: str
    cat11: str
    cat12: str
    cat13: str
    cat14: str
    cat15: str
    cat16: str
    cat17: str
    cat18: str
    cat19: str
    cat20: str
    cat21: str
    cat22: str
    cat23: str
    cat24: str


class Criteo(t.utils.data.IterableDataset):
    def __init__(self, datapath):
        if not Path.exists(datapath):
            raise ValueError(f"{datapath} does not exist!")
        self._datapath = datapath

    def __iter__(self):
        with open(self._datapath, "rt") as f:
            for line in f:
                flds = line.split("\t")
                label = int(flds[0])
                int_features = [flds[1:]]


def main():
    pass


if __name__ == "__main__":
    main()
