import os
from datetime import datetime
import time

import numpy as np
from cprint import cprint
from torch.utils.data import DataLoader, Dataset, get_worker_info

rng = np.random.default_rng()


def log_worker(msg):
    wi = get_worker_info()
    pid = os.getpid()
    if wi is not None:
        wid = wi.id
        n_workers = wi.num_workers
    else:
        wid, n_workers = -1, -1
    now = datetime.now().strftime("%H:%M:%S")
    color = wid + 1
    cprint(color, f"{now} - ({pid}){wid}/{n_workers}: {msg}")


class MyMappedDataset(Dataset):
    def __init__(self, n=5, m=10):
        log_worker("Instantiating dataset")
        self._m = m
        self._n = n

    def __getitem__(self, idx):
        # log_worker(f"Fetching data[{idx}]")
        time.sleep(rng.integers(2, 5))
        return np.full((self._n,), fill_value=idx), rng.choice([0.0, 1.0])

    def __len__(self):
        return self._m


def main():
    ds = MyMappedDataset(m=30)

    # 9.017 seconds
    # dl = DataLoader(ds, shuffle=False, batch_size=3)

    # 5.791 seconds
    # dl = DataLoader(ds, shuffle=False, batch_size=3, num_workers=1)

    # 6.126 seconds
    # dl = DataLoader(ds, shuffle=False, batch_size=3, num_workers=1, prefetch_factor=3)

    # 1.451 seconds
    # dl = DataLoader(ds, shuffle=False, batch_size=3, num_workers=2, prefetch_factor=3)

    # 0.115 seconds
    # dl = DataLoader(ds, shuffle=False, batch_size=3, num_workers=3, prefetch_factor=2)

    # 1.229 seconds
    dl = DataLoader(ds, shuffle=False, batch_size=3, num_workers=2, prefetch_factor=2)

    times = []
    for i, (X, y) in enumerate(dl):
        times.append(time.time())
        now = datetime.now().strftime("%H:%M:%S")
        print(f"{now} - Got batch")
        # input("Press [ENTER] to accept batch:\n")
        print(X, y)
        time.sleep(3)  # pretend to work
    times = np.array(times)
    avg_fetch_time = np.mean(times[1:] - times[:-1]) - 3
    print(f"Average batch fetch time was {avg_fetch_time:.3f} seconds.")


if __name__ == "__main__":
    main()
