# Parallelizing with Pool.apply()

import numpy as np
import multiprocessing as mp


def load(row, minimum, maximum):
    """Returns how many numbers lie within `maximum` and `minimum` in a given `row`"""
    count = 0
    for n in row:
        if minimum <= n <= maximum:
            count = count + 1
    return count

def run (pool,arr):
    results = [pool.apply(load, args=(row, 4, 8)) for row in arr]
    return results

if __name__ == '__main__':
    # Prepare data
    np.random.RandomState(100)
    arr = np.random.randint(0, 10, size=[200000, 5])
    data = arr.tolist()
    sample = data[:5]
    print(data[:5])

    pool = mp.Pool(mp.cpu_count())
    print(run(pool,sample))
    pool.close()
    pool.join()
 