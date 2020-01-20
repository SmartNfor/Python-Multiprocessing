# Parallelizing with Pool.apply_async()
#Problem Statement: Count how many numbers exist between a given range in each row
import numpy as np
import multiprocessing as mp

pool = mp.Pool(mp.cpu_count())


# Prepare data
np.random.RandomState(100)
arr = np.random.randint(0, 10, size=[200000, 5])
data = arr.tolist()
data[:5]
print(data[:5])


results = []

# Step 1: Redefine, to accept `i`, the iteration number
def load(i, row, minimum, maximum):
    """Returns how many numbers lie within `maximum` and `minimum` in a given `row`"""
    count = 0
    for n in row:
        if minimum <= n <= maximum:
            count = count + 1
    return (i, count)


# Step 2: Define callback function to collect the output in `results`
def call_back(result):
    print(result)


# Step 3: Use loop to parallelize
def run ():
    for i, row in enumerate(data):
        pool.apply_async(load, args=(i, row, 4, 8), callback=call_back)

# Step 4: Close Pool and let all the processes complete    
pool.close()
pool.join()  # postpones the execution of next line of code until all processes in the queue are done.

# # Step 5: Sort results [OPTIONAL]
# results.sort(key=lambda x: x[0])
# results_final = [r for i, r in results]
if __name__ == '__main__':  
    run()