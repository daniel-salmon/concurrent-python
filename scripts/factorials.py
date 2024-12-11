import operator
from concurrent.futures import ThreadPoolExecutor
from functools import reduce


def factorial(n: int) -> int:
    return reduce(operator.mul, range(1, n + 1), 1)


with ThreadPoolExecutor(max_workers=5) as executor:
    for result in executor.map(factorial, range(1, 1000)):
        print(result)
