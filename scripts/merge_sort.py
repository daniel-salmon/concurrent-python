from concurrent.futures import ThreadPoolExecutor
from itertools import batched

MAX_WORKERS = 6


def merge_sort(a: list[int]) -> list[int]:
    if len(a) <= 1:
        return a
    left = merge_sort(a[: len(a) // 2])
    right = merge_sort(a[len(a) // 2 :])
    return merge(left, right)


def merge(left: list[int], right: list[int]) -> list[int]:
    lidx, ridx, out = 0, 0, []
    while lidx < len(left) and ridx < len(right):
        if left[lidx] <= right[ridx]:
            out.append(left[lidx])
            lidx += 1
        else:
            out.append(right[ridx])
            ridx += 1
    out.extend(left[lidx:])
    out.extend(right[ridx:])
    return out


def threaded_merge_sort(a: list[int], max_workers: int = MAX_WORKERS) -> list[int]:
    if len(a) <= 1:
        return a

    todo = [[x] for x in a]
    if len(todo) % 2 == 1:
        new_last = sorted([todo[-2][0], todo[-1][0]])
        todo.pop()
        todo[-1] = new_last

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        while len(todo) > 1:
            futures = []
            for pair in batched(todo, n=2):
                futures.append(executor.submit(merge, pair[0], pair[1]))

            todo = []
            for future in futures:
                todo.append(future.result())

    return todo[0]


if __name__ == "__main__":
    x = [5, 4, 3, 2, 1, 6, 7, 8]
    print(f"Attempting to sort {x}")
    print(f"Sorted: {threaded_merge_sort(x)}")
    x = [5, 4, 3, 2, 1, 6, 7, 8, 9]
    print(f"Attempting to sort {x}")
    print(f"Sorted: {threaded_merge_sort(x)}")
