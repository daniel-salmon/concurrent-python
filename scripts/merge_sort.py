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
