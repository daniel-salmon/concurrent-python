import pytest

from scripts.merge_sort import merge_sort


@pytest.mark.parametrize(
    "a",
    [
        [4, 2, 5, 0, 3],
        [99, 87, 97, 37, 137, 11, 66, 44, 96, 13],
        [203, 924, 133, 1, 83, 2942, 11, 39, 385],
        [4],
        [],
        [2, 3],
        [3, 2],
        [2, 2],
        [4, 2, 5, 0, 4],
    ],
)
def test_merge_sort(a: list[int]):
    assert merge_sort(a) == sorted(a)


@pytest.mark.parametrize(
    "a",
    [
        [4, 2, 5, 0, 3],
        [99, 87, 97, 37, 137, 11, 66, 44, 96, 13],
        [203, 924, 133, 1, 83, 2942, 11, 39, 385],
        [4],
        [],
        [2, 3],
        [3, 2],
        [2, 2],
        [4, 2, 5, 0, 4],
    ],
)
def test_threaded_merge_sort(a: list[int]):
    assert merge_sort(a) == sorted(a)
