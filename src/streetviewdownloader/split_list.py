#!/usr/bin/env python3

"""Split a list into equal parts."""


import math
from typing import Generator


def split_list(lst: list, num_parts: int) -> Generator[list, None, list]:
    """
    Split a list into multiple lists of roughly equal length.

    Arguments
    ---------
    lst : list
        The list to split
    num_parts : int
        how many parts to split the list into

    Returns
    -------
    Generator[list, None, list] :
        yields parts of the original list as lists
    """
    length = len(lst)

    if not length:
        return []

    bin_size = math.ceil(length / float(num_parts))
    for i in range(0, length, bin_size):
        yield lst[i : (i + bin_size)]
