"""Typed finite numeric contracts shared by audit and report consumers."""
import math
from numbers import Real

import numpy as np


def valid_number(value, *, integer=False, minimum=0):
    return (isinstance(value, Real) and not isinstance(value, (bool, np.bool_))
            and math.isfinite(value) and (minimum is None or value >= minimum)
            and (not integer or value % 1 == 0))
