import pytest
from datetime import datetime

import influx_utils


def test_vdif_time_to_timestamp():
    epoch_ts = datetime(2000, 1, 1, 0, 0, 0).timestamp()
    assert epoch_ts == 946684800.0

    assert influx_utils.vdif_time_to_timestamp(0, 0, 0, 'KP') == epoch_ts
    assert influx_utils.vdif_time_to_timestamp(0, 1, 0, 'KP') == epoch_ts + 1
    assert influx_utils.vdif_time_to_timestamp(0, 1, 0, 'XYZ') == epoch_ts + 1, 'unknown stations have a default'
    assert influx_utils.vdif_time_to_timestamp(0, 0, 1, 'KP') == epoch_ts + 1.0 / 125000
    assert influx_utils.vdif_time_to_timestamp(0, 0, 101, 'KP') == epoch_ts + 101.0 / 125000

    even_ts = datetime(2001, 1, 1, 0, 0, 0).timestamp()
    seconds_per_leap_year = even_ts - epoch_ts
    another_even_ts = datetime(2002, 1, 1, 0, 0, 0).timestamp()
    seconds_per_year = another_even_ts - epoch_ts - seconds_per_leap_year
    assert influx_utils.vdif_time_to_timestamp(2, 1, 0, 'KP') == epoch_ts + seconds_per_leap_year + 1
    assert influx_utils.vdif_time_to_timestamp(4, 1, 0, 'KP') == epoch_ts + seconds_per_leap_year + seconds_per_year + 1

    odd_ts = datetime(2000, 7, 1, 0, 0, 0).timestamp()
    seconds_per_leap_first_half = odd_ts - epoch_ts
    another_odd_ts = datetime(2002, 7, 1, 0, 0, 0).timestamp()
    seconds_per_first_half = another_odd_ts - epoch_ts - seconds_per_leap_year - seconds_per_year
    print('leap half', seconds_per_leap_first_half)
    print('normal half', seconds_per_first_half)
    assert influx_utils.vdif_time_to_timestamp(1, 2, 0, 'KP') == epoch_ts + seconds_per_leap_first_half + 2
    assert influx_utils.vdif_time_to_timestamp(3, 2, 0, 'KP') == epoch_ts + seconds_per_leap_year + seconds_per_first_half + 2

    # there was a leap second at the end of 2005
    ts_2005 = datetime(2005, 1, 1, 0, 0, 0).timestamp()
    ts_2006 = datetime(2006, 1, 1, 0, 0, 0).timestamp()
    with pytest.raises(AssertionError):
        # sadly, python's datetime does not support leap seconds
        assert ts_2005 + seconds_per_year + 1 == ts_2006, 'make sure python sees the leap second'
