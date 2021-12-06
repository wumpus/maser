import os
import os.path
import configparser
import sys
from datetime import datetime
from collections import defaultdict

import pandas as pd

from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import ASYNCHRONOUS, SYNCHRONOUS, WriteOptions
from influxdb_client.client.exceptions import InfluxDBError
from influxdb_client.domain.write_precision import WritePrecision


class WrapInfluxDBClient(InfluxDBClient):
    def __init__(self, *args, **kwargs):
        if 'bucket' in kwargs:
            self._bucket = kwargs['bucket']
            del kwargs['bucket']
        return super().__init__(*args, **kwargs)

    @property
    def bucket(self):
        return self._bucket

    @classmethod
    def from_config_file(cls, fname, *args, **kwargs):
        '''Test for some common mistakes that cause inscrutable errors:
        file not found => KeyError
        inline comments in the ini file are not stripped, and if in the URL,
        can easily result in a weird error like:
        "FluxCsvParserException: Unable to parse CSV response. FluxTable definition was not found."
        If the bucket name is in the config file
        '''
        fname = os.path.expandvars(os.path.expanduser(fname))
        if not os.path.isfile(fname):
            raise ValueError('ini file does not exist: '+str(fname))

        config = configparser.ConfigParser()
        config.read(fname)

        if 'influx2' not in config:
            raise ValueError('config file lacks an [influx2] section')

        ci = config['influx2']
        for key, value in ci.items():
            value = value.replace('"', '')
            if ' ' in value or '#' in value:
                raise ValueError('from_config_file: saw a space or # in {}... hint: inline comments are not valid in config files'.format(key))
            if key in kwargs:
                print('from_config_file: letting keyword override config file for key', key)
            else:
                kwargs[key] = value

        if 'bucket' in kwargs:
            bucket = kwargs['bucket']
            del kwargs['bucket']
        else:
            bucket = None

        instance = cls(*args, **kwargs)

        if bucket is not None:
            instance._bucket = bucket

        return instance


def write_points(write_api, bucket, measurement, points, t, station):
    objs = []
    for p in points:
        objs.append(Point(measurement).tag('station', station).time(t, write_precision=WritePrecision.S))

    try:
        async_result = write_api.write(bucket=bucket, record=p)
    except InfluxDBError:
        # I have never seen this happen XXX try stopping the influx instance mid-write
        raise

    if async_result is not None:
        print('type', type(async_result))  # type <class 'multiprocessing.pool.ApplyResult'>
        print(async_result.get())  # None


ref_epoch_cache = {}
vdif_per_sec = defaultdict(lambda: 125000, {'SMA': 0, 'ALMA': 0})  # XXX fixme ALMA
# XXX make sure that we examine SMA packets to see if they're pre- or post-aphids.


def row_to_timestamp(row, station):
    return vdif_time_to_timestamp(row['ref_epoch'],
                                  row['sec_since_epoch'],
                                  row['data_frame'],
                                  station)


def vdif_time_to_timestamp(ref_epoch, sec_since_epoch, data_frame, station):
    '''Convert the time information in a vdif frame to a unixtime'''
    '''Due to the cache, this function rarely has to use an expensive datetime() object.'''

    if ref_epoch in ref_epoch_cache:
        epoch_ts = ref_epoch_cache[ref_epoch]
    else:
        year = ref_epoch // 2 + 2000
        month = (ref_epoch % 2)*6 + 1  # Jan or Jul
        epoch_ts = datetime(year, month, 1, 0, 0, 0).timestamp()
        ref_epoch_cache[ref_epoch] = epoch_ts

    frame_offset = 1.0 * data_frame / vdif_per_sec[station]

    timestamp = epoch_ts + sec_since_epoch + frame_offset
    return timestamp


def read_old_pps_csv(station, fname, for_csv=True):
    df = pd.read_csv(fname)
    for column in ('ref_epoch', 'sec_since_epoch', 'data_frame'):
        if column not in df:
            raise ValueError('input file lacks column '+column)

    df['timestamp'] = df.apply(lambda row: int(row_to_timestamp(row, station) * 1000000000), axis=1)

    if not for_csv:
        # the write api expects the df index to be this timestamp
        df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ns')
        df.set_index('timestamp', inplace=True)
    else:
        # for the write api, this is specified as a kwarg
        df['measurement'] = 'pps'
    df['station'] = station
        
    return df


def write_influx_csv(df, fname_out):
    # when I want it, how do I specify a write precision of 1s? not really relevant for pps though
    # influx write -precision s
    with open('out.csv', 'w') as fd:
        fd.write('#datatype measurement,tag,long,dateTime:number\n')
    columns = ('measurement', 'station', 'pps_offset', 'timestamp')
    df.to_csv('out.csv', columns=columns, header=True, index=False, mode='a')
    print('cli hint: influx write dryrun -b vibimon -f out.csv')


def main_csv(station, fname):
    df = read_old_pps_csv(station, fname)
    write_influx_csv(df, 'out.csv')


def main_insert_influx(station, fname):
    df = read_old_pps_csv(station, fname, for_csv=False)
    print('before: columns', list(df))  # ['ref_epoch', 'sec_since_epoch', 'data_frame', 'pps_offset', 'station', 'measurement']
    for c in ('ref_epoch', 'sec_since_epoch', 'data_frame', 'measurement'):
        df.drop(labels=c, axis=1, inplace=True, errors='ignore')
    print('after: columns', list(df))  # ['pps_offset', 'station']

    client = WrapInfluxDBClient.from_config_file('influxdb2_config.ini')
    bucket = client.bucket

    batching_wo = WriteOptions(batch_size=500, flush_interval=10_000, jitter_interval=2_000, retry_interval=5_000)
    # intervals are in milliseconds

    write_api = client.write_api(write_options=batching_wo)
    # write_options=SYNCHRONOUS)  # SYNCHRONOUS or ASYNCHRONOUS

    write_api.write(bucket=bucket,
                    record=df,
                    data_frame_measurement_name='pps',  # this is the name on the influx side
                    data_frame_tag_columns=['station'],  # these are df column names
                   )  # write_precision=...


    write_api.close()  # required for batching
    client.close()  # required for async, else hang on exit


if __name__ == '__main__':
    main_csv(sys.argv[1], sys.argv[2])
