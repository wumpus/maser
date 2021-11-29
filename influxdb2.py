import time

from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import ASYNCHRONOUS, SYNCHRONOUS
from influxdb_client.client.exceptions import InfluxDBError
from influxdb_client.domain.write_precision import WritePrecision

client = InfluxDBClient.from_config_file('influxdb2_config.ini')
# does file exist
# does it have url, org, token
# any '#' or whitespace
# might need to return the org ?

bucket = 'vlbimon'

write_api = client.write_api(write_options=SYNCHRONOUS)  # SYNCHRONOUS or ASYNCHRONOUS

t = int(time.time())

p = Point('my_measurement').tag('location', 'Prague').field('temperature', 25.3).time(t, write_precision=WritePrecision.S)

try:
    async_result = write_api.write(bucket=bucket, record=p)
except InfluxDBError:
    # I have never seen this happen XXX try stopping the influx instance mid-write
    pass

if async_result is not None:
    print('type', type(async_result))  # type <class 'multiprocessing.pool.ApplyResult'>
    print(async_result.get())  # None

client.close()  # required (for async) or hang on exit
