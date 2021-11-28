import time

import influxdb_client
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import ASYNCHRONOUS
from influxdb_client.client.exceptions import InfluxDBError
from influxdb_client.domain.write_precision import WritePrecision

client = InfluxDBClient.from_config_file('influxdb2_config.ini')
bucket = 'vlbimon'

write_api = client.write_api(write_options=ASYNCHRONOUS)

t = time.time()

p = Point('my_measurement').tag('location', 'Prague').field('temperature', 25.3).time(int(t), write_precision=WritePrecision.S)

async_result = write_api.write(bucket=bucket, record=p)

print('type', type(async_result))  # type <class 'multiprocessing.pool.ApplyResult'>

print(async_result.get())  # None

client.close()  # required or hang on exit
