from os import getenv


HBASE_HOSTNAME=getenv('HBASE_HOSTNAME')
HBASE_PORT=int(getenv('HBASE_PORT', 9090))


if not HBASE_HOSTNAME or not HBASE_PORT:
    raise Exception('HBASE_HOSTNAME and HBASE_PORT must be configured.')
