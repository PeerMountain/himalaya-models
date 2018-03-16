from os import getenv


HBASE_HOSTNAME = getenv('HBASE_HOSTNAME')
HBASE_PORT = int(getenv('HBASE_PORT', 9090))

if not HBASE_HOSTNAME or not HBASE_PORT:
    raise Exception('HBASE_HOSTNAME and HBASE_PORT must be configured.')

ES_URL = getenv('ES_URL')
if not ES_URL:
    raise Exception('ES_URL must be configured.')

LOG_LEVEL = getenv('LOG_LEVEL', 'INFO')
LOGGING = {
    'version': 1,
    'disable_existing_loggers': False,
    'handlers': {
        'console': {
            'class': 'logging.StreamHandler',
            'formatter': 'default',
        },
    },
    'formatters': {
        'default': {
            'format': '%(asctime)s [%(name)s] [%(levelname)s] %(message)s',
            'datefmt': '%Y-%m-%d %H:%M:%S:',
        }
    },
    'loggers': {
        'himalaya_models': {
            'handlers': ['console'],
            'level': LOG_LEVEL,
        },
    },
}
