import happybase

from .config import HBASE_HOSTNAME, HBASE_PORT


hbase_conn = happybase.Connection(HBASE_HOSTNAME, HBASE_PORT, autoconnect=True)


def create_persona_table():
    connection.create_table(
        name='persona',
        families={'persona': dict(max_versions=1)}
    )


def create_message_table():
    connection.create_table(
        name='message',
        families={'message': dict(max_versions=1)}
    )
