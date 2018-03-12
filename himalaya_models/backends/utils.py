import happybase

from ..config import HBASE_HOSTNAME, HBASE_PORT


def create_hbase_message_table():
    conn = happybase.Connection(HBASE_HOSTNAME, HBASE_PORT, autoconnect=True)
    conn.create_table(
        name='persona',
        families={'persona': dict(max_versions=1)}
    )


def create_hbase_persona_table():
    conn = happybase.Connection(HBASE_HOSTNAME, HBASE_PORT, autoconnect=True)
    conn.create_table(
        name='message',
        families={'message': dict(max_versions=1)}
    )
