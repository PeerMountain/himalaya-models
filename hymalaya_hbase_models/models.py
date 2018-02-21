import datetime

from .base import HBaseBase
from .config import HBASE_HOSTNAME, HBASE_PORT


class Persona(HBaseBase):
    table = 'persona'
    column_family = 'persona'
    hbase = HBaseBase.hbase_conn.table(table)

    def __init__(self, address=None, pubkey=None, nickname=None):
        self.address = address
        self.pubkey = pubkey
        self.nickname = nickname

    def __repr__(self):
        return f"<Persona: '{self.address}'>"

    def save(self):
        self.hbase.put(
            self.address,
            {
                b'persona:address': self.to_bytes(self.address),
                b'persona:pubkey': self.to_bytes(self.pubkey),
                b'persona:nickname': self.to_bytes(self.nickname),
                b'persona:created_at': self.to_bytes(self.created_at),
            }
        )

    @classmethod
    def get(cls, address=None):
        if address:
            return cls.get_by_row_key(address)

    @classmethod
    def filter(cls, nickname=None, pubkey=None):
        if nickname:
            return cls.get_by_field('nickname', nickname)

        if pubkey:
            return cls.get_by_field('pubkey', pubkey)


class Message(HBaseBase):
    table = 'message'
    column_family = 'message'
    hbase = HBaseBase.hbase_conn.table(table)

    def __init__(
        self,
        address=None,
        pubkey=None,
        nickname=None,
        object_hash=None,
        object_meta_hashes=None,
        message_hash=None,
        message_type=None,
        message_sign=None,
        message_body=None,
        message_sender=None,
        message_dossier_hash=None,
        message_body_hash=None,
        container_hash=None,
        container_sign=None,
        container_body=None,
    ):
        
        self.address = address
        self.pubkey = pubkey
        self.nickname = nickname
        self.object_hash = object_hash
        self.object_meta_hashes = object_meta_hashes
        self.message_hash = message_hash
        self.message_type = message_type
        self.message_sign = message_sign
        self.message_body = message_body
        self.message_sender = message_sender
        self.message_dossier_hash = message_dossier_hash
        self.message_body_hash = message_body_hash
        self.container_hash = container_hash
        self.container_sign = container_sign
        self.container_body = container_body

    def __repr__(self):
        return f"<Message: '{self.message_hash}'>"
        
    def save(self):
        self.hbase.put(
            self.message_hash,
            {
                b'message:address': self.to_bytes(self.address),
                b'message:pubkey': self.to_bytes(self.pubkey),
                b'message:nickname': self.to_bytes(self.nickname),
                b'message:object_hash': self.to_bytes(self.object_hash),
                b'message:object_meta_hashes': self.to_bytes(self.object_meta_hashes),
                b'message:message_hash': self.to_bytes(self.message_hash),
                b'message:message_type': self.to_bytes(self.message_type),
                b'message:message_sign': self.to_bytes(self.message_sign),
                b'message:message_body': self.to_bytes(self.message_body),
                b'message:message_sender': self.to_bytes(self.message_sender),
                b'message:messsage_dossier_hash': self.to_bytes(self.messsage_dossier_hash),
                b'message:message_body_hash': self.to_bytes(self.message_body_hash),
                b'message:container_hash': self.to_bytes(self.container_hash),
                b'message:container_sign': self.to_bytes(self.container_sign),
                b'message:container_body': self.to_bytes(self.container_body),
                b'message:created_at': self.to_bytes(self.created_at),
            }
        )

    @classmethod
    def get(cls, message_hash=None, container_hash=None):
        if message_hash:
            return cls.hbase.row(cls.to_bytes(message_hash))

        # container_hash should be unique, so they are suited for get()
        # but we need to use get_by_field since they are not the row_key
        # TODO: separate better the concerns of get() and filter()
        if container_hash:
            return cls.get_by_field('container_hash', container_hash)[0]

    @classmethod
    def filter(cls, created_at=None, address=None):
        if message_date:
            return cls.get_by_field('created_at', created_at)

        if address:
            return cls.get_by_field('address', address)
