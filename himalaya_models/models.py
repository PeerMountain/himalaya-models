import logging
import datetime

from umsgpack import unpackb

from .backends.hbase import HBaseBase
from .backends.elasticsearch import ESMessage, ESPersona
from .config import HBASE_HOSTNAME, HBASE_PORT, LOGGING


logger = logging.getLogger(__name__)
logging.config.dictConfig(LOGGING)


class ModelBase:
    """
    Define some common methods to be used in all Models.
    """

    def save(self):
        try:
            # Raise exception if trying to double save a message
            self.save_to_hbase()
            self.save_to_es()
        except:
            self.delete_from_hbase()
            self.delete_from_es()
            logger.error(exc)
        else:
            logger.info(f'{self} sucessfully saved')


class Message(ModelBase, HBaseBase, ESMessage):
    hbase_table = 'message'
    hbase_family = 'message'
    hbase_row_key = 'hash'
    hbase_fields = (
        'persona_sender',
        'persona_pubkey',
        'persona_nickname',
        'type',
        'hash',
        'signature',
        'timestamp',
        'dossier_hash',
        'body_hash',
        'acl',
        'objects',
        'message',
        'created_at',
    )
    es_fields = (
        'persona_sender',
        'persona_nickname',
        'hash',
        'dossier_hash',
        'created_at',
    )
    es_id = 'hash'


    def __repr__(self):
        return f"<Message: '{self.hash}'>"

    def to_dict(self):
        result = super().to_dict()

        acl = result.get('acl')
        if acl:
            result['acl'] = unpackb(acl)

        objects = result.get('objects')
        if objects:
            result['objects'] = unpackb(objects)

        return result

    @classmethod
    def get(cls, hash=None, container_hash=None):
        if hash:
            return cls.get_from_hbase(hash)

        if container_hash:
            hash = cls.get_hash_from_es(container_hash=container_hash)
            return cls.get_from_hbase(hash)

    @classmethod
    def filter(cls, created_at=None, persona_sender=None):
        if created_at:
            hashes = cls.filter_from_es(created_at=created_at)

        if persona_sender:
            hashes = cls.filter_from_es(persona_sender=persona_sender)

        messages = [cls.get(hash=hash) for hash in hashes if hashes]
        return messages

class Persona(ModelBase, HBaseBase, ESPersona):
    hbase_table = 'persona'
    hbase_family = 'persona'
    hbase_row_key = 'address'
    hbase_fields = (
        'address',
        'pubkey',
        'nickname',
        'created_at',
    )
    es_fields = (
        'address',
        'pubkey',
        'nickname',
        'created_at',
    )
    es_id = 'address'

    def __repr__(self):
        return f"<Persona: '{self.address}'>"

    @classmethod
    def get(cls, address=None, nickname=None, pubkey=None):
        if address:
            return cls.get_from_hbase(address)

        if pubkey:
            address = cls.get_address_from_es(pubkey=pubkey)
            return cls.get_from_hbase(address)

        if nickname:
            address = cls.get_address_from_es(nickname=nickname)
            return cls.get_from_hbase(address)
