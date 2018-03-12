import datetime
import logging
import logging.config
from msgpack import unpackb

from himalaya_models.config import ES_URL, LOGGING
from elasticsearch_dsl import connections, DocType, Text, Keyword, InnerDoc, Nested, Q
from elasticsearch.exceptions import ConnectionError


logger = logging.getLogger(__name__)
logging.config.dictConfig(LOGGING)

connections.create_connection(hosts=[ES_URL])
logger.debug('Connected to Elasticsearch')


class ESMessageContainer(InnerDoc):
    hash = Keyword()


class ESMessage(DocType):
    persona_sender = Keyword()
    persona_nickname = Keyword()
    hash = Keyword()
    dossier_hash = Keyword()
    containers = Nested(ESMessageContainer)
    created_at = Text()

    class Meta:
        index = 'message'

    def save(self, **kwargs):
        self.meta.id = self.hash
        return super().save(**kwargs)

    def add_container(self, hash):
        self.containers.append(ESMessageContainer(hash=hash))

    @classmethod
    def get_hash_from_es(cls, container_hash=None):
        """
        Given a filter, returns a Message hash from Elasticasech.
        """

        if container_hash:
            result = cls.search().query(
                'nested',
                path='containers',
                query=Q('match', containers__hash=container_hash)
            ).execute()
            logger.info(f'{result} returned from Elasticsearch')

        if result and len(result) > 1:
            message = f'get_hash_from_es should return just one result'
            logger.error(message)
            raise Exception(message)

        return result[0].hash
    
    @classmethod
    def filter_from_es(cls, created_at=None, persona_sender=None):
        """
        Given a filter, return a list of hashes from Elasticsearch.
        """

        if created_at:
            results = cls.search().query(
                'match_phrase_prefix',
                created_at=created_at
            ).execute()

        if persona_sender:
            results = cls.search().query(
                'match',
                persona_sender=persona_sender
            ).execute()

        if results:
            logger.info(f'{results} returned from Elasticsearch')

        return [message.hash for message in results if results]

    def save_to_es(self):
        """
        Saves an entry to Elastichsearch.
        """

        message = ESMessage()
        self.validate_fields()

        # Construct message
        for field in self.es_fields:
            setattr(message, field, getattr(self, field))

        # Adds container
        for object in unpackb(self.objects):
            container_hash = object.get(b'container').get(b'containerHash').decode()
            if container_hash:
                message.add_container(container_hash)

        try:
            saved = message.save()
            logger.info(f'{self} saved on Elasticsearch')
            return saved
        except Exception as exc:
            logger.error(f'Could not save to Elasticsearch: {exc}')
            raise

    def delete_from_es(self):
        """
        Deletes an entry from Elasticsearch.
        """

        try:
            message = ESMessage.get(id=getattr(self, self.es_id))
            message.delete()
            logger.info(f'{self} deleted from Elasticsearch')
            return
        except Exception as exc:
            logger.error(f'Could not delete from Elasticsearch: {exc}')
            raise

    def validate_fields(self):
        """
        Verify if the Class has all attributes necessary for saving into Elasticsearch.
        """

        for field in self.es_fields:
            if not hasattr(self, field):
                message = f'field={field} is required when saving to Elasticsearch'
                logger.error(message)
                raise Exception(message)


class ESPersona(DocType):
    address = Keyword()
    pubkey = Keyword()
    nickname = Keyword()
    created_at = Text()

    class Meta:
        index = 'persona'

    def save(self, **kwargs):
        self.meta.id = self.address
        return super().save(**kwargs)

    @classmethod
    def get_address_from_es(cls, address=None, nickname=None, pubkey=None):
        """
        Given a filter, returns a Persona address from Elasticasech.
        """

        if address:
            result = cls.search().query(
                'match',
                address=address
            ).execute()
            logger.info(f'{result} returned from Elasticsearch')

        if nickname:
            result = cls.search().query(
                'match',
                nickname=nickname
            ).execute()
            logger.info(f'{result} returned from Elasticsearch')

        if pubkey:
            result = cls.search().query(
                'match',
                pubkey=pubkey
            ).execute()
            logger.info(f'{result} returned from Elasticsearch')

        if result and len(result) > 1:
            message = f'get_address_from_es should return just one result'
            logger.error(message)
            raise Exeception(message)

        return result[0].address

    def save_to_es(self):
        persona = ESPersona()
        self.validate_fields()

        # Construct message
        for field in self.es_fields:
            setattr(persona, field, getattr(self, field))

        saved = persona.save()
        logger.info(f'{self} saved on Elasticsearch')

        return saved

    def validate_fields(self):
        """
        Verify if the Class has all attributes necessary for saving into Elasticsearch.
        """

        for field in self.es_fields:
            if not hasattr(self, field):
                message = f'field={field} is required when saving to Elasticsearch'
                logger.error(message)
                raise Exception(message)
