import datetime
import happybase
import logging
import logging.config

from thriftpy.transport import TTransportException

from himalaya_models.config import HBASE_HOSTNAME, HBASE_PORT, LOGGING


logger = logging.getLogger(__name__)
logging.config.dictConfig(LOGGING)


class HBaseBase:
    """
    Main Class for dealing with HBase data.
    """

    @classmethod
    def connect_hbase(self):
        # TODO: Instead of calling connect_hbase each time we need
        # abstract it to a `connections` variable, as Elasticsearch-DSL does
        try:
            conn = happybase.Connection(HBASE_HOSTNAME, HBASE_PORT, autoconnect=True).table(self.hbase_table)
            logger.debug('Connected to HBase')
            return conn
        except TTransportException as exc:
            logger.error(f'Could not connect to HBase: {exc}')
            raise

    @classmethod
    def get_from_hbase(cls, row_key):
        """
        Gets a row from HBase using a row_key.

        Returns a class object.
        """

        conn = cls.connect_hbase()
        row = conn.row(cls.to_bytes(row_key))

        if row:
            built_class = cls.dict_to_class(cls, row)
            logger.info(f'{built_class} returned from HBase')
            return built_class

    def save_to_hbase(self):
        """
        Saves an entry on HBase.

        Returns a class object.
        """

        try:
            conn = self.connect_hbase()
            self.validate_fields()
            save_payload = self.construct_save_payload()
            row_key_value = getattr(self, self.hbase_row_key)
            conn.put(self.to_bytes(row_key_value), save_payload)
            logger.info(f'{self} saved on HBase')
            return self
        except Exception as exc:
            logger.error(f'Could not save {self} on HBase: {exc}')
            raise

    def delete_from_hbase(self):
        """
        Delete a row from HBase.
        """

        try:
            conn = self.connect_hbase()
            row_key_value = getattr(self, self.hbase_row_key)
            conn.delete(self.to_bytes(row_key_value))
            logger.info(f'{self} deleted from HBase')
            return
        except Exception as exc:
            logger.error(f'Could not delete {self} from HBase: {exc}')
            raise

    def validate_fields(self):
        """
        Verify if the Class has all attributes necessary for saving into HBase.
        """

        for field in self.hbase_fields:
            if not hasattr(self, field):
                message = f'field={field} is required when saving to HBase'
                logger.error(message)
                raise Exception(message)

    def construct_save_payload(self):
        """
        Construct the payload needed by happybase to save the data to HBase.
        """

        save_payload = {}

        for field in self.hbase_fields:
            full_field_name = f'{self.hbase_family}:{field}'.encode()
            value = getattr(self, field)
            if not isinstance(value, bytes):
                value = self.to_bytes(value)
            save_payload[full_field_name] = value

        return save_payload

    @classmethod
    def to_bytes(cls, field):
        """
        Converts a string to bytes.
        """

        if isinstance(field, str):
            return field.encode()

    def dict_to_class(cls, _dict):        
        """
        Converts a dictonary from HBase to a class object.

        Returns a class object.
        """

        clean_object = {}
        
        for key, item in _dict.items():
            key = key.decode().split(':')[-1]

            # we may receive some msgpacked objects from HBase
            # they should remain the same
            try:
                item = item.decode()
            except UnicodeDecodeError:
                pass

            clean_object[key] = item

        return cls(**clean_object)
