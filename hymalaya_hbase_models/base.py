import happybase
import datetime

from .config import HBASE_HOSTNAME, HBASE_PORT


class HBaseBase:
    """
    Main Class for dealing with HBase data.
    """

    hbase_conn = happybase.Connection(HBASE_HOSTNAME, HBASE_PORT, autoconnect=True)

    @property
    def created_at(self):
        """
        Returns a ISO formated date as a string.
        """

        return self.to_bytes(datetime.datetime.now().isoformat())

    @classmethod
    def get_by_row_key(self, row_key):
        """
        Gets a row from HBase using a row_key.

        Returns a class object.
        """

        return self.dict_to_class(self, self.hbase.row(self.to_bytes(row_key)))

    @classmethod
    def get_by_field(self, field, value):
        """
        Gets a row from HBase from a specific column qualifier and value.

        Returns a list of class objects.
        """

        full_qualifier = f'{self.column_family}:{field}'.encode()

        # For created_at ISO dates, we remove the $ in the regex,
        # so we can search for dates like '2018-02-20' 
        value = f'^{value}' if field == 'created_at' else f'^{value}$'    

        scan = self.hbase.scan(
            columns=[full_qualifier], 
            filter=f"ValueFilter(=, 'regexstring:{value}')"
        )
        
        # Get a row for each entry on the scan generator
        rows = [self.get_by_row_key(row[0].decode()) for row in scan if row]

        return rows

    @classmethod
    def to_bytes(self, field):
        """
        Converts a string to bytes.
        """

        if isinstance(field, str):
            return field.encode()

    @classmethod
    def dict_to_class(cls, _cls, _dict):        
        """
        Converts a dictonary from HBase to a class object.
        """

        clean_object = {}
        
        for key, item in _dict.items():
            key = key.decode().split(':')[-1]
            if key == 'created_at':
                continue
            item = item.decode()
            clean_object[key] = item
            
        return _cls(**clean_object)
