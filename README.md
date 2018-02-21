# Hymalaya HBase Models

HBase Models for Himalaya.

```python
from hymalaya_hbase_models import Persona, Message


# Writing data
persona = Persona(address='address string', pubkey='the public key', nickname='jojo')
persona.save()

# Use get() to get just one entry, for fields that are unique
persona = Persona.get(address='address string')
print(persona)

# Use filter() to get a list of entries, for fields that aren't unique
messages = Message.filter(created_at='2018-02-20')
print(messages)
```

The **Persona** and **Message** models are available. To check supported fields, take a looks at `models.py`.

### How to use it?
Install it from Stash and import into your Python project :)

### Configurations
There are two environment variables to set:
* `HBASE_HOSTNAME`: HBase Thrift server hostname.
* `HABSE_PORT`: HBase Thrift server port. Default to 9090.
