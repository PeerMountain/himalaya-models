# Hymalaya HBase Models

HBase Models for Himalaya.

### Configurations
There are two environment variables to be set:

* `HBASE_HOSTNAME`: HBase Thrift server hostname.  
* `HABSE_PORT`: HBase Thrift server port. Default to 9090.  

### How to use it?
First, install it:
```bash
$ pipenv install git+ssh://git@stash.dxm.local:7999/kyc/himalaya-hbase-models.git
```

Second, configure the environment variables.

Third, run it :)
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
