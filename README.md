# Himalaya Models

Models for Himalaya.

### Configurations
There are two environment variables to be set:

* `HBASE_HOSTNAME`: HBase Thrift server hostname.  
* `HBASE_PORT`: HBase Thrift server port. Default to 9090.  
* `ES_URL`: Full Elasticsearch URL.

### How to use it?
First, install it as a [Git Submodule](https://chrisjean.com/git-submodules-adding-using-removing-and-updating/).

```
$ cd service/
$ git submodule add ssh://git@stash.dxm.local:7999/kyc/himalaya-models.git himalaya-models
```

Third, run it :)

```python
from himalaya_models import Persona, Message


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

The **Persona** and **Message** models are available. Here are the fields supported:

### Message

```
persona_sender
persona_pubkey
persona_nickname
type
hash
signature
timestamp
dossier_hash
body_hash
acl
objects
message
created_at
```

### Persona
```
address
pubkey
nickname
created_at
```
