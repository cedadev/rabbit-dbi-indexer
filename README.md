# Rabbit DBI Elastic Indexer

This provides the code to read from the rabbit queue and process updates
to the directory index.

Watched events:
- DEPOSIT/REMOVE - For 00READMES
- MKDIR
- RMDIR
- SYMLINK

## Configuration

Configuration is handled using a YAML file. The full configuration options 
are described in the [rabbit_indexer repo](https://github.com/cedadev/rabbit-index-ingest/blob/master/README.md#rabbit_event_indexer)

The required sections for the dbi indexer are:
- rabbit_server
- rabbit_indexer
- logging
- moles
- elasticsearch
- directory_index

An example YAML file (secrets noted by ***** ): 

```yaml

---
rabbit_server:
  name: "*****"
  user: "*****"
  password: "*****"
  vhost: "*****"
  source_exchange:
    name: deposit_logs
    type: fanout
  dest_exchange:
    name: fbi_fanout
    type: fanout
  queues:
    - name: elasticsearch_dbi_update_queue
      kwargs:
        auto_delete: false
rabbit_indexer:
  queue_consumer_class: rabbit_dbi_elastic_indexer.queue_consumers.DBIQueueConsumer
logging:
  log_level: info
moles:
  moles_obs_map_url: http://api.catalogue.ceda.ac.uk/api/v2/observations.json/?publicationState__in=citable,published,preview,removed
elasticsearch:
  es_api_key: "*****"
directory_index:
  name: ceda-dirs

```

## Running

The indexer can be run using the helper script provided by [rabbit_indexer repo](https://github.com/cedadev/rabbit-index-ingest/blob/master/README.md#configuration).
This uses an entry script and parses the config file to run your selected queue_consumer_class: 

`rabbit_event_indexer --conf <path_to_configuration_file>`