# Rabbit DBI Elastic Indexer

This provides the code to read from the rabbit queue and process updates
to the directory index.

Watched events:
- DEPOSIT/REMOVE - For 00READMES
- MKDIR
- RMDIR
- SYMLINK
