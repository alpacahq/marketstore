package replication

/**
This package is for the replication feature of marketstore.
Replication of marketstore is based on WAL (Write Ahead Log).
When the marketstore instance starts, the following 2 threads are initialized:

- WAL sender
	WAL sender is a thread running only on a master instance to send WAL records to replica servers.
	When marketstore processes a write request and flushes the record to a primary store,
	WAL sender is triggered and it sends the record to replica servers through a GRPC streaming connection.

	Delete API is not supported for now, only Write API triggers the replication.

- WAL receiver
	WAL receiver is a thread running only on replica instances to listen to WAL records sent from the master instance.
	When WAL record is sent, WAL receiver stores it to WAL file and replay it.
 */
