package replication

/**
This package is for the replication feature of marketstore.
Replication of marketstore is based on WAL (Write Ahead Log).
When the marketstore instance is started, the following 2 threads are initialized:

- WAL sender
	WAL sender is a thread running only on a master server to send WAL records to replica servers.
	When marketstore processes a write request and flushes the record to a primary store,
	WAL sender is triggered and it sends the record to replica servers through a GRPC streaming connection.

	Also, WAL sender copies all data stored in the master server to replica servers through write API
	in order to initialize the replication when the marketstore starts.

- WAL receiver
	WAL receiver is a thread running only on replica servers to listen to WAL records sent from the master server.
	When WAL record is sent, WAL receiver stores it to WAL file and replay it.
 */