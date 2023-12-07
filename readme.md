# Deploy

A example of sending messages from the cloud to edge servers.

## for each edge server

- to create a new terminal, e.g., node 1 on the first edge server
- run the following command

```shell
python PeerServer.py -server_id 1
```

 (note: here 1 means the ID of current edge server, the first edge server's ID is 1, the second's is 2, and so on.)

## for cloud

- to create a new terminal, e.g., node 1 on the first edge server
- run the following command

```
python ClientMain.py
```

- Enter the entry server ID and the message to send as prompted.

  ​