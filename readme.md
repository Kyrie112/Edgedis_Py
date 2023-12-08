# Deploy

A example of sending messages from the cloud to edge servers.

## for each edge server

- to create a new terminal, e.g., node 1 on the first edge server
- run the following command

```shell
python server.py -server_id 1
```

 (note: here 1 means the ID of current edge server, the first edge server's ID is 1, the second's is 2, and so on.)

## for cloud

- to create a new terminal, e.g., node 1 on the first edge server
- run the following command

```
python client.py
```

- Enter the data size, data block num and the entry server num to send as prompted.

# 更新部分
- 新增了服务器和云端启动逻辑，启动时与所有服务器建立连接，将建立连接的客户端保存。
- 重写了一下socket的发送和接收逻辑，以及如何接受response。
- 将```Data_client_handle, Data_sender_handle```等函数实现了，可以阅读代码了解发送和接收消息的逻辑。
- 已基本实现云端发送数据，服务器自动转发的功能，重传逻辑还未实现。
- 可以找个时间讨论一下下一步的分工实现