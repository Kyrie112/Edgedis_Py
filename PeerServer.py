import socket
import threading
import logging
import argparse
import time
import json

logger = logging.getLogger(__name__)

# 配置全局日志
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s [%(levelname)s] [%(threadName)s] %(message)s')

peers = [("127.0.0.1", 8888, 1),
         ("127.0.0.1", 8889, 2)]
        #  ("127.0.0.1", 8890, 3)]

def construct_hyper_param(parser):
    parser.add_argument('-server_id', required=False, default=1, type=int,
                        help='Id for deploied server')

    args = parser.parse_args()

    return args

class PeerServer:
    def __init__(self, ip, port, id):
        self.id = id
        self.ip = ip
        self.port = port
        self.cluster_num = 0
        self.peers = {}  # 存储其他服务器的ip和端口号
        self.send_clients = {}
        self.receive_clients = {}
        self.lock = threading.Lock()

    def start(self):
        # 启动服务端监听
        server_thread = threading.Thread(target=self.start_server, name="server_thread")
        server_thread.start()

        # 启动客户端连接
        client_threads = []
        for peer_ip, peer_port, peer_id in self.peers.values():
            client_thread = threading.Thread(target=self.connect_to_peer, args=(peer_ip, peer_port, peer_id), name="client_thread")
            client_threads.append(client_thread)
            client_thread.start()

        # 等待所有客户端连接线程完成
        for client_thread in client_threads:
            client_thread.join()

        logger.info(f"All Server Connected...")


    def start_server(self):
        server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server.bind((self.ip, self.port))
        server.listen(5)
        print(f"Server listening on {self.ip}:{self.port}...")

        while True:
            client, addr = server.accept()
            print(f"Accepted connection from {addr[0]}:{addr[1]}")


            client_handler = threading.Thread(target=self.handle_client, args=(client, addr), name="handle_client")
            client_handler.start()

    def handle_client(self, client, addr):
        # 在这里进行与客户端的通信
        while True:
            try:
                data = client.recv(1024)
                if not data:
                    break

                json_str = data.decode('utf-8')
                msg_dict = json.loads(json_str)
                sender_id = msg_dict["sender_id"]
                msg = msg_dict["message"]

                print(msg_dict)

                if sender_id == 0:

                    # 这块可以改成多线程的发送

                    with self.lock:

                        new_msg_dict = {"sender_id": self.id, 'type': "data", "message": msg}

                        new_json_str = json.dumps(new_msg_dict)

                        for id, send_client in self.send_clients.items():
                                try:
                                    send_client.send(new_json_str.encode('utf-8'))
                                except Exception as e:
                                    print(f"Error sending message to {id}: {e}")


            except Exception as e:
                print(f"Error handling client {addr[0]}:{addr[1]}: {e}")
                break

        client.close()

    def connect_to_peer(self, peer_ip, peer_port, peer_id):
        while True:
            try:
                client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                client_socket.connect((peer_ip, peer_port))
                print(f"Connected to peer {peer_ip}:{peer_port}")

                # 将新连接加入到 peers 和 clients 列表中
                with self.lock:
                    self.send_clients[peer_id] = client_socket

                break
            except Exception as e:
                print(f"Error connecting to peer {peer_ip}:{peer_port}: {e}")
                time.sleep(1)
                # 可以添加重试逻辑，或者等待一段时间后再次尝试

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    args = construct_hyper_param(parser)

    local_ip, local_port, local_id = peers[args.server_id - 1]

    # 启动多个服务器实例
    server = PeerServer(local_ip, local_port, local_id)
    
    # 将服务器添加到服务器1的peers列表中
    with server.lock:
        for ip, port, id in peers:
            if id != local_id:
                server.peers[id] = (ip, port, id)
    
    server.cluster_num = len(peers)

    # 启动服务器
    server_thread = threading.Thread(target=server.start, name="server_thread")

    server_thread.start()

    # 等待服务器线程完成
    server_thread.join()

