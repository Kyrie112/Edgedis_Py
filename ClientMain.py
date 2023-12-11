import socket
import threading
import uuid
import json
import pickle

HOST = '127.0.0.1'
PORT = 8888

import message 


# 随机生成一个唯一 ID
UID = str(uuid.uuid4())
# 创建一个 TCP 协议的套接字对象
client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
# 连接指定主机和端口
client_socket.connect((HOST, PORT))

client_socket2 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

client_socket2.connect((HOST, 8889))

def receive_response(client):
    pass



def send_message():
    while True:
        print("server id:")
        id = input()
        print("message:")
        message_str = input()

        # consturct message
        mess = message.Message_Data_Client(message_str, 1, "0.0.0.0", 0)

        mess_b = pickle.dumps(mess)

        if id == "1":
            client_socket.sendall(mess_b)

            # response
            try:
                # 在代码区域内设置超时时间
                client_socket.settimeout(0.25)

                # 执行接收操作
                mess_response_b = client_socket.recv(1024)
                mess_response = pickle.loads(mess_response_b)

                print("Received Response:", mess_response.type, mess_response.id, mess_response.data_id, mess_response.status, mess_response)


            except socket.timeout:
                print("Response timed out.")

            finally:
                # 恢复之前的超时时间或取消超时限制
                client_socket.settimeout(None)

        else:
            client_socket2.sendall(mess_b)
        
        # # 构造消息字典
        # msg_dict = {
        #     'sender_id': 0,
        #     'type': "data",
        #     'message': message_str
        # }
 
        # # 将消息字典序列化为JSON格式字符串
        # json_str = json.dumps(msg_dict)
 
        # # 将消息发送给服务端
        
        # if id == "1":
        #     client_socket.sendall(json_str.encode('utf-8'))
        # else:
        #     client_socket2.sendall(json_str.encode('utf-8'))
    

if __name__ == '__main__':
    # 启动发送消息的线程
    t2 = threading.Thread(target=send_message)
    t2.daemon = True
    t2.start()
 
    # 等待发送线程结束，结束接收线程
    t2.join()