# this is the code file for the cloud
import threading
import random
import string
import socket
import message
import time
import math
import pickle
import re
import util


class Thread_send_block(threading.Thread):
    def __init__(self, data_block, send_client, id, lock):
        super(Thread_send_block, self).__init__()
        self.data_block = data_block
        self.send_client = send_client
        self.id = id
        self.lock = lock

    def run(self):
        mess = message.Message_Data_Client(self.data_block, self.id, "0.0.0.0", 0)    # how to send out a data whose type is a struct
        start_time = time.time()
        with self.lock:
            util.send_mess(self.send_client, mess)

            try:
                mess_send_response = util.recive_mess(self.send_client, 0.25)
                # print(mess_send_response)
                if not mess_send_response:
                    raise TimeoutError("Receive time exceeded")

            except TimeoutError as Te:
                pass

        end_time = time.time()
        print(f"elapsed time: {end_time - start_time}")
            





class Thread_send_data(threading.Thread):
    def __init__(self, data_len, block_cnt, send_clients, block_id):  # the data here needs to be a string
        super(Thread_send_data, self).__init__()
        self.data_len = data_len  # create a string with the length is data_len
        self.block_cnt = block_cnt  # the count of the data blocks
        self.send_clients = send_clients # the list of server ids which are going to become senders
        # self.server_host = server_host  # the list of server hosts which are going to become senders
        # self.server_port = server_port  # the list of server ports which are going to become receivers(should be senders)
        self.block_id = block_id

    def run(self):
        n = len(self.send_clients)
        chars = string.ascii_lowercase + string.digits + string.ascii_uppercase
        random_data = ''.join(random.choices(chars, k=self.data_len))  # create a random string as datas
        block_size = math.ceil(self.data_len / self.block_cnt) 
        data_blocks = [random_data[i:i+block_size] for i in range(0, self.data_len, block_size)]
        each_cnt = int(self.block_cnt / n)
        ind_block = 0
        # print(data_blocks, self.block_id)

        tsbs = []
        locks = []
        print("data get...")
        for ind in range(n):  # start sending the data blocks
            cnt = 0
            lock = threading.Lock()
            while ind_block < len(data_blocks):
                # print(ind, ind_block, data_blocks)
                tsb = Thread_send_block(data_blocks[ind_block], self.send_clients[ind], self.block_id + ind_block, lock)
                cnt += 1
                ind_block += 1
                tsb.start()
                tsbs.append(tsb)
                if cnt == each_cnt:  # enough data blocks for this server
                    break
            locks.append(lock)
        
        for tsb in tsbs:
            tsb.join()

class Client:
    def __init__(self, client_host, client_port):
        self.block_send_out = 0  # store the count of blocks which have been sent out
        self.server_count = 0  # store the count of servers
        self.server_host = []  # store the host of each server
        self.server_port = []  # store the port of each server
        self.send_clients ={} # store the client established for connecting of each server
        self.data_dict = dict()  # store the statues of each data transmission
        self.ready = True   # whether the cloud is ready for a new file
        self.lock = threading.Lock()
        self.c = socket.socket()
        self.c.bind((client_host, client_port))
        self.c.listen(6)  # the parament can be set by yourself
    
    def start(self):
        client_threads = []
        for i in range(self.server_count):
            ip = self.server_host[i]
            port = self.server_port[i]
            id = i + 1
            client_thread = threading.Thread(target=self.connect_to_server, args=(ip, port, id), name="client_thread")
            client_threads.append(client_thread)
            client_thread.start()
        
        for client_thread in client_threads:
            client_thread.join()

        print(f"All Server Connected...")
    
    def connect_to_server(self, ip, port, id):
        while True:
            try:
                client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                client_socket.connect((ip, port))

                print(f"Connected to server {ip}:{port}")

                # add the new connection to the send_clients list
                with self.lock:
                    self.send_clients[id] = client_socket

                break
            except Exception as e:
                print(f"Error connecting to peer {ip}:{port}: {e}")
                time.sleep(1)
                # add retry logic or wait for a period before connecting again    

    def send_data(self):
        # considering a single entry server for now
        while True:
            data_len = int(input('please input the size of the data: '))
            block_cnt = int(input('please input the count of the data block: '))
            entry_cnt = int(input('please input the count of the entry server: '))

            tsd = Thread_send_data(data_len, block_cnt, [self.send_clients[i + 1] for i in range(entry_cnt)],
                                self.block_send_out + 1)
            tsd.start()
            tsd.join()  # need to wait until each server has received the data
            self.block_send_out += block_cnt
            print('the data has been sent to entry servers(senders)')

    def receive_response(self):  # this function is used to receiver response
        while True:
            conn, addr = self.c.accept()
            data = conn.recv(2048)
            data = data.decode()  # something wrong here(how to let data be a struct?)
            if data.type == 'data_sender_response':
                self.data_dict[data.id] = True  # tag it
            elif data.type == 'sign_in':    # new server has joined(not complete here, don't know how to continue)
                self.server_count += 1
                self.server_host.append(data.server_host)
                self.server_port.append(data.server_port)

    def Data_Client_Response_Handle(self, mess):
        if mess.status:
            self.data_dict[mess.data_id] = True
        tag = True
        # check whether all data has been transmitted to the servers successfully
        for ind in range(self.data_send_out):
            if self.data_dict.get(ind) is None:
                tag = False
                break
        self.ready = tag


if __name__ == "__main__":  # can this code be arranged in server?
    cloud_host = "127.0.0.1"
    cloud_port = 8051
    # no need?
    Edge_Cloud = Client(cloud_host, cloud_port)  # create an edge server cloud
    # server 1 is entry server, for example.
    Edge_Cloud.server_count = 2
    Edge_Cloud.server_host = ["127.0.0.1", "127.0.0.1"]
    Edge_Cloud.server_port = [8888, 8889]
    Edge_Cloud.start()
    trr = threading.Thread(target=Edge_Cloud.send_data())    # a thread which can be run forever
    trr.setDaemon(True)
    trr.start()

    # while True:
    #     flag = input('need to send data? 1.Yes 0.No')
    #     if flag == 1:
    #         Edge_Cloud.send_data()
    #     else:
    #         break
    # print('End')
