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
import logging

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s [%(levelname)s] [%(threadName)s] %(message)s')

class Thread_send_block(threading.Thread):
    def __init__(self, data_block, send_client, id, lock, name):
        super(Thread_send_block, self).__init__(name=name)
        self.data_block = data_block
        self.send_client = send_client
        self.id = id
        self.lock = lock
        self.start_time = None
        self.end_time = None

    def run(self):
        mess = message.Message_Data_Client(self.data_block, self.id, "0.0.0.0", 0)    # how to send out a data whose type is a struct
        with self.lock:
            try:
                self.start_time = time.time()
                logger.info(f"Sending data block {self.id}")
                util.send_mess(self.send_client, mess)
                logger.info(f"Sent data block {self.id}")
            except:
                pass
            try:
                mess_send_response, error_str = util.recive_mess(self.send_client)
                if not mess_send_response:
                    raise TimeoutError(error_str)
                logger.info(f"Received response for data block {self.id}")

            except TimeoutError:
                logger.info(f"Response timed out... {error_str}")

            self.end_time = time.time()
    
class Client:
    def __init__(self, client_host, client_port):
        self.block_send_out = 0  # store the count of blocks which have been sent out
        self.server_count = 0  # store the count of servers
        self.server_host = [""]  # store the host of each server
        self.server_port = [0]  # store the port of each server
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
            id = i + 1
            ip = self.server_host[id]
            port = self.server_port[id]
            client_thread = threading.Thread(target=self.connect_to_server, args=(ip, port, id), name="client_thread")
            client_threads.append(client_thread)
            client_thread.start()
        
        for client_thread in client_threads:
            client_thread.join()

        logger.info(f"All Server Connected...")
    
    def connect_to_server(self, ip, port, id):
        while True:
            try:
                client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                client_socket.connect((ip, port))

                logger.info(f"Connected to server {ip}:{port}")

                # add the new connection to the send_clients list
                self.send_clients[id] = client_socket

                logger.debug("add connect over") 

                break
            except Exception as e:
                logger.info(f"Error connecting to peer {ip}:{port}: {e}")
                time.sleep(1)
                # add retry logic or wait for a period before connecting again 

        logger.debug("connect over")   

    def send_data(self):
        # considering a single entry server for now
        test_round = int(input('please input the test round number: '))
        while True:
            data_len = int(input('please input the size of the data: '))
            block_cnt = int(input('please input the count of the data block: '))
            entry_cnt = int(input('please input the count of the entry server: '))
            block_id = self.block_send_out + 1

            n = entry_cnt
            chars = string.ascii_lowercase + string.digits + string.ascii_uppercase
            random_data = ''.join(random.choices(chars, k=data_len))  # create a random string as datas
            block_size = math.ceil(data_len / block_cnt) 
            data_blocks = [random_data[i:i+block_size] for i in range(0, data_len, block_size)]
            each_cnt = int(block_cnt / n)
            ind_block = 0
            # print(data_blocks, self.block_id)

            tsbs = []
            start_ind = 0
            end_ind = each_cnt
            for ind in range(n):  # start sending the data blocks
                print(start_ind, end_ind, each_cnt)

                tsb = threading.Thread(target=self.send_block, args=(data_blocks, start_ind, end_ind, ind+1, block_id), name=f"send_data_to_{ind+1}")
                tsb.start()
                tsbs.append(tsb)

                start_ind += each_cnt
                end_ind = min(block_cnt, end_ind + each_cnt)
                
            
            all_start_time = time.time()
            for tsb in tsbs:
                tsb.join()
            all_end_time = time.time()
            logger.info(f"Data transfer time: {all_end_time - all_start_time} seconds")
            
            self.block_send_out += block_cnt
            logger.info('the data has been sent to entry servers(senders)')
            
            test_round -= 1
            if test_round == 0:
                break
    
    def send_block(self, data_blocks, start_ind, end_ind, server_id, block_id):
        print(start_ind, end_ind)
        send_lock = threading.Lock()
        for ind_block in range(start_ind, end_ind):
            data_block = data_blocks[ind_block]
            id = block_id + ind_block
            print(id, block_id, ind_block)
            mess = message.Message_Data_Client(data_block, id, "0.0.0.0", 0)    # how to send out a data whose type is a struct
            send_client = self.send_clients[server_id]
            with send_lock:
                while True:
                    try:
                        start_time = time.time()
                        logger.info(f"Sending data block {id}")
                        util.send_mess(send_client, mess)
                        logger.info(f"Sent data block {id}")
                    
                    except: # reconnect
                        send_client.close()
                        client_thread = threading.Thread(target=self.connect_to_server, args=(self.server_host[server_id], self.server_port[server_id], server_id), name="connect_thread")
                        client_thread.start()
                        client_thread.join()
                        send_client = self.send_clients[server_id]
                        logger.debug("reconnect over") 
                        continue
                    try:
                        mess_send_response, error_str = util.recive_mess(send_client)
                        if not mess_send_response:
                            raise TimeoutError(error_str)
                        logger.info(f"Received response for data block {id}")
                        break
     
                    except TimeoutError as Te:
                        logger.info(f"Response timed out... {Te}")

                end_time = time.time()
                transfer_time = end_time - start_time
                logger.info(f"Data block {id} transfer time: {transfer_time} seconds")

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
    config = util.load_config("./config.json")

    cloud_host = config["cloud_host"]
    cloud_port = config["cloud_port"]
    # no need?
    Edge_Cloud = Client(cloud_host, cloud_port)  # create an edge server cloud
    # server 1 is entry server, for example.
    Edge_Cloud.server_count = len(config["server_host_public"])
    Edge_Cloud.server_host = Edge_Cloud.server_host + config["server_host_public"]
    Edge_Cloud.server_port = Edge_Cloud.server_port + config["server_port"]
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
