# this is the code file for the cloud
import threading
import random
import string
import socket
import message
import time
import math
import csv
import pickle
import re
import util
import logging
import queue

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s [%(levelname)s] [%(threadName)s] %(message)s')
logger = logging.getLogger(__name__)

handler = logging.FileHandler('cloud.log', encoding='UTF-8')
handler.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s [%(levelname)s] [%(threadName)s] %(message)s')
handler.setFormatter(formatter)

logger.addHandler(handler)

import argparse

def construct_hyper_param(parser):
    parser.add_argument('-test_round', required=True, default=1, type=int,
                        help='test round')
    parser.add_argument('-data_len', required=True, default=512000, type=int,
                        help='data size')
    parser.add_argument('-block_cnt', required=True, default=1, type=int,
                        help='data block num')
    parser.add_argument('-entry_cnt', required=True, default=1, type=int,
                        help='entry server cnt')

    args = parser.parse_args()

    random_seed = 1234

    return args

class Client:
    def __init__(self, client_host, client_port):
        self.block_send_out = 0  # store the count of blocks which have been sent out
        self.server_count = 0  # store the count of servers
        self.server_host = [""]  # store the host of each server
        self.server_port = [0]  # store the port of each server
        self.send_clients ={} # store the client established for connecting of each server
        self.data_dict = dict()  # store the statues of each data transmission
        self.ready = True   # whether the cloud is ready for a new file
        self.args = None
        self.entry_cnt = None
        self.timeout = None
        self.data_que = dict()
        self.que_lock = dict()
        self.lock = threading.Lock()
        self.thread_exit_event = threading.Event()
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
                # client_socket.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 16*1024*1024)
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
        test_round = self.args.test_round
        data_len = self.args.data_len
        block_cnt = self.args.block_cnt
        entry_cnt = self.args.entry_cnt
        # block_id = self.block_send_out + 1
        
        n = entry_cnt
        self.entry_cnt = entry_cnt
        chars = string.ascii_lowercase + string.digits + string.ascii_uppercase
        random_data = ''.join(random.choices(chars, k=data_len))  # create a random string as datas
        block_size = math.ceil(data_len / block_cnt) 
        data_blocks = [random_data[i:i+block_size] for i in range(0, data_len, block_size)]
        each_cnt = int(block_cnt / n)
        ind_block = 0
        block_id = self.block_send_out + 1
        # print(data_blocks, self.block_id)
        
        experiment_data = []
        while True:
            # data_len = int(input('please input the size of the data: '))
            # block_cnt = int(input('please input the count of the data block: '))
            # entry_cnt = int(input('please input the count of the entry server: '))
            # block_id = self.block_send_out + 1
            
            # n = entry_cnt
            # chars = string.ascii_lowercase + string.digits + string.ascii_uppercase
            # random_data = ''.join(random.choices(chars, k=data_len))  # create a random string as datas
            # block_size = math.ceil(data_len / block_cnt) 
            # data_blocks = [random_data[i:i+block_size] for i in range(0, data_len, block_size)]
            # each_cnt = int(block_cnt / n)
            # ind_block = 0
            # # print(data_blocks, self.block_id)

            tsbs = []
            start_ind = 0
            end_ind = each_cnt
            for ind in range(n):  # start sending the data blocks
                # print(start_ind, end_ind, each_cnt)

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
            experiment_data.append(all_end_time - all_start_time)
            
            self.block_send_out += block_cnt
            logger.info('the data has been sent to entry servers(senders)')
            
            test_round -= 1
            if test_round == 0:
                # 计算平均值
                average_time = sum(experiment_data) / len(experiment_data)

                # 计算最大值和最小值
                max_time = max(experiment_data)
                min_time = min(experiment_data)

                logger.info(f"平均传输时间: {average_time}")
                logger.info(f"最大传输时间: {max_time}")
                logger.info(f"最小传输时间: {min_time}")
                
                current_time = time.strftime("%Y_%m_%d_%H_%M")
                csv_file_path = f'experiment_{data_len}_{block_cnt}_{entry_cnt}_{current_time}.csv'

                with open(csv_file_path, 'w', newline='') as csvfile:
                    # 创建CSV写入对象
                    csv_writer = csv.writer(csvfile)

                    # 写入表头
                    csv_writer.writerow(['Experiment Iteration', 'Transmission Time'])

                    # 写入数据
                    for i, time_value in enumerate(experiment_data, start=1):
                        csv_writer.writerow([i, time_value])
                    
                    csv_writer.writerow(["average", average_time])
                    csv_writer.writerow(["max", max_time])
                    csv_writer.writerow(["min", min_time])
                
                break
    
    def send_block(self, data_blocks, start_ind, end_ind, server_id, block_id):
        send_lock = threading.Lock()

        with self.que_lock[server_id]:
            for ind_block in range(start_ind, end_ind):
                data_block = data_blocks[ind_block]
                id = block_id + ind_block
                # print(id, block_id, ind_block)
                mess = message.Message_Data_Client(data_block, id, "0.0.0.0", 0)
                self.data_que[server_id].put(mess)
        
        logger.info(f"Server {server_id} start to send data block")

        while True:
            while(not self.data_que[server_id].empty()):
                mess = self.data_que[server_id].queue[0]
                send_client = self.send_clients[server_id]
                # send_client.settimeout(self.timeout)
                with send_lock:
                    start_time = time.time()
                    try:
                        logger.info(f"Sending data block {mess.data_id}")
                        util.send_mess(send_client, mess)
                        logger.info(f"Sent data block {mess.data_id}")
                        
                        if self.timeout:
                            if (self.timeout <= (time.time() - start_time)):
                                raise TimeoutError("send timeout error...")

                            remain_time = self.timeout - (time.time() - start_time)


                        while True:
                            if self.timeout:
                                logger.info(f"Remain time for data block {mess.data_id}: {remain_time}")
                                mess_send_response, error_str = util.recive_mess(send_client, remain_time)
                            else:
                                mess_send_response, error_str = util.recive_mess(send_client)
                            # logger.info(f"Received response for data block {mess_send_response.data_id}")
                            if not mess_send_response:
                                raise TimeoutError(error_str)
                            
                            if mess_send_response.data_id == mess.data_id:
                                logger.info(f"Received response for data block {mess.data_id}")
                                break
                            else:
                                logger.info(f"Continue Reciving...")

                            if self.timeout:
                                if (self.timeout <= (time.time() - start_time)):
                                    raise TimeoutError("recive timeout error...")
                                
                                remain_time = self.timeout - (time.time() - start_time)
                            # logger.info(f"remain_time: {remain_time}")
                        
                        end_time = time.time()
                        transfer_time = end_time - start_time
                        logger.info(f"Data block {mess.data_id} transfer time: {transfer_time} seconds")

                    except socket.timeout as Te:
                        random_id = random.choice([i for i in range(1, self.entry_cnt + 1) if i != server_id])
                        logger.error(f"Transfer timed out... {Te}")
                        with self.que_lock[random_id]:
                            self.data_que[random_id].put(mess)
                            logger.error(f"server {random_id} add data block {mess.data_id}, now size: {self.data_que[random_id].qsize()}")
                        
                        end_time = time.time()
                        transfer_time = end_time - start_time
                        logger.error(f"Data block {mess.data_id} timeout time: {transfer_time} seconds")
                        if self.timeout:
                            send_client.settimeout(None)
                    
                    except TimeoutError as Te:
                        random_id = random.choice([i for i in range(1, self.entry_cnt + 1) if i != server_id])
                        logger.error(f"Transfer timed out... {Te}")
                        with self.que_lock[random_id]:
                            self.data_que[random_id].put(mess)
                            logger.error(f"server {random_id} add data block {mess.data_id}, now size: {self.data_que[random_id].qsize()}")
                        
                        end_time = time.time()
                        transfer_time = end_time - start_time
                        logger.error(f"Data block {mess.data_id} timeout time: {transfer_time} seconds")
                        if self.timeout:
                            send_client.settimeout(self.timeout)
                    
                    except Exception as e: # reconnect
                        logger.error(f"Error while sending data block {mess.data_id}: {e}")
                        send_client.close()
                        client_thread = threading.Thread(target=self.connect_to_server, args=(self.server_host[server_id], self.server_port[server_id], server_id), name="connect_thread")
                        client_thread.start()
                        client_thread.join()
                        send_client = self.send_clients[server_id]
                        logger.debug("reconnect over") 

                        random_id = random.choice([i for i in range(1, self.entry_cnt + 1) if i != server_id])
                        with self.que_lock[random_id]:
                            self.data_que[random_id].put(mess)
                            logger.info(f"server {random_id} add data block {mess.data_id}, now size: {self.data_que[random_id].qsize()}")
                        if self.timeout:
                            send_client.settimeout(None)

                    with self.que_lock[server_id]:
                        self.data_que[server_id].get()
                        logger.info(f"server {server_id} remove data block {mess.data_id} now size: {self.data_que[server_id].qsize()}")
                    
                    self.thread_exit_event.set()
            
            if self.data_que_empty():
                break
            
            self.thread_exit_event.wait()
            self.thread_exit_event.clear() 

                    

    def data_que_empty(self):
        res = True
        for i in range(self.entry_cnt):
            logger.info(f"server {i+1}: {self.data_que[i+1].qsize()}")
            if not self.data_que[i+1].empty():
                res = False
        return res


if __name__ == "__main__":  # can this code be arranged in server?
    parser = argparse.ArgumentParser()
    args = construct_hyper_param(parser)

    config = util.load_config("./config.json")

    cloud_host = config["cloud_host"]
    cloud_port = config["cloud_port"]
    # no need?
    Edge_Cloud = Client(cloud_host, cloud_port)  # create an edge server cloud
    # server 1 is entry server, for example.
    Edge_Cloud.server_count = len(config["server_host_public"])
    Edge_Cloud.server_host = Edge_Cloud.server_host + config["server_host_public"]
    Edge_Cloud.server_port = Edge_Cloud.server_port + config["server_port"]
    Edge_Cloud.args = args
    for i in range(Edge_Cloud.server_count):
        Edge_Cloud.data_que[i+1] = queue.Queue()
        Edge_Cloud.que_lock[i+1] = threading.Lock()
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
