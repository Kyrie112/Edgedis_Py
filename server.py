# this is the code file for node and how they handle different kinds of message
import socket
import message
import util
import threading
import time
import logging
import pickle
import random


logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s [%(levelname)s] [%(threadName)s] %(message)s')

import argparse

def construct_hyper_param(parser):
    parser.add_argument('-server_id', required=True, default=1, type=int,
                        help='Id for deploied server')

    args = parser.parse_args()

    random_seed = 1234

    return args

class Node:
    def __init__(self, server_host, server_port, server_id):
        self.server_host = server_host  # local ip
        self.server_port = server_port  # local port
        # self.client_host = client_host  # cloud ip
        # self.client_port = client_port  # cloud port
        self.coordinator_host = server_host  # these three parament need to be updated during the sign_in process and
        # change when a new coordinator is elected
        self.coordinator_port = server_port
        self.coordinator_id = 0
        self.status = 'follower'  # follower, candidate or coordinator
        self.sub_status = 'receiver'  # receiver or sender
        self.last_heartbeat = None  # time out 
        self.data_id = -1  # if the node is a sender, it needs to update this, current data id
        self.data_to_send = ""  # if the node is a sender, it needs to update this, current data
        self.server_host_list = [""]  # the ith value is the ith server's ip
        self.server_port_list = [0]  # all server information
        self.send_clients = dict()
        self.receive_clients = dict()
        self.data_ind = dict()  # store each data block with an id
        self.max_id = 0  # recive max data id
        self.num = 2 # the num of servers
        self.term = 0  # need to be update
        self.id = server_id  # server id
        self.vote_for = 0  # the node's id of the node vote for during this term
        self.vote_map = dict()  # store the vote condition
        self.store_map = dict()  # store the store condition(only sender can use it, it needs to be refreshed when senders need to send out a new data block)
        self.lock = threading.Lock()
        self.sublock = threading.Lock() # need another lock here

    
    def sign_in(self):  # need to be implemented
        # start server listen
        server_thread = threading.Thread(target=self.start_server, name="server_thread")
        server_thread.start()

        # create client to connect other server
        client_threads = []
        for i in range(self.num):
            id = i + 1
            if id != self.id:
                client_thread = threading.Thread(target=self.connect_to_other, args=(self.server_host_list[id], self.server_port_list[id], id), name="connect_thread")
                client_threads.append(client_thread)
                client_thread.start()

        # wait all server create connect
        for client_thread in client_threads:
            client_thread.join()

        logger.info(f"All Server Connected...")

        # self.start_vote()
    
    def start_server(self):
        server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server.bind((self.server_host, self.server_port))
        server.listen(5)
        logger.info(f"Server listening on {self.server_host}:{self.server_port}...")

        while True:
            client, addr = server.accept()

            client_handler = threading.Thread(target=self.handle_message, args=(client, ), name="handle_message")
            client_handler.start()
    
    def connect_to_other(self, ip, port, id):
        while True:
            try:
                client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                client_socket.connect((ip, port))

                mess_sign_in = message.Message_Sign_In(self.server_host, self.server_port, self.id)

                # append in send client list
                with self.lock:
                    self.send_clients[id] = client_socket

                self.Send(id, 1, mess_sign_in) # send_client

                logger.info(f"Connected to server {ip}:{port}")


                break
            except Exception as e:
                logger.info(f"Error connecting to server {ip}:{port}: {e}")
                time.sleep(1)
    
    def start_vote(self):
        while True:
            logger.debug(f"c: {self.coordinator_id}, s: {self.status}")
            if self.status == 'coordinator':
                time.sleep(0.05)
                self.Broadcast_Heartbeat()
                continue
            now_time = self.last_heartbeat = time.time()
            # election timeout
            while True:
                random_time = 0.125 + random.uniform(0, 0.125)
                time.sleep(random_time)
                logger.debug(f"c: {self.coordinator_id}, s: {self.status}")
                now_time = time.time()
                if now_time - self.last_heartbeat > random_time:
                    logger.debug(f"time: {now_time - self.last_heartbeat}")
                    break

            # need to broadcast the vote request
            self.status = 'candidate'  # become candidate
            self.term += 1  # change the term
            self.vote_for = self.id  # vote for itself
            self.vote_map.clear()
            self.vote_map[self.id] = True
            mess = message.Message_Vote(self.id, self.term, self.max_id, self.id, len(self.data_ind),
                                        self.server_host, self.server_port)

            # broadcast the data
            threads = []
            with self.lock:            
                for id, send_client in self.send_clients.items():
                        thread = threading.Thread(target=self.Send_and_Handle_Response, args=(id, 1, mess), name=f"send_vote_to_{id}") # send_client
                        thread.start()
                        threads.append(thread)

            for thread in threads:
                thread.join()
            

    def handle_message(self, client):  # this function need to be run by a thread so that it can be run forever
            from_id = None
            while True:
                try:
                    # if from_id is not None:
                    #     logger.debug(f"server is watting message from {from_id}...")

                    mess, error_str = self.Recive(client)

                    if not mess:
                        logger.info(error_str + f"{from_id}")
                        break

                    if mess.type == 'sign_in':
                        self.receive_clients[mess.server_id] = client
                        from_id = mess.server_id
                        current_thread = threading.current_thread()
                        current_thread.name = f"handle_message_from_{from_id}"
                    elif mess.type == 'data_client':
                        self.receive_clients[0] = client
                    
                    if self.status == 'follower':
                        self.follower_handle(mess)
                    elif self.status == 'candidate':
                        self.candidate_handle(mess)
                    elif self.status == 'coordinator':
                        self.coordinator_handle(mess)


                except Exception as e:
                    client.close()
                    logger.info(f"Error handling client :{e}")
                    break
            
            logger.info(f"Handling client over...")

    
    # these three functions is used for different roles
    def follower_handle(self, mess):
        if mess.type == 'data_client':
            self.Data_Client_Handle(mess)
        elif mess.type == 'heartbeat':
            self.Heartbeat_Handle(mess)
        elif mess.type == 'vote':
            self.Vote_Handle(mess)
        elif mess.type == 'data_sender':
            self.Data_Sender_Handle(mess)
        elif mess.type == 'data_sender_response':
            if self.sub_status == 'sender':
                self.Data_Sender_Response_Handle(mess)
        elif mess.type == 'data_request':
            self.Data_Request_Handle(mess)
        elif mess.type == 'data_supplement':
            self.Data_Supplement_Handle(mess)

        return

    def candidate_handle(self, mess):
        if mess.type == 'data_client':
            self.Data_Client_Handle(mess)
        elif mess.type == 'heartbeat':
            self.Heartbeat_Handle(mess)
        elif mess.type == 'vote':
            self.Vote_Handle(mess)
        elif mess.type == 'data_sender':
            self.Data_Sender_Handle(mess)
        elif mess.type == 'vote_response':  # compared with follower it needs to handle this type of message
            self.Vote_Response_Handle(mess)
        elif mess.type == 'data_sender_response':
            if self.sub_status == 'sender':
                self.Data_Sender_Response_Handle(mess)
        return

    def coordinator_handle(self, mess):
        if mess.type == 'data_client':
            self.Data_Client_Handle(mess)
        elif mess.type == 'heartbeat':
            self.Heartbeat_Handle(mess)
        elif mess.type == 'vote':
            self.Vote_Handle(mess)
        elif mess.type == 'data_sender':
            self.Data_Sender_Handle(mess)
        elif mess.type == 'data_request_response':
            self.Data_Request_Response_Handle(mess)
        elif mess.type == 'data_sender_response':
            if self.sub_status == 'sender':
                self.Data_Sender_Response_Handle(mess)
        elif mess.type == 'heartbeat_response':  # compared with follower it needs to handle this type of message
            self.Heartbeat_Response_Handle(mess)
        return

    # these functions are used for handle different types of message and different roles need to handle them differently
    def Heartbeat_Handle(self, mess):  # need to be implemented
        self.last_heartbeat = time.time()

        miss_data_block = []
        from_host = mess.from_host
        from_port = mess.from_port
        pre_max_id = self.max_id
        id = mess.id
        s = self.receive_clients[id]

        # ask for the missing data
        for ind in range(pre_max_id + 1, self.max_id, 1):
            miss_data_block.append(ind)
        # check the term of the coordinator
        if self.term < mess.term:
            self.term = mess.term
            if self.coordinator_id != mess.id:
                logger.info(f"server {mess.id} become the coordinator...")
            self.coordinator_id = mess.id
            
        # create a response and send to the coordinator
        mess = message.Message_Heartbeat_Response(self.id, miss_data_block, self.max_id,
                                                  self.term, self.server_host, self.server_port)
        self.Send(id, 0, mess) # receive_client

    def Heartbeat_Response_Handle(self, mess):
        # supply the follower with the data it needs
        s = self.send_clients[mess.id]
        data_block = []
        data_block_id = mess.miss_data_block
        for ind in mess.miss_data_block:
            data_block.append(self.data_ind[ind])
        if len(data_block) != 0:
            mess1 = message.Message_Data_Supplement(data_block, data_block_id)
            self.Send(mess.id, 1, mess1) # send_client
        # check if there is any new data that followers process
        request_list = []
        if mess.max_id > self.max_id:
            self.max_id = mess.max_id
        # request the data
        for ind in range(self.max_id):
            if self.data_ind.get(ind) is None:
                request_list.append(ind)
        # send out the request
        if len(request_list) != 0:
            mess2 = message.Message_Data_Request(self.server_host, self.server_port, request_list)
            self.Send_and_Handle_Response(mess.id, 1, mess2) # send_client


        if mess.term > self.term:
            self.term = mess.term
            self.status = 'follower'

    def Vote_Handle(self, mess):
        id = mess.id
        s = self.receive_clients[id]

        # check whether it needs to support the candidate
        if mess.tot_block >= len(self.data_ind) and (
                (mess.term == self.term and self.vote_for == 0) or mess.term > self.term):
            self.term = mess.term  # change the term
            self.vote_map.clear()   # clear the vote map
            self.vote_for = mess.id
            self.status = 'follower'    # need to step back to follower
            mess_vr = message.Message_Vote_Response(self.id, True, self.term, self.server_host, self.server_port)
        else:
            mess_vr = message.Message_Vote_Response(self.id, False, self.term, self.server_host, self.server_port)
        
        self.Send(id, 0, mess_vr) # receive_client

    def Vote_Response_Handle(self, mess):
        n = self.num  # the count of the servers
        if mess.support:
            self.vote_map[mess.id] = True
        # accept
        if len(self.vote_map) > int((n + 1) / 2):
            self.status = 'coordinator'
            self.coordinator_id = self.id
            logger.info(f"server {self.id} is the coordinator!")
            self.Broadcast_Heartbeat()
        # the node falls behind, it needs to become follower
        if mess.term > self.term:
            self.status = 'follower'

    # below three functions don't have pseudocode
    def Data_Sender_Handle(self, mess):
        self.data_ind[mess.data_id] = mess.data
        self.max_id = max(self.max_id, mess.data_id)

        logger.debug(f"Received data block from {mess.id}... [data_id: {mess.data_id}, data_len: {len(mess.data)}, ip: {mess.from_host}, port: {mess.from_port}]")

        mess_response = message.Message_Data_Sender_Response(self.id, mess.data_id, "True", self.server_host, self.server_port)
        self.Send(mess.id, 0, mess_response) # receive_client

    def Data_Sender_Response_Handle(self, mess):
        logger.debug(f"Received response from {mess.id}... [data_id: {mess.data_id}, ip: {mess.from_host}, port: {mess.from_port}]")
        n = self.num
        if mess.status:  # received successfully
            self.store_map[mess.id] = True
        if len(self.store_map) > int((n + 1) / 2):  # need to tell the cloud that the data block has been transmitted
            # successfully
            self.store_map.clear()
            self.sub_status = 'receiver'  # need to step back to receiver

            mess_response = message.Message_Data_Client_Response(self.id, mess.data_id, True, self.server_host, self.server_port)
            self.Send(0, 0, mess_response) # receive_client (cloud)

    def Data_Client_Handle(self, mess):
        logger.debug(f"Received data block from cloud... [data_id: {mess.data_id}, data_len: {len(mess.data)}, ip: {mess.from_host}, port: {mess.from_port}]")

        # init some parament and store the data
        self.sub_status = 'sender'
        self.data_id = mess.data_id
        self.data_to_send = mess.data
        self.data_ind[mess.data_id] = mess.data
        self.max_id = max(self.max_id, mess.data_id)
        self.store_map[self.id] = True
       

        mess_send = message.Message_Data_Sender(self.id, mess.data, mess.data_id, self.server_host, self.server_port)

        # broadcast the data
        threads = []
        with self.lock:            
            for id, send_client in self.send_clients.items():
                    thread = threading.Thread(target=self.Send_and_Handle_Response, args=(id, 1, mess_send), name=f"send_data_to_{id}") # send_client
                    thread.start()
                    threads.append(thread)

        for thread in threads:
            thread.join()


    def Data_Request_Handle(self, mess):
        # collect the missing data of coordinator
        id = mess.id
        s = self.receive_clients[id]
        request_list = mess.request_list
        request_data = []
        for ind in request_list:
            request_data.append(self.data_ind[ind])
        mess = message.Message_Data_Request_Response(self.server_host, self.server_port, request_list,
                                                     request_data)
        self.Send(id, 0, mess) # receive_client

    def Data_Request_Response_Handle(self, mess):
        # store the new data blocks that are processed by follower
        for ind in range(len(mess.request_list)):
            self.data_ind[mess.request_list[ind]] = mess.request_data[ind]

    def Data_Supplement_Handle(self, mess):
        # store the data blocks that are processed by coordinator
        for ind in range(len(mess.data_block_id)):
            self.data_ind[mess.data_block_id[ind]] = mess.data_block[ind]
    
    def Send_and_Handle_Response(self, send_id, sor, mess, timeout=None): # timeout parameter, unit: seconds
        if sor: # 1 is send_client
            send_client = self.send_clients[send_id]
        else: # 0 is receive_client
            send_client = self.receive_clients[send_id]
        
        while True:
            # send
            try:
                if mess.type == 'data_sender':
                    logger.debug(f"Sending data block {mess.id} to server {send_id}")
                util.send_mess(send_client, mess)
                if mess.type == 'data_sender':
                    logger.debug(f"Sent data block {mess.id} to server {send_id}")

            except Exception as e:
                logger.info(f"Error sending message to {send_id}: {e}")
                if sor:
                    send_client.close()
                    client_thread = threading.Thread(target=self.connect_to_other, args=(self.server_host_list[send_id], self.server_port_list[send_id], send_id), name="connect_thread")
                    client_thread.start()
                    client_thread.join()
                    send_client = self.send_clients[send_id]
                continue
            
            # recive response 
            try:
                mess_send_response, error_str = util.recive_mess(send_client, timeout)
                # print(mess_send_response)
                if not mess_send_response:
                    raise TimeoutError(error_str)
                if mess_send_response.type == 'data_sender_response':
                    self.Data_Sender_Response_Handle(mess_send_response)
                elif mess_send_response.type == 'data_request_response':
                    self.Data_Request_Response_Handle(mess_send_response)
                elif mess_send_response.type == 'heartbeat_response':
                    self.Heartbeat_Response_Handle(mess_send_response)
                elif mess_send_response.type == 'vote_response':
                    self.Vote_Response_Handle(mess_send_response)
                break
            except TimeoutError as Te:
                logger.info(f"Response timed out. Retry... {Te}")
                break
                # Implement timeout retry logic, todo...
    
    def Send(self, send_id, sor, mess):
        if sor: # 1 is send_client
            send_client = self.send_clients[send_id]
        else: # 0 is receive_client
            send_client = self.receive_clients[send_id]
        
        try:
            util.send_mess(send_client, mess)
        except Exception as e:
            logger.info(f"Error sending message to {id}: {e}")
    
    def Recive(self, recive_client, timeout=None):
        return util.recive_mess(recive_client, timeout)

    def Broadcast_Heartbeat(self):
        # can be used for a coordinator to broadcast its heartbeat
        mess = message.Message_Heartbeat(self.id, self.term, self.max_id, self.server_host, self.server_port)
        # broadcast the heartbeat
        threads = []   
        with self.sublock:
            # logger.info("broadcast heartbeat lock get")   
            for id, send_client in self.send_clients.items():
                    thread = threading.Thread(target=self.Send_and_Handle_Response, args=(id, 1, mess), name=f"send_heartbeat_to_{id}") # send_client
                    thread.start()
                    threads.append(thread)
        
        for thread in threads:
            thread.join()

                    # may be need reconnect
        # logger.info("broadcast heartbeat lock release")
        


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    args = construct_hyper_param(parser)

    config = util.load_config("./config.json")
    
    local_ip, local_port, local_id = config["server_host_private"][args.server_id - 1], config["server_port"][args.server_id - 1], args.server_id

    server = Node(local_ip, local_port, local_id)
    server.server_host_list = server.server_host_list + config["server_host_private"]
    server.server_port_list = server.server_port_list + config["server_port"]
    server.num = len(config["server_host_public"])

    server.sign_in()