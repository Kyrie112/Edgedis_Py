# this is the code file for the cloud
import threading
import random
import string
import socket
import message
import re


class Thread_send_block(threading.Thread):
    def __init__(self, data_block, host, port, id):
        super().__init__()
        self.data_block = data_block
        self.host = host
        self.port = port
        self.id = id

    def run(self):
        c = socket.socket()  # create a socket
        c.bind((self.host, self.port))
        mess = message.Message_Data_Client(self.data_block, self.id)    # how to send out a data whose type is a struct
        c.send(mess)
        print(self.host, self.port, 'receives data', self.id)  # successfully send out
        c.close()


class Thread_send_data(threading.Thread):
    def __init__(self, data_len, block_cnt, server_host, server_port, block_id):  # the data here needs to be a string
        super(Thread_send_data).__init__()
        self.data_len = data_len  # create a string with the length is data_len
        self.block_cnt = block_cnt  # the count of the data blocks
        self.server_host = server_host  # the list of server hosts which are going to become senders
        self.server_port = server_port  # the list of server ports which are going to become receivers
        self.block_id = block_id

    def run(self):
        n = len(self.server_host)
        chars = string.ascii_lowercase + string.digits + string.ascii_uppercase
        random_data = ''.join(random.sample(chars, self.data_len))  # create a random string as datas
        block_size = self.data_len / self.block_cnt
        data_blocks = re.findall(r'.{block_size}', random_data)
        each_cnt = self.block_cnt / n
        ind_block = 0
        for ind_server in range(n):  # start sending the data blocks
            cnt = 0
            while ind_block < len(data_blocks):
                tsb = Thread_send_block(data_blocks[ind_block], self.server_host[ind_server],
                                        self.server_port[ind_server], self.block_id + ind_block)
                cnt += 1
                ind_block += 1
                tsb.start()
                if cnt == each_cnt:  # enough data blocks for this server
                    break


class Client:
    def __init__(self, client_host, client_port):
        self.block_send_out = 0  # store the count of blocks which have been sent out
        self.server_count = 0  # store the count of servers
        self.server_host = []  # store the host of each server
        self.server_port = []  # store the port of each server
        self.data_dict = dict()  # store the statues of each data transmission
        self.ready = True   # whether the cloud is ready for a new file
        self.c = socket.socket()
        self.c.bind((client_host, client_port))
        self.c.listen(6)  # the parament can be set by yourself

    def send_data(self):
        data_len = input('please input the size of the data')
        block_cnt = input('please input the count of the data block')
        entry_cnt = input('please input the count of the entry server')
        tsd = Thread_send_data(data_len, block_cnt, self.server_host[0:entry_cnt], self.server_port[0:entry_cnt],
                               self.block_send_out + 1)
        tsd.start()
        tsd.join()  # need to wait until each server has received the data
        self.block_send_out += block_cnt
        print('the data has been sent to entry servers(senders)')

    def receive_response(self):  # this function needs to be run forever to listen to the response from each senders
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
    cloud_host = input('please input the host of cloud')
    cloud_port = input('please input the port of cloud')
    Edge_Cloud = Client(cloud_host, cloud_port)  # create an edge server cloud
    trr = threading.Thread(target=Edge_Cloud.receive_response())    # a thread which can be run forever
    trr.setDaemon(True)
    trr.start()
    while True:
        flag = input('need to send data? 1.Yes 0.No')
        if flag == 1:
            Edge_Cloud.send_data()
        else:
            break
    print('End')
