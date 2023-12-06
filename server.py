# this is the code file for node and how they handle different kinds of message
import socket
import message


class Node:
    def __init__(self, server_host, server_port, client_host, client_port, time_out):
        self.server_host = server_host
        self.server_port = server_port
        self.client_host = client_host
        self.client_port = client_port
        self.coordinator_host = server_host  # these three parament need to be updated during the sign_in process and
        # change when a new coordinator is elected
        self.coordinator_port = server_port
        self.coordinator_id = 0
        self.status = 'follower'  # follower, candidate or coordinator
        self.sub_status = 'receiver'  # receiver or sender
        self.time_out = time_out
        self.data_id = -1  # if the node is a sender, it needs to update this
        self.data_to_send = ""  # if the node is a sender, it needs to update this
        self.server_host_list = []
        self.server_port_list = []  # these two lists need to be updated during the sign_in process
        self.data_ind = dict()  # store each data block with an id
        self.max_id = 0  # need to be updated during the sign_in process
        self.term = 0  # need to be updated during the sign_in process
        self.id = 0  # need client to give it an id
        self.vote_for = 0  # the node's id of the node vote for during this term
        self.vote_map = dict()  # store the vote condition
        self.store_map = dict()  # store the store condition(only sender can use it, it needs to be refreshed when senders need to send out a new data block)

    def sign_in(self, client_host, client_port):  # need to be implemented
        # how to join a new server?
        return

    def start_vote(self):  # need to be implemented
        is_time_out = True
        """
        how to check if time reach time_out
        below code is the core code to start broad_cast
        """
        # need to broadcast the vote request
        if is_time_out:
            self.status = 'candidate'  # become candidate
            self.term += 1  # change the term
            self.vote_for = self.id  # vote for itself
            self.vote_map.clear()
            self.vote_map[self.id] = True
            mess = message.Message_Vote(self.term, self.max_id, self.id, len(self.data_ind),
                                        self.server_host, self.server_port)
            for ind in range(len(self.server_host_list)):  # need to broadcast the message
                s = socket.socket()
                s.connect((self.server_host_list[ind], self.server_port_list[ind]))
                s.send(mess)
                s.close()

    def handle_message(self):  # this function need to be run by a thread so that it can be run forever
        s = socket.socket()
        s.bind((self.server_host, self.server_port))
        s.listen(6)
        while True:
            conn, addr = s.accept()
            while True:
                data = conn.recv(2048)  # how to receive struct?
                if data:  # handle different types of message according to role of the node
                    if self.status == 'follower':
                        self.follower_handle(data)
                    elif self.status == 'candidate':
                        self.candidate_handle(data)
                    elif self.status == 'coordinator':
                        self.coordinator_handle(data)
                else:
                    break

    # these three functions is used for different roles
    def follower_handle(self, mess):
        if mess.type == 'data_client':
            self.Data_Client_Handle(mess)
        elif mess.type == 'Heartbeat':
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
        elif mess.type == 'data_client':
            if self.sub_status == 'sender':
                self.Data_Client_Handle(mess)
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
        """
        here needs to refresh the timeout of this follower, but how to do ?
        """
        miss_data_block = []
        from_host = mess.from_host
        from_port = mess.from_port
        pre_max_id = self.max_id
        s = socket.socket()
        s.connect((from_host, from_port))
        # ask for the missing data
        for ind in range(pre_max_id + 1, self.max_id, 1):
            miss_data_block.append(ind)
        # check the term of the coordinator
        if self.term < mess.term:
            self.term = mess.term
            self.coordinator_id = mess.id
        # create a response and send to the coordinator
        mess = message.Message_Heartbeat_Response(miss_data_block, self.max_id,
                                                  self.term, self.server_host, self.server_port)
        s.send(mess)
        s.close()

    def Heartbeat_Response_Handle(self, mess):
        s = socket.socket()
        s.connect((mess.from_host, mess.from_port))
        # supply the follower with the data it needs
        data_block = []
        data_block_id = mess.miss_data_block
        for ind in mess.miss_data_block:
            data_block.append(self.data_ind[ind])
        if len(data_block) != 0:
            mess1 = message.Message_Data_Supplement(data_block, data_block_id)
            s.send(mess1)
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
            s.send(mess2)
        s.close()
        if mess.term > self.term:
            self.term = mess.term
            self.status = 'follower'

    def Vote_Handle(self, mess):
        s = socket.socket()
        s.connect((mess.from_host, mess.from_port))
        # check whether it needs to support the candidate
        if mess.total_block >= len(self.data_ind) and (
                (mess.term == self.term and self.term == -1) or mess.term > self.term):
            self.term = mess.term  # change the term
            mess = message.Message_Vote_Response(self.id, True, self.term, self.server_host, self.server_port)
        else:
            mess = message.Message_Vote_Response(self.id, True, self.term, self.server_host, self.server_port)
        s.send(mess)
        s.close()

    def Vote_Response_Handle(self, mess):
        n = len(self.server_host_list)  # the count of the servers
        if mess.support:
            self.vote_map[mess.id] = True
        # accept
        if len(self.vote_map) > (n + 1) / 2:
            self.status = 'coordinator'
            self.coordinator_id = self.id
            self.Broadcast_Heartbeat()
        # the node falls behind, it needs to become follower
        if mess.term > self.term:
            self.status = 'follower'

    # below three functions don't have pseudocode
    def Data_Sender_Handle(self, mess):
        self.data_ind[mess.data_id] = mess.data
        self.max_id = max(self.id, mess.data_id)
        s = socket.socket()
        s.connect((mess.from_host, mess.from_port))
        mess = message.Message_Data_Sender_Response(self.id, True, self.server_host, self.server_port)
        s.send(mess)
        s.close()

    def Data_Sender_Response_Handle(self, mess):
        n = len(self.server_host_list)
        if mess.status:  # received successfully
            self.store_map[mess.id] = True
        if len(self.store_map) > (n + 1) / 2:  # need to tell the cloud that the data block has been transmitted
            # successfully
            self.store_map.clear()
            self.sub_status = 'receiver'  # need to step back to receiver
            s = socket.socket()
            s.connect((self.client_host, self.client_port))
            # the cloud needs to handle this message and transmit new data block
            mess = message.Message_Data_Client_Response(self.id, self.data_id, True, self.server_host, self.server_port)
            s.send(mess)
            s.close()

    def Data_Client_Handle(self, mess):
        # the server don't need to handle the response of this message as the client will handle its response
        # init some parament and store the data
        self.status = 'sender'
        self.data_id = mess.data_id
        self.data_to_send = mess.data
        self.data_ind[mess.data_id] = mess.data
        self.store_map[self.id] = True
        # broadcast the data
        mess = message.Message_Data_Sender(self.data_to_send, self.data_id, self.server_host, self.server_port)
        for ind in range(len(self.server_host_list)):
            s = socket.socket()
            s.connect((self.server_host_list[ind], self.server_port_list[ind]))
            s.send(mess)
            s.close()

    def Data_Request_Handle(self, mess):
        # collect the missing data of coordinator
        s = socket.socket()
        s.connect((mess.from_host, mess.from_port))
        request_list = mess.request_list
        request_data = []
        for ind in request_list:
            request_data.append(self.data_ind[ind])
        mess = message.Message_Data_Request_Response(self.server_host, self.server_port, request_list,
                                                     request_data)
        s.send(mess)
        s.close()

    def Data_Request_Response_Handle(self, mess):
        # store the new data blocks that are processed by follower
        for ind in range(len(mess.request_list)):
            self.data_ind[mess.request_list[ind]] = mess.request_data[ind]

    def Data_Supplement_Handle(self, mess):
        # store the data blocks that are processed by coordinator
        for ind in range(len(mess.data_block_id)):
            self.data_ind[mess.data_block_id[ind]] = mess.data_block[ind]

    def Broadcast_Heartbeat(self):
        # can be used for a coordinator to broadcast its heartbeat
        mess = message.Message_Heartbeat(self.id, self.term, self.max_id, self.server_host, self.server_port)
        for ind in range(len(self.server_host_list)):  # need to broadcast the message
            s = socket.socket()
            s.connect((self.server_host_list[ind], self.server_port_list[ind]))
            s.send(mess)
            s.close()
