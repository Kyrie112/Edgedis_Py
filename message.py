# this is the code file for different types of message and their response
class Message_Heartbeat:
    def __init__(self, id, term, max_id, from_host, from_port):
        self.type = 'heartbeat'
        self.id = id
        self.term = term
        self.max_id = max_id
        self.from_host = from_host
        self.from_port = from_port


class Message_Heartbeat_Response:
    def __init__(self, id, miss_data_block, max_id, term, from_host, from_port):
        self.type = 'heartbeat_response'
        self.id = id
        self.miss_data_block = miss_data_block
        self.max_id = max_id
        self.term = term
        self.from_host = from_host
        self.from_port = from_port


class Message_Vote:
    def __init__(self, id, term, max_id, candidate_id, total_block, from_host, from_port):
        self.id = id
        self.type = 'vote'
        self.term = term
        self.max_id = max_id
        self.candidate_id = candidate_id
        self.tot_block = total_block
        self.from_host = from_host
        self.from_port = from_port


class Message_Vote_Response:
    def __init__(self, id, support, term, from_host, from_port):
        self.type = 'vote_response'
        self.id = id
        self.support = support
        self.term = term
        self.from_host = from_host
        self.from_port = from_port


class Message_Data_Sender:  # how to use this class to help sender to send out data
    def __init__(self, id, data, data_id, from_host, from_port):
        self.type = 'data_sender'
        self.id = id
        self.data = data
        self.data_id = data_id
        self.from_host = from_host
        self.from_port = from_port


class Message_Data_Sender_Response:
    def __init__(self, id, data_id, status, from_host, from_port):
        self.type = 'data_sender_response'
        self.id = id
        self.data_id = data_id
        self.status = status
        self.from_host = from_host
        self.from_port = from_port


class Message_Data_Client:
    def __init__(self, data, data_id, from_host, from_port):
        self.type = 'data_client'
        self.data = data
        self.data_id = data_id
        self.from_host = from_host
        self.from_port = from_port


class Message_Data_Client_Response:
    def __init__(self, id, data_id, status, from_host, from_port):
        self.type = 'data_client_response'
        self.id = id
        self.data_id = data_id
        self.status = status
        self.from_host = from_host
        self.from_port = from_port


class Message_Sign_In:  # this message is used for server connect
    def __init__(self, server_host, server_port, server_id):
        self.type = 'sign_in'
        self.server_host = server_host
        self.server_port = server_port
        self.server_id = server_id


# class Message_Sign_In_Response:  # this message is used to response server connect
#     def __init__(self, server_host_list, server_port_list):
#         self.type = 'sign_in_response'
#         self.server_host_list = server_host_list
#         self.server_port_list = server_port_list


class Message_Data_Request:  # used for coordinator to request data
    def __init__(self, from_host, from_port, request_list):
        self.type = 'data_request'
        self.from_host = from_host
        self.from_port = from_port
        self.request_list = request_list


class Message_Data_Request_Response:    # used for follower to supply the data
    def __init__(self, from_host, from_port, request_list, request_data):
        self.type = 'data_request_response'
        self.from_host = from_host
        self.from_port = from_port
        self.request_list = request_list
        self.request_data = request_data


class Message_Data_Supplement:  # coordinator sends this message to supply follower
    def __init__(self, data_block, data_block_id):  # don't need to response
        self.type = 'data_supplement'
        self.data_block = data_block
        self.data_block_id = data_block_id

