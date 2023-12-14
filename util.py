import pickle
import struct
import socket
import time
import json
import logging

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s [%(levelname)s] [%(threadName)s] %(message)s')


def load_config(file_path):
    with open(file_path, 'r') as file:
        config = json.load(file)
    return config

def print_mess(mess):
    attribute_dict = vars(mess)

    # 打印属性字典的键值对
    for key, value in attribute_dict.items():
        print(f"{key}: {value}")

def send_mess(client, mess):
    mess_b = pickle.dumps(mess)
    mess_b_len = len(mess_b)

    len_pre = struct.pack("!I", mess_b_len)

    client.sendall(len_pre)
    client.sendall(mess_b)


def recive_mess(client, timeout=None):
    start_time = time.time()

    len_pre = b""
    while len(len_pre) < 4:
        try:
            if timeout:
                remaining_timeout = timeout - (time.time() - start_time)
                if remaining_timeout < 0:
                    raise TimeoutError("Timeout while receiving message length")
                client.settimeout(remaining_timeout)
            chunk = client.recv(4 - len(len_pre))
            if not chunk:
                # error
                break
            len_pre += chunk
        except:
            logger.info("Timeout while receiving message length")
            client.settimeout(None)
            return None, "Timeout while receiving message length"
    
    if len(len_pre) == 0:
        client.settimeout(None)
        return None, "Connection closed..."
    
    if len(len_pre) < 4:
        client.settimeout(None)
        return None, "len_pre length < 4"
    
    mess_b_len = struct.unpack("!I", len_pre)[0]

    mess_recive_b = b""
    while len(mess_recive_b) < mess_b_len:
        try:
            if timeout:
                remaining_timeout = timeout - (time.time() - start_time)
                if remaining_timeout < 0:
                    raise TimeoutError("Timeout while receiving message content")
                client.settimeout(remaining_timeout)
            chunk = client.recv(min(mess_b_len - len(mess_recive_b), 16*1024))
            if not chunk:
                # error
                break
            mess_recive_b += chunk
        except:
            # print("Timeout while receiving message content")
            client.settimeout(None)
            return None, "Timeout while receiving message content"

    if len(mess_recive_b) < mess_b_len:
        # error
        client.settimeout(None)
        return None, "mess_recive_b length < mess_b_len"
        
    try:
        mess = pickle.loads(mess_recive_b)
        client.settimeout(None)
        return mess, None
    except pickle.UnpicklingError as e:
        # print(f"Errpr unpickling data {e}")
        client.settimeout(None)
        return None, f"Error unpickling data {e}"


