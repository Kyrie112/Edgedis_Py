import pickle
import struct
import socket
import time
import json
import logging

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s [%(levelname)s] [%(threadName)s] %(message)s')
logger = logging.getLogger(__name__)

handler = logging.FileHandler('cloud.log', encoding='UTF-8')
handler.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s [%(levelname)s] [%(threadName)s] %(message)s')
handler.setFormatter(formatter)

logger.addHandler(handler)

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
    combined_message = len_pre + mess_b

    client.sendall(combined_message)


def recive_mess(client, timeout=None):
    client.settimeout(timeout)

    len_pre = b""
    while len(len_pre) < 4:
        try:
            chunk = client.recv(4 - len(len_pre))
            if not chunk:
                # error
                break
            len_pre += chunk
            logger.info("Recived length chunk...")
        except:
            logger.error("Timeout while receiving message length")
            client.settimeout(None)
            return None, "Timeout while receiving message length"
    
    if len(len_pre) == 0:
        client.settimeout(None)
        return None, "Connection closed..."
    
    if len(len_pre) < 4:
        client.settimeout(None)
        return None, "len_pre length < 4"
    
    mess_b_len = struct.unpack("!I", len_pre)[0]
    
    logger.info(f"mess length: {mess_b_len}")
    mess_recive_b = b""
    while len(mess_recive_b) < mess_b_len:
        try:
            chunk = client.recv(min(mess_b_len - len(mess_recive_b), 100*1024*1024))
            if not chunk:
                # error
                break
            mess_recive_b += chunk
            chunk_len = len(chunk)
            now_mess_len = len(mess_recive_b)
            logger.info(f"Recived message chunk, chunk length: {chunk_len}, now mess length: {now_mess_len}...")
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


