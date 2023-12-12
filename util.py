import pickle
import struct
import socket

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


def recive_mess(client):
    len_pre = b""
    while len(len_pre) < 4:
        chunk = client.recv(4 - len(len_pre))
        if not chunk:
            # error
            break
        len_pre += chunk
    
    if len(len_pre) < 4:
        return None
    
    mess_b_len = struct.unpack("!I", len_pre)[0]

    mess_recive_b = b""
    while len(mess_recive_b) < mess_b_len:
        chunk = client.recv(min(mess_b_len - len(mess_recive_b), 16*1024))
        if not chunk:
            # error
            break
        print(len(chunk))
        mess_recive_b += chunk
    
    if len(mess_recive_b) < mess_b_len:
        # error
        return None
    
    try:
        mess = pickle.loads(mess_recive_b)
        return mess
    except pickle.UnpicklingError as e:
        print(f"Errpr unpickling data {e}")
        return None


