def print_mess(mess):
    attribute_dict = vars(mess)

    # 打印属性字典的键值对
    for key, value in attribute_dict.items():
        print(f"{key}: {value}")