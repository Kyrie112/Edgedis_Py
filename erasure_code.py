import zfec


def encode_data(original_data, data_block_cnt, tot_cnt):
    # create a encoder
    encoder = zfec.easyfec.Encoder(data_block_cnt, tot_cnt)
    encoded_blocks = encoder.encode(original_data)
    if len(original_data) % k != 0:
        padlen = (len(original_data) // k) - len(original_data) % k
    else:   padlen = 0
    return encoded_blocks, padlen


def decode_data(encoded_blocks, encoded_blocks_ind, data_block_cnt, tot_cnt, padlen):
    # create a decoder
    decoder = zfec.easyfec.Decoder(data_block_cnt, tot_cnt)
    decoded_data = decoder.decode(encoded_blocks, encoded_blocks_ind, padlen)  # here needs block_num and padlen(
    # padlen is used for fulfill the
    # string so that it can be divided by k)
    return decoded_data


# examples
original_data = b'abcde'
k = 2
m = 3
# k is the count of the original data, m is the final count of the dat blocks

# encode
encoded_blocks, padlen = encode_data(original_data, k, m)
print(encoded_blocks)
# some blocks missing or corrupt
blocks_ind = [0, 1]
blocks = (encoded_blocks[0], encoded_blocks[1])

# decode
decoded_data = decode_data(blocks, blocks_ind, k, m, padlen)

# print
print("Original Data:", original_data)
print("Encoded Blocks:", encoded_blocks)
print("Decoded Data:", decoded_data.decode())
