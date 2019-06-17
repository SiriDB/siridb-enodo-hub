import socket

# MESSAGE TYPES

HANDSHAKE = 1
UNKNOW_CLIENT = 2
SHUTDOWN = 3
ADD_SERIE = 4
REMOVE_SERIE = 5
HANDSHAKE_OK = 6
HANDSHAKE_FAIL = 7
HEARTBEAT = 8
LISTENER_ADD_SERIE = 9
LISTENER_REMOVE_SERIE = 10
LISTENER_ADD_SERIE_COUNT = 11
REPONSE_OK = 12
UPDATE_SERIES = 13
TRAIN_MODEL = 14
FIT_MODEL = 15
FORECAST_SERIE = 16

'''
Header:
size,     int,    32bit
type,     int     8bit
packetid, int     8bit

total header length = 48 bits
'''

PACKET_HEADER_LEN = 48


async def read_packet(sock, header_data=None):
    if isinstance(sock, socket.socket):
        if header_data is None:
            header_data = sock.recv(PACKET_HEADER_LEN)
        body_size, packet_type, packet_id = read_header(header_data)
        return packet_type, packet_id, sock.recv(body_size)
    else:
        if header_data is None:
            header_data = await sock.read(PACKET_HEADER_LEN)
        body_size, packet_type, packet_id = read_header(header_data)
        return packet_type, packet_id, await sock.read(body_size)


def create_header(size, type, id):
    return size.to_bytes(32, byteorder='big') + type.to_bytes(8, byteorder='big') + id.to_bytes(8, byteorder='big')


def read_header(binary_data):
    return int.from_bytes(binary_data[:32], 'big'), int.from_bytes(binary_data[32:40], 'big'), int.from_bytes(
        binary_data[40:48], 'big')
