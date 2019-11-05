import socket

# MESSAGE TYPES

HANDSHAKE = 1
HANDSHAKE_OK = 2
HANDSHAKE_FAIL = 3
UNKNOWN_CLIENT = 4
HEARTBEAT = 5
SHUTDOWN = 6
CLIENT_SHUTDOWN = 7

ADD_SERIES = 8
REMOVE_SERIES = 9
LISTENER_ADD_SERIES = 10
LISTENER_REMOVE_SERIES = 11
LISTENER_NEW_SERIES_POINTS = 12
UPDATE_SERIES = 13

REPONSE_OK = 14

WORKER_JOB = 15
WORKER_JOB_RESULT = 16
WORKER_JOB_CANCEL = 17
WORKER_JOB_CANCELLED = 18
WORKER_UPDATE_BUSY = 19
WORKER_REFUSED = 20


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
