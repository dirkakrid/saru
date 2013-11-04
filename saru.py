# saru: simple and reliable udp
#
# how it create and send packets?
#
# +------------------+----------------+------------------+-----------------------+--------------------+
# | length (2 bytes) | flags (1 byte) | counter (1 byte) |data (variable length) | checksum (4 bytes) |
# +------------------+----------------+------------------+-----------------------+--------------------+
#
# - length: size of whole packet: 2 (length) + 1 (flags) + data length + 4 (checksum)
#
# - flags: 76543210
#   0-1: type (type of this packet)
#       0 (00): data
#       1 (01): success ack
#       2 (10): failure ack
#       3 (11): reset ack
#   2-3: position (position of this packet among this chain)
#       0 (00): middle
#       1 (01): first
#       2 (10): last
#       3 (11): first and last
#
# - counter: counter of packets in the stream
#
# - data: zero to max_packet_size bytes of data. size of data is zero for non data packets.
#
# - checksum: adler32 checksum of full packet except checksum part (length, flags, data)
#
#
# is it two side connection?
# is it thread safe?
# is it connection less as udp itself?
#


import zlib
import time
import struct
import socket as _socket
import random


full_header_size=2+1+1+4
max_packet_size=8*1024
max_data_size=max_packet_size-full_header_size
flag_mask_type=3
flag_mask_position=3<<2
flag_type_data=0
flag_type_success_ack=1
flag_type_failure_ack=2
flag_type_reset_ack=3
flag_position_middle=0<<2
flag_position_first=1<<2
flag_position_last=2<<2
flag_position_firstlast=3<<2
socket_timeout=1.000
max_attempts=10


class socket(object):

    def __init__(self):
        self.sock=_socket.socket(_socket.AF_INET, _socket.SOCK_DGRAM)
        self.success_ack=self._pack('', flag_position_firstlast, 0, flag_type_success_ack)
        self.failure_ack=self._pack('', flag_position_firstlast, 0, flag_type_failure_ack)
        self.reset_ack=self._pack('', flag_position_firstlast, 0, flag_type_reset_ack)


    def sendto(self, fulldata, address):
        self.sock.settimeout(socket_timeout)
        i=0
        counter=0
        attempts=0
        while i<len(fulldata):
            data=fulldata[i:i+max_data_size]
            if i==0:
                if len(data)==len(fulldata):
                    pos=flag_position_firstlast
                else:
                    pos=flag_position_first
            else:
                if i+len(data)>=len(fulldata):
                    pos=flag_position_last
                else:
                    pos=flag_position_middle
            packet=self._pack(data, counter, pos)
            if True:# random.random()<0.50:
                self.sock.sendto(packet, address)
            try:
                received, addr=self.sock.recvfrom(max_packet_size)
            except _socket.timeout, err:
                attempts+=1
                if attempts>max_attempts:
                    raise _socket.timeout()
                continue
            flags=self._unpack(received)[1]
            if flags&flag_mask_type==flag_type_success_ack:
                pass # print 'success: %d to %d'%(i, i+len(data))
            elif flags&flag_mask_type==flag_type_reset_ack:
                pass # print 'may be success!: %d to %d'%(i, i+len(data))
            else:
                # print 'failure: %d to %d'%(i, i+len(data))
                continue
            i+=len(data)
            counter=(counter+1)%256
            attempts=0


    def recvfrom(self):
        self.sock.settimeout(None)
        st_look_for_first=0
        st_look_for_next=1
        state=st_look_for_first
        finished=False
        full_data=[]
        expected_counter=0
        while not finished:
            packet, addr=self.sock.recvfrom(max_packet_size)
            data, flags, counter, checksum_is_correct=self._unpack(packet)
            # print 'packet:', [data, flags, checksum_is_correct]
            ack=None
            if not checksum_is_correct:
                ack=self.failure_ack
                # print 'failure: checksum'
            elif flags&flag_mask_type!=flag_type_data:
                ack=self.failure_ack
                # print 'failure: not data!'
            elif state==st_look_for_first and (flags&flag_mask_position not in (flag_position_first, flag_position_firstlast)):
                ack=self.reset_ack
                # print 'failure: not first!', flags, flags&flag_mask_position, (flag_position_first, flag_position_firstlast)
            elif counter>expected_counter:
                assert 0, (counter, expected_counter)
            elif False:#random.random()<0.10:
                ack=self.failure_ack
            if ack==None:
                ack=self.success_ack
                if counter==expected_counter:
                    full_data.append(data)
                    expected_counter=(expected_counter+1)%256
                state=st_look_for_next
                if flags&flag_mask_position in (flag_position_last, flag_position_firstlast):
                    finished=True
            if True:#random.random()<0.60:
                self.sock.sendto(ack, addr)
            else:
                pass # print 'not sent'
        return ''.join(full_data)


    def bind(self, address):
        self.sock.bind(address)


    def _pack(self, data, counter, position, type=flag_type_data):
        if len(data)>max_data_size:
            raise ValueError('data size is more than max_data_size')
        length=struct.pack('!H', len(data)+full_header_size)
        flags=struct.pack('B', position | type)
        cntr=struct.pack('B', counter)
        packet=length+flags+cntr+data
        checksum=struct.pack('!L', zlib.adler32(packet) & 0xffffffff)
        return packet+checksum


    def _unpack(self, packet):
        if len(packet)<full_header_size:
            raise ValueError('packet size is less than header size')
        length=struct.unpack('!H', packet[0:2])[0]
        if length>max_packet_size:
            raise ValueError('packet size is more than max_packet_size')
        flags=struct.unpack('B', packet[2])[0]
        counter=struct.unpack('B', packet[3])[0]
        data=packet[4:length-4]
        received_checksum=struct.unpack('!L', packet[length-4:length])[0]
        checksum=zlib.adler32(packet[0:length-4]) & 0xffffffff
        return (data, flags, counter, checksum==received_checksum)


def test():
    import sys
    random.seed(100)
    data=''.join([chr(random.randint(65, 89)) for i in range(10*1024)])
    send_to_addr=('127.0.0.1', 8111)
    bind_to_addr=('127.0.0.1', 8111)
    random.seed()
    if len(sys.argv)!=2 or sys.argv[1] not in ('send', 'recv'):
        print 'usage: %s recv|send'%sys.argv[0]
        return
    if sys.argv[1]=='send':
        sock=socket()
        sock.sendto(data, send_to_addr)
    elif sys.argv[1]=='recv':
        sock=socket()
        sock.bind(bind_to_addr)
        res=sock.recvfrom()
        print 'received %d bytes. [%s]'%(len(res), 'OK' if res==data else 'FAIL')


if __name__=='__main__':
    test()
