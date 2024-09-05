class SafeSocket: 
    def __init__(self, sock, length_bytes): 
        self._sock = sock
        self._length_bytes = length_bytes

    def recv_all_with_length_bytes(self):
        """Receive all data from the socket, handling short-reads."""
        data = b""
        while True:
            length = self._sock.recv(self._length_bytes).decode('utf-8')
            
            if len(length) == 0:
                # The other side closed the connection
                raise OSError
            
            part = self._sock.recv(int(length)) 
            
            if len(part) == 0:
                # The other side closed the connection
                raise OSError
            

            data += part

            if b"\n" in part:
                # End of the message
                break
        return data

    def __format_length(self, length):
        s = str(length)
        while len(s) < self._length_bytes: 
            s = "0" + s
        return s


    def send_all(self, data):
        """Send all data through the socket, handling short-writes."""
        total_sent = 0
        protocol_message = "{}".format(self.__format_length(len(data))).encode("utf-8") + data

        while total_sent < len(protocol_message):
            sent = self._sock.send(protocol_message[total_sent:])
            if sent == 0:
                raise OSError
            total_sent += sent

    def close(self):
        self._sock.close()

    def getpeername(self):
        return self._sock.getpeername()