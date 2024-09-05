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

    def close(self):
        self._sock.close()

    def getpeername(self):
        return self._sock.getpeername()