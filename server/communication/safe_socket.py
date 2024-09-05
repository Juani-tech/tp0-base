class SafeSocket:
    def __init__(self, sock, length_bytes, got_sigterm):
        """
        Initializes the SafeSocket instance.

        Parameters:
        - sock: The underlying socket object.
        - length_bytes: The number of bytes used to represent the length of a message.
        - got_sigterm: A threading.Event to check if a SIGTERM signal was received.
        """
        self._sock = sock
        self._length_bytes = length_bytes
        self._got_sigterm = got_sigterm

    def recv_all(self):
        """
        Receives all data from the socket, handling short-reads and socket disconnection.

        Returns:
        - The complete message received from the socket.

        Raises:
        - OSError: If the connection is closed unexpectedly.
        - SystemExit: If a SIGTERM signal is detected.
        """
        data = b""
        while True:
            if self._got_sigterm.is_set():
                raise SystemExit
            
            # Receives the first part (length of the message in bytes) and decodes it
            length = self._sock.recv(6).decode('utf-8')
            
            if len(length) == 0:
                # The other side closed the connection
                raise OSError
            
            # Receives the actual data part based on the length
            part = self._sock.recv(int(length)) 
            
            if len(part) == 0:
                # The other side closed the connection
                raise OSError

            data += part

            if b"\n" in part:
                # End of the message detected
                break
        return data

    def __format_length(self, length):
        """
        Formats the length of the message to ensure it's padded to the specified length in bytes.

        Parameters:
        - length: The length of the message to be sent.

        Returns:
        - A string representation of the message length padded with leading zeroes.
        """
        s = str(length)
        while len(s) < self._length_bytes:
            s = "0" + s
        return s

    def send_all(self, data):
        """
        Sends all data through the socket, handling short-writes and ensuring that the 
        message length is sent first.

        Parameters:
        - data: The message to be sent through the socket.

        Raises:
        - OSError: If the connection is closed unexpectedly.
        - SystemExit: If a SIGTERM signal is detected.
        """
        total_sent = 0
        # Prepend the message with its length
        protocol_message = "{}".format(self.__format_length(len(data))).encode("utf-8") + data

        # Sends the message in chunks until the entire message is sent
        while total_sent < len(protocol_message):
            if self._got_sigterm.is_set():
                raise SystemExit
            
            sent = self._sock.send(protocol_message[total_sent:])
            if sent == 0:
                raise OSError
            total_sent += sent

    def close(self):
        """Closes the underlying socket connection."""
        self._sock.close()

    def getpeername(self):
        """Returns the address of the peer connected on the other side of the socket."""
        return self._sock.getpeername()
