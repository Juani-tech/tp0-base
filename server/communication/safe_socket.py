class SafeSocket:
    def __init__(self, sock, length_bytes):
        self._sock = sock
        self._length_bytes = length_bytes

    def __receive_length(self):
        """
        Raises:
            OSError: if the amount of read bytes is 0 (zero), which indicates that the socket is closed

        Returns:
            int: the length decoded (utf-8) as int
        """
        received = b""
        while len(received) < 6:
            received += self._sock.recv(self._length_bytes)
            if len(received) == 0:
                raise OSError

        return int(received.decode("utf-8"))

    def __receive_message(self, message_len):
        """_summary_

        Args:
            message_len (int): length of the message in bytes

        Raises:
            OSError: if the amount of read bytes is 0 (zero), which indicates that the socket is closed

        Returns:
            the raw message (bytes)
        """
        received = b""
        while len(received) < message_len:
            received += self._sock.recv(message_len)

            if len(received) == 0:
                raise OSError

        return received

    def recv_all_with_length_bytes(self):
        """Receive all data from the socket, handling short-reads."""
        length = self.__receive_length()
        message = self.__receive_message(length)

        return message

    def __format_length(self, length):
        s = str(length)
        while len(s) < self._length_bytes:
            s = "0" + s
        return s

    def send_all(self, data):
        """Send all data through the socket, handling short-writes."""
        total_sent = 0
        protocol_message = (
            "{}".format(self.__format_length(len(data))).encode("utf-8") + data
        )

        while total_sent < len(protocol_message):
            sent = self._sock.send(protocol_message[total_sent:])
            if sent == 0:
                raise OSError
            total_sent += sent

    def close(self):
        self._sock.close()

    def getpeername(self):
        return self._sock.getpeername()
