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
            the message decoded (utf-8) as string
        """
        received = b""
        while len(received) < message_len:
            received += self._sock.recv(message_len)

            if len(received) == 0:
                raise OSError

        return received

    def recv_all_with_length_bytes(self):
        """Receive all data from the socket, handling short-reads."""
        # data = b""
        # while True:
        # length = self._sock.recv(self._length_bytes).decode("utf-8")
        length = self.__receive_length()
        message = self.__receive_message(length)
        # if len(length) == 0:
        #     # The other side closed the connection
        #     raise OSError

        # part = self._sock.recv(int(length))

        # if len(part) == 0:
        #     # The other side closed the connection
        #     raise OSError

        # data += part

        # if b"\n" in part:
        # End of the message
        # break
        return message

    def close(self):
        self._sock.close()

    def getpeername(self):
        return self._sock.getpeername()
