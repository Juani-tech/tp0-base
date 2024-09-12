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

    def __receive_length(self):
        """
        Raises:
            OSError: if the amount of read bytes is 0 (zero), which indicates that the socket is closed

        Returns:
            int: the length decoded (utf-8) as int
        """
        received = b""
        while len(received) < 6:
            if self._got_sigterm.is_set():
                raise SystemExit

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
            if self._got_sigterm.is_set():
                raise SystemExit

            received += self._sock.recv(message_len)

            if len(received) == 0:
                raise OSError

        return received

    def recv_all_with_length_bytes(self):
        """
        Receives all data from the socket, handling short-reads and socket disconnection.

        Returns:
        - The complete message received from the socket.

        Raises:
        - OSError: If the connection is closed unexpectedly.
        - SystemExit: If a SIGTERM signal is detected.
        """
        length = self.__receive_length()
        message = self.__receive_message(length)

        return message

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
        protocol_message = (
            "{}".format(self.__format_length(len(data))).encode("utf-8") + data
        )

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
