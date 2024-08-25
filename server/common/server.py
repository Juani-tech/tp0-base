import socket
import logging
import signal
import time


class Server:

    def __init__(self, port, listen_backlog):
        # Initialize server socket
        self._server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._server_socket.bind(("", port))
        self._server_socket.listen(listen_backlog)
        self._sigterm_received = False

    def _sigterm_handler(self, signum, frames):
        self._sigterm_received = True
        self._server_socket.shutdown(socket.SHUT_RDWR)  # No further writes/read allowed

    def run(self):
        """
        Dummy Server loop

        Server that accept a new connections and establishes a
        communication with a client. After client with communucation
        finishes, servers starts to accept new connections again
        """

        # TODO: Modify this program to handle signal to graceful shutdown
        # the server
        signal.signal(signal.SIGTERM, self._sigterm_handler)

        while not self._sigterm_received:
            try:
                client_sock = self.__accept_new_connection()
                self.__handle_client_connection(client_sock)
            except OSError as e:
                logging.error(
                    "action: accept_new_connections | result: fail | error: {e}"
                )

    def __handle_client_connection(self, client_sock):
        """
        Read message from a specific client socket and close the socket.

        If a problem arises in the communication with the client, the
        client socket will also be closed.
        """

        def recv_all(sock, buffer_size):
            """Receive all data from the socket, handling short-reads."""
            data = b""
            while True:
                part = sock.recv(buffer_size)
                if len(part) == 0:
                    # The other side closed the connection
                    raise OSError
                data += part
                if len(part) < buffer_size:
                    # End of the message
                    break
            return data

        def send_all(sock, data):
            """Send all data through the socket, handling short-writes."""
            total_sent = 0
            while total_sent < len(data):
                sent = sock.send(data[total_sent:])
                if sent == 0:
                    raise OSError
                total_sent += sent

        try:
            msg = recv_all(client_sock, 1024).rstrip().decode("utf-8")
            addr = client_sock.getpeername()
            logging.info(
                f"action: receive_message | result: success | ip: {addr[0]} | msg: {msg}"
            )
            send_all(client_sock, "{}\n".format(msg).encode("utf-8"))
        except OSError as e:
            logging.error(f"action: receive_message | result: fail | error: {e}")
        finally:
            client_sock.close()

    def __accept_new_connection(self):
        """
        Accept new connections

        Function blocks until a connection to a client is made.
        Then connection created is printed and returned
        """

        # Connection arrived
        logging.info("action: accept_connections | result: in_progress")
        c, addr = self._server_socket.accept()
        logging.info(f"action: accept_connections | result: success | ip: {addr[0]}")
        return c
