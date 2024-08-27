import socket
import logging
import signal
import time

from common.utils import Bet, parse_csv_kv, store_bets


class Server:

    def __init__(self, port, listen_backlog):
        # Initialize server socket
        self._server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._server_socket.bind(("", port))
        self._server_socket.listen(listen_backlog)
        self._sigterm_received = False

    # Sets sigtemr_received flag and executes shutdown on the socket
    # which causes the server to "tell" to the connected parts that it's closing them
    def __sigterm_handler(self, signum, frames):
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

        # Add the signal handler for SIGTERM
        signal.signal(signal.SIGTERM, self.__sigterm_handler)

        while not self._sigterm_received:
            try:
                client_sock = self.__accept_new_connection()
                self.__handle_client_connection(client_sock)
            except OSError:
                logging.error(f"action: accept_new_connections | result: terminated")
                return

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
                elif b"\n" in part:
                    data += part
                    # End of the message
                    break
            return data

        # # sendall allowed (?)
        # def send_all(sock, data):
        #     """Send all data through the socket, handling short-writes."""
        #     total_sent = 0
        #     while total_sent < len(data):
        #         sent = sock.send(data[total_sent:])
        #         if sent == 0:
        #             raise OSError
        #         total_sent += sent

        try:
            msg = recv_all(client_sock, 1024).rstrip(b"\n").rstrip().decode("utf-8")
            addr = client_sock.getpeername()

            logging.info(
                f"action: receive_message | result: success | ip: {addr[0]} | msg: {msg}"
            )

            # parsed_bet_data = parse_csv_kv(msg)
            # # I guess it has to be a list for batching (ej6)
            # store_bets(
            #     [
            #         Bet(
            #             agency=parsed_bet_data["AGENCIA"],
            #             first_name=parsed_bet_data["NOMBRE"],
            #             last_name=parsed_bet_data["APELLIDO"],
            #             document=parsed_bet_data["DOCUMENTO"],
            #             birthdate=parsed_bet_data["NACIMIENTO"],
            #             number=parsed_bet_data["NUMERO"],
            #         )
            #     ]
            # )
            # Not needed anymore
            # send_all(client_sock, "{}\n".format(msg).encode("utf-8"))
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
