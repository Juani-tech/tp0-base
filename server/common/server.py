import socket
import logging
import signal

from common.utils import Bet, store_bets, winners_for_agency
from common.utils import Bet, store_bets
from communication.safe_socket import SafeSocket
from communication.protocol import Protocol


class Server:

    def __init__(self, port, listen_backlog, total_agencies, length_bytes):
        # Initialize server socket
        self._server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._server_socket.bind(("", port))
        self._server_socket.listen(listen_backlog)
        self._agencies = dict()
        self._total_agencies = total_agencies
        self._protocol = Protocol(total_agencies)
        self._length_bytes = length_bytes

    # Sets sigtemr_received flag and executes shutdown on the socket
    # which causes the server to "tell" to the connected parts that it's closing them
    def __sigterm_handler(self, signum, frames):
        self._server_socket.shutdown(socket.SHUT_RDWR)
        raise SystemExit

    def run(self):
        """
        Dummy Server loop

        Server that accept a new connections and establishes a
        communication with a client. After client with communucation
        finishes, servers starts to accept new connections again
        """

        # Add the signal handler for SIGTERM
        signal.signal(signal.SIGTERM, self.__sigterm_handler)

        while True:
            try:
                client_sock = SafeSocket(self.__accept_new_connection(), self._length_bytes)
                self.__handle_client_connection(client_sock)

            except (OSError, SystemExit):
                logging.debug("action: accept_new_connections | result: finished ")
                return

 

    def __handle_client_connection(self, client_sock):
        """
        Read message from a specific client socket and close the socket.

        If a problem arises in the communication with the client, the
        client socket will also be closed.
        """
        try:
            while True: 
                msg = client_sock.recv_all_with_length_bytes().rstrip().rstrip(b"\n").decode('utf-8')
                addr = client_sock.getpeername()

                logging.info(
                    f"action: receive_message | result: success | ip: {addr[0]} | msg: {msg}"
                )

                message_type, body = msg.split(",", 1)

                # match case supported in 3.10 python version =(
                if message_type == "BATCH":
                    self._protocol.process_batch(body, client_sock)
                elif message_type == "FIN":
                    self._protocol.process_fin(body)
                elif message_type == "GANADORES":
                    self._protocol.process_who_won(body, client_sock)
                    break
                else:
                    raise RuntimeError(f"Message type not recognized: {message_type}")

                # self.__process_message(msg, client_sock)
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
