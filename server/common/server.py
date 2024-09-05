import socket
import logging
import signal
import threading
from common.utils import Bet, store_bets, winners_for_agency
from communication.safe_socket import SafeSocket
from communication.protocol import Protocol

class Server:

    def __init__(self, port, listen_backlog, total_agencies, length_bytes):
        # Initialize server socket
        self._server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._server_socket.bind(("", port))
        self._server_socket.listen(listen_backlog)
        self._got_sigterm = threading.Event()
        self._sigterm_cv = threading.Condition()
        self._protocol = Protocol(self._got_sigterm, self._sigterm_cv, total_agencies)
        self._length_bytes = length_bytes  
        self._alive_threads = set()
        self._alive_threads_lock = threading.Lock()


    # Sets sigtemr_received flag and executes shutdown on the socket
    # which causes the server to "tell" to the connected parts that it's closing them
    def __sigterm_handler(self, signum, frames):
        self._server_socket.shutdown(socket.SHUT_RDWR)
        self._got_sigterm.set()
        with self._sigterm_cv:
            self._sigterm_cv.notify_all()
    
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
                client_sock = self.__accept_new_connection()
                sock = SafeSocket(client_sock, self._length_bytes, self._got_sigterm)
                thread = threading.Thread(target=self.__handle_client_connection, args=(sock,))
                with self._alive_threads_lock: 
                    self._alive_threads.add(thread)
                thread.start()
            except (OSError, SystemExit):
                logging.debug("action: accept_new_connections | result: finished ")
                break
            # finally: 
        with self._alive_threads_lock: 
            for thread in self._alive_threads: 
                thread.join()



    """
    Demultiplexes messages received and calls the functions that process them
    Raises a RuntimeError in case the message is not recognized
    """


    def __handle_client_connection(self, client_sock):
        """
        Read message from a specific client socket and close the socket.

        If a problem arises in the communication with the client, the
        client socket will also be closed.
        """

        try:
            while True: 
                msg = (
                    client_sock.recv_all()
                    .rstrip(b"\n")
                    .rstrip()
                    .decode("utf-8")
                )
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
