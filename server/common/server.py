import socket
import logging
import signal

from common.utils import Bet, parse_batch_data, store_bets


class Server:

    def __init__(self, port, listen_backlog):
        # Initialize server socket
        self._server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._server_socket.bind(("", port))
        self._server_socket.listen(listen_backlog)
        self._sigterm_received = False
        self._agencies = dict()

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
                return

    def __recv_all(self, sock, buffer_size):
        """Receive all data from the socket, handling short-reads."""
        data = b""
        while True:
            part = sock.recv(buffer_size)
            if len(part) == 0:
                # The other side closed the connection
                raise OSError

            data += part

            if b"\n" in part:
                # End of the message
                break
        return data

    # sendall allowed (?)
    def __send_all(self, sock, data):
        """Send all data through the socket, handling short-writes."""
        total_sent = 0
        while total_sent < len(data):
            sent = sock.send(data[total_sent:])
            if sent == 0:
                raise OSError
            total_sent += sent

    # Returns true if the agency has sent a FIN message
    # Returns false if either the agency isn't in the agencies dict (adds it and
    # then returns), or the agency is in the dict but hasn't sent FIN yet.
    def __agency_sent_fin(self, message):
        agency, _ = message.split(",", 1)
        if agency in self._agencies.keys():
            if self._agencies[agency]:
                return True
            return False
        else:
            _, agency_number = agency.split("=")
            self._agencies[agency_number] = False
            return False

    def __process_batch(self, batch_message, client_sock):
        try:
            batch_size, data = batch_message.split(",", 1)
            # If agency sent FIN message do not process bets
            if not self.__agency_sent_fin(data):
                parsed_batch_data = parse_batch_data(data, int(batch_size))
                store_bets(parsed_batch_data)

                logging.info(
                    f"action: apuesta_recibida | result: success | cantidad: {batch_size}"
                )
                # Send success to the client
                self.__send_all(client_sock, "{}\n".format("EXITO").encode("utf-8"))
        except (ValueError, RuntimeError) as e:
            logging.error(
                f"action: apuesta_recibida | result: fail | error: {e} cantidad: {batch_size or 0}"
            )
            # Send error to the client
            self.__send_all(client_sock, "{}\n".format("ERROR").encode("utf-8"))

    def __process_fin(self, agency):
        _, agency_number = agency.split("=")
        self._agencies[agency_number] = True
        if self.__all_agencies_finished():
            logging.info("action: sorteo | result: success")

    def __all_agencies_finished(self):
        # TODO: pass this to a constant (after my question is answered)
        if len(self._agencies) < 5:
            return False
        for finished in self._finished_agencies.values():
            if not finished:
                return False
        return True

    # def __process_who_won(self, message):

    def __process_message(self, message, client_sock):
        message_type, body = message.split(",", 1)

        # match case supported in 3.10 python version =(
        if message_type == "BATCH":
            self.__process_batch(body, client_sock)
        elif message_type == "FIN":
            self.__process_fin(body)
        # elif message_type == "GANADOR":
        #     self.__process_who_won(body)
        else:
            raise RuntimeError(f"Message type not recognized: {message_type}")

    def __handle_client_connection(self, client_sock):
        """
        Read message from a specific client socket and close the socket.

        If a problem arises in the communication with the client, the
        client socket will also be closed.
        """

        # Have to declare it here, otherwise the except block woudln't be able to access it
        batch_size = None

        try:
            msg = (
                self.__recv_all(client_sock, 1024)
                .rstrip(b"\n")
                .rstrip()
                .decode("utf-8")
            )
            addr = client_sock.getpeername()

            logging.info(
                f"action: receive_message | result: success | ip: {addr[0]} | msg: {msg}"
            )
            self.__process_message(msg, client_sock)
            # batch_size, data = msg.split(",", 1)

            # parsed_batch_data = parse_batch_data(data, int(batch_size))
            # store_bets(parsed_batch_data)

            # logging.info(
            #     f"action: apuesta_recibida | result: success | cantidad: {batch_size}"
            # )
            # # Send success to the client
            # self.__send_all(client_sock, "{}\n".format("EXITO").encode("utf-8"))
        except (ValueError, RuntimeError) as e:
            logging.error(
                f"action: apuesta_recibida | result: fail | error: {e} cantidad: {batch_size or 0}"
            )
            # Send error to the client
            self.__send_all(client_sock, "{}\n".format("ERROR").encode("utf-8"))
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
