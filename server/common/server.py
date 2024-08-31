import socket
import logging
import signal
import time

from common.utils import Bet, store_bets


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

    def __validate_bet_data(self, data):
        expected_keys = [
            "AGENCIA",
            "NOMBRE",
            "APELLIDO",
            "DOCUMENTO",
            "NACIMIENTO",
            "NUMERO",
        ]
        for key in expected_keys:
            if key not in data.keys():
                raise RuntimeError("Missing data: ", key)

    """ 
    Parses a message with the format: K1=V1,K2=V2,...,Kn=Vn and returns a Bet object.
    Raises an exception if the data is not correctly formatted, or something is misising 
    """

    def __parse_csv_kv(self, msg):
        data = dict()
        # Split by comma, leaving a list of [Key=Value, ...] values
        separated_csv = msg.split(",")

        for kv in separated_csv:
            if "=" not in kv:
                raise ValueError(f"Invalid key-value pair format: '{kv}'")

            # Split by "=", leaving a (key,value) pair
            k, v = kv.split("=")

            k = k.strip()
            v = v.strip()

            if not k:
                raise ValueError(f"Empty key found in: '{kv}'")
            if not v:
                raise ValueError(f"Empty value found for key '{k}'")

            if k in data:
                raise ValueError(f"Duplicate key found: '{k}'")

            data[k] = v

        self.__validate_bet_data(data)

        bet = Bet(
            agency=data["AGENCIA"],
            first_name=data["NOMBRE"],
            last_name=data["APELLIDO"],
            document=data["DOCUMENTO"],
            birthdate=data["NACIMIENTO"],
            number=data["NUMERO"],
        )

        return bet

    def __parse_batch_data(self, batch, expectedBatchSize):
        if not batch:
            raise ValueError("Batch data is empty")

        records = batch.split(":")
        bets = [self.__parse_csv_kv(record) for record in records]

        if len(bets) != expectedBatchSize:
            raise RuntimeError(
                f"Expected batch size: {expectedBatchSize}, got batch of: {len(bets)}"
            )

        return bets

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
            batch_size, data = msg.split(",", 1)

            parsed_batch_data = self.__parse_batch_data(data, int(batch_size))
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
