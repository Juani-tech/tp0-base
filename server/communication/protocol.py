from common.utils import Bet, store_bets, winners_for_agency
import logging
from threading import Lock, Condition

class Protocol:
    def __init__(self, got_sigterm, sigterm_cv, total_agencies):
        """
        Initializes the Protocol class with necessary synchronization objects and
        keeps track of the total number of agencies.

        Args:
            got_sigterm (threading.Event): Event to signal that a SIGTERM was received.
            sigterm_cv (threading.Condition): Condition variable to synchronize the shutdown.
            total_agencies (int): Total number of agencies expected to send bets.
        """
        self._got_sigterm = got_sigterm
        self._total_agencies = total_agencies
        self._store_bets_lock = Lock()
        self._load_bets_lock = Lock()
        self._agencies_sent_fin = dict()
        self._agencies_sent_fin_lock = Lock()
        self._sigterm_cv = sigterm_cv

    def __validate_bet_data(self, data):
        """
        Validates that all required keys are present in the data.

        Args:
            data (dict): Dictionary containing bet information.

        Raises:
            RuntimeError: If any required key is missing.
        """
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

    def __parse_csv_kv(self, msg):
        """
        Parses a message in the format K1=V1,K2=V2,...,Kn=Vn and returns a Bet object.

        Args:
            msg (str): String containing key-value pairs.

        Returns:
            Bet: Bet object created from the parsed key-value pairs.

        Raises:
            ValueError: If the data is not correctly formatted or contains duplicate/missing keys.
        """
        data = dict()
        separated_csv = msg.split(",")

        for kv in separated_csv:
            if "=" not in kv:
                raise ValueError(f"Invalid key-value pair format: '{kv}'")

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
        """
        Parses batch data into individual bet records and ensures the number
        of records matches the expected batch size.

        Args:
            batch (str): String containing batch data.
            expectedBatchSize (int): The expected number of bets in the batch.

        Returns:
            list: List of Bet objects.

        Raises:
            ValueError: If batch data is empty.
            RuntimeError: If the number of parsed bets doesn't match the expected batch size.
        """
        if not batch:
            raise ValueError("Batch data is empty")

        records = batch.split(":")
        bets = [self.__parse_csv_kv(record) for record in records]

        if len(bets) != expectedBatchSize:
            raise RuntimeError(
                f"Expected batch size: {expectedBatchSize}, got batch of: {len(bets)}"
            )

        return bets

    def __format_winners(self, winners):
        """
        Formats the winners list into a string for sending to the agencies.

        Args:
            winners (list): List of winner names.

        Returns:
            str: Formatted string of winners.
        """
        msg = f"{len(winners)},"
        for winner in winners:
            msg += winner
            if winner != winners[-1]:
                msg += ","
        msg += "\n"
        return msg

    def __send_results_to_agency(self, agency_number, agency_socket):
        """
        Sends the results to the specified agency after ensuring that all bets
        have been loaded.

        Args:
            agency_number (int): Agency number to send results to.
            agency_socket (socket): Socket to send the results to.

        Raises:
            SystemExit: If a SIGTERM signal was received.
        """
        logging.debug("Waiting for load_bets lock...")
        with self._load_bets_lock:
            if self._got_sigterm.is_set():
                raise SystemExit
            winners = winners_for_agency(agency_number)
        msg = "{}".format(self.__format_winners(winners)).encode("utf-8")

        logging.debug(f"Enviando a la agencia: {agency_number} | msg: {msg}")

        agency_socket.send_all(msg)

    def process_batch(self, batch_message, client_sock):
        """
        Processes a batch of bets sent by a client, stores the bets, and sends
        a success or error message back to the client.

        Args:
            batch_message (str): The batch message containing bet data.
            client_sock (socket): Client socket to send the response to.
        """
        batch_size = None
        try:
            batch_size, data = batch_message.split(",", 1)
            parsed_batch_data = self.__parse_batch_data(data, int(batch_size))

            with self._store_bets_lock:
                if self._got_sigterm.is_set():
                    raise SystemExit
                store_bets(parsed_batch_data)

            logging.info(
                f"action: apuesta_recibida | result: success | cantidad: {batch_size}"
            )
            # Send success to the client
            client_sock.send_all("{}\n".format("EXITO").encode("utf-8"))

        except (ValueError, RuntimeError) as e:
            logging.error(
                f"action: apuesta_recibida | result: fail | error: {e} cantidad: {batch_size or 0}"
            )
            # Send error to the client
            client_sock.send_all("{}\n".format("ERROR").encode("utf-8"))

    def process_fin(self, agency):
        """
        Marks an agency as finished sending bets and checks if all agencies
        have finished. If all have finished, it notifies the waiting threads.

        Args:
            agency (str): String containing the agency number.
        """
        _, agency_number = agency.split("=")
        with self._agencies_sent_fin_lock:
            self._agencies_sent_fin[agency_number] = True
        if self.__all_agencies_finished():
            logging.info("action: sorteo | result: success")
            with self._sigterm_cv:
                self._sigterm_cv.notify_all()

    def __all_agencies_finished(self):
        """
        Checks if all agencies have finished sending bets.

        Returns:
            bool: True if all agencies are finished, False otherwise.
        """
        if len(self._agencies_sent_fin) < self._total_agencies:
            return False
        with self._agencies_sent_fin_lock:
            for finished in self._agencies_sent_fin.values():
                if not finished:
                    return False
        return True

    def process_who_won(self, agency, client_sock):
        """
        Sends the winning results to the specified agency once all agencies
        have finished sending bets.

        Args:
            agency (str): String containing the agency number.
            client_sock (socket): Client socket to send the results to.

        Raises:
            SystemExit: If a SIGTERM signal was received.
        """
        _, agency_number = agency.split("=")

        with self._sigterm_cv:
            while not self.__all_agencies_finished():
                if self._got_sigterm.is_set():
                    raise SystemExit
                self._sigterm_cv.wait()

        self.__send_results_to_agency(int(agency_number), client_sock)
