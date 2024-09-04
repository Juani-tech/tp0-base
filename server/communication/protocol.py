from common.utils import Bet, store_bets, winners_for_agency
import logging
from threading import Lock, Condition

class Protocol: 
    def __init__(self, got_sigterm, sigterm_cv, total_agencies):
        self._got_sigterm = got_sigterm
        self._total_agencies = total_agencies
        self._store_bets_lock = Lock()
        self._load_bets_lock = Lock()
        self._total_finished = 0
        self._total_finished_lock = Lock()
        self._sigterm_cv = sigterm_cv

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

    """
    Returns a list with all the records from a batch, checks if the length
    of the received batch is the same as the announced
    """

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


    def __format_winners(self,winners):
        msg = f"{len(winners)},"
        for winner in winners:
            msg += winner
            if winner != winners[-1]:
                msg += ","
        msg += "\n"
        return msg




    """
    Iterate over all waiting agencies, send the winners for that agency and remove it from
    the waiting list
    """

    def __send_results_to_agency(self, agency_number, agency_socket):
        logging.debug("Waiting for load_bets lock...")
        with self._load_bets_lock: 
            if self._got_sigterm.is_set(): 
                raise SystemExit
            winners = winners_for_agency(agency_number)
        msg = "{}".format(self.__format_winners(winners)).encode("utf-8")

        logging.debug(f"Enviando a la agencia: {agency_number} | msg: {msg}")
        
        agency_socket.send_all(msg)


        # agency_socket.close()



    def process_batch(self, batch_message, client_sock):
        # Have to declare it here, otherwise the except block woudln't be able to access it
        batch_size = None
        try:
            batch_size, data = batch_message.split(",", 1)
            # If agency sent FIN message do not process bets
            # if not self.__agency_sent_fin(data):
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




    def process_fin(self):
        with self._total_finished_lock:
            self._total_finished += 1
        if self._total_finished == self._total_agencies:
            logging.info("action: sorteo | result: success")
            with self._sigterm_cv: 
                self._sigterm_cv.notify_all()
        # _, agency_number = agency.split("=")
        # with self._agencies_lock: 
            # self._agencies[agency_number] = True
            # if self.__all_agencies_finished():



    def process_who_won(self, agency, client_sock):
        _, agency_number = agency.split("=")

        with self._sigterm_cv: 
            while self._total_finished < self._total_agencies :
                if self._got_sigterm.is_set():
                    raise SystemExit
                self._sigterm_cv.wait()


        logging.debug("Finished waiting for barrier")

        self.__send_results_to_agency(int(agency_number), client_sock)

        # self._finished_agencies.wait()
        # if self.__all_agencies_finished():
            # all agencies finished -> sending results
        