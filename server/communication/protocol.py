from common.utils import Bet
import logging
from common.utils import Bet, store_bets

class Protocol: 
    def __init__(self):
        pass
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
    Parses a message with the format: K1=V1,K2=V2,...,Kn=Vn and returns a dictionary with the
    parsed key-values 
    """

    def parse_csv_kv(self, msg):
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
        bets = [self.parse_csv_kv(record) for record in records]

        if len(bets) != expectedBatchSize:
            raise RuntimeError(
                f"Expected batch size: {expectedBatchSize}, got batch of: {len(bets)}"
            )

        return bets

    def process_batch(self, batch_message, client_sock):
        # Have to declare it here, otherwise the except block woudln't be able to access it
        batch_size = None
        try:
            batch_size, data = batch_message.split(",", 1)
            parsed_batch_data = self.__parse_batch_data(data, int(batch_size))
            
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


