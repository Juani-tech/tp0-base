import csv
import datetime
import time
import logging


""" Bets storage location. """
STORAGE_FILEPATH = "./bets.csv"
""" Simulated winner number in the lottery contest. """
LOTTERY_WINNER_NUMBER = 7574


""" A lottery bet registry. """


class Bet:
    def __init__(
        self,
        agency: str,
        first_name: str,
        last_name: str,
        document: str,
        birthdate: str,
        number: str,
    ):
        """
        agency must be passed with integer format.
        birthdate must be passed with format: 'YYYY-MM-DD'.
        number must be passed with integer format.
        """
        self.agency = int(agency)
        self.first_name = first_name
        self.last_name = last_name
        self.document = document
        self.birthdate = datetime.date.fromisoformat(birthdate)
        self.number = int(number)


""" Checks whether a bet won the prize or not. """


def has_won(bet: Bet) -> bool:
    return bet.number == LOTTERY_WINNER_NUMBER


"""
Persist the information of each bet in the STORAGE_FILEPATH file.
Not thread-safe/process-safe.
"""


def store_bets(bets: list[Bet]) -> None:
    with open(STORAGE_FILEPATH, "a+") as file:
        writer = csv.writer(file, quoting=csv.QUOTE_MINIMAL)
        for bet in bets:
            writer.writerow(
                [
                    bet.agency,
                    bet.first_name,
                    bet.last_name,
                    bet.document,
                    bet.birthdate,
                    bet.number,
                ]
            )
            logging.info(
                f"action: apuesta_almacenada | result: success | dni: {bet.document} | numero: {bet.number}"
            )


"""
Loads the information all the bets in the STORAGE_FILEPATH file.
Not thread-safe/process-safe.
"""


def load_bets() -> list[Bet]:
    with open(STORAGE_FILEPATH, "r") as file:
        reader = csv.reader(file, quoting=csv.QUOTE_MINIMAL)
        for row in reader:
            yield Bet(row[0], row[1], row[2], row[3], row[4], row[5])


def validate_bet_data(data):
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


def parse_csv_kv(msg):
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

    validate_bet_data(data)

    bet = Bet(
        agency=data["AGENCIA"],
        first_name=data["NOMBRE"],
        last_name=data["APELLIDO"],
        document=data["DOCUMENTO"],
        birthdate=data["NACIMIENTO"],
        number=data["NUMERO"],
    )

    return bet


def parse_batch_data(batch, expectedBatchSize):
    if not batch:
        raise ValueError("Batch data is empty")

    records = batch.split(":")
    bets = [parse_csv_kv(record) for record in records]

    if len(bets) != expectedBatchSize:
        raise RuntimeError(
            f"Expected batch size: {expectedBatchSize}, got batch of: {len(bets)}"
        )

    return bets
