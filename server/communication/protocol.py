from common.utils import Bet

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