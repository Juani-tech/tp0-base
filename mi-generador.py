import random
import sys
import yaml


def format_date(year, month, day):
    return f"{year}-{month:02d}-{day:02d}"


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python mi-generador.py <path_to_output_yaml> <number_of_clients>")
        sys.exit(1)

    output_path = sys.argv[1]

    try:
        # Check validity of number of clients
        number_of_clients = int(sys.argv[2])
        if number_of_clients <= 0:
            raise ValueError

    except ValueError:
        print("Error: The second argument must be a positive integer.")
        sys.exit(1)

    # Skeleton (starting point)
    data = {
        "name": "tp0",
        "services": {
            "server": {
                "container_name": "server",
                "image": "server:latest",
                "entrypoint": "python3 /main.py",
                "environment": ["PYTHONUNBUFFERED=1", "LOGGING_LEVEL=DEBUG"],
                "networks": ["testing_net"],
            },
        },
        "networks": {
            "testing_net": {
                "ipam": {"driver": "default", "config": [{"subnet": "172.25.125.0/24"}]}
            }
        },
    }
    random.seed(81)

    names = ["Santiago", "Pablo", "Juan", "Ignacio", "Martin"]
    surnames = ["Perez", "Rodriguez", "Chiazza", "Martinez"]
    # Iterate and add to the skeleton the new clients
    for i in range(1, number_of_clients + 1):
        client_name = f"client{i}"
        year = random.randint(1990, 2024)
        month = random.randint(1, 12)
        day = random.randint(1, 28)
        birthDate = format_date(year, month, day)

        data["services"][client_name] = {
            "container_name": client_name,
            "image": "client:latest",
            "entrypoint": "/client",
            "environment": {
                "CLI_ID": i,
                "CLI_LOG_LEVEL": "DEBUG",
                "CLI_NOMBRE": random.choice(names),
                "CLI_APELLIDO": random.choice(surnames),
                # random.randint(0,28):02d -> adds left zero padding if necessary
                # (up to 2 characters)
                "CLI_NACIMIENTO": birthDate,
                "CLI_NUMERO": f"{random.randint(1, 10000)}",
                # Don't try to match document/year, it doesn't make sense in this context
                "CLI_DOCUMENTO": f"{random.randint(20_000_000, 40_000_000)}",
            },
            "networks": ["testing_net"],
            "depends_on": ["server"],
            "volumes": ["./client/config.yaml:/config.yaml"],
        }
    # Open the file and save the result
    with open(sys.argv[1], "w") as yaml_file:
        yaml.dump(data, yaml_file, default_flow_style=False)

    print(
        f"YAML file '{sys.argv[1]}' generated successfully with {number_of_clients} clients."
    )
