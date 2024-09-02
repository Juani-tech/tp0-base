import sys
import yaml
import os

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python mi-generador.py <path_to_output_yaml> <number_of_clients>")
        sys.exit(1)

    output_path = sys.argv[1]

    # if not os.path.isdir(os.path.dirname(output_path)):
    #     print(f"Error: The path '{output_path}' is not a valid directory.")
    #     sys.exit(1)

    try:
        number_of_clients = int(sys.argv[2])
        if number_of_clients <= 0:
            raise ValueError

    except ValueError:
        print("Error: The second argument must be a positive integer.")
        sys.exit(1)

    data = {
        "name": "tp0",
        "services": {
            "server": {
                "container_name": "server",
                "image": "server:latest",
                "entrypoint": "python3 /main.py",
                "environment": ["PYTHONUNBUFFERED=1", "LOGGING_LEVEL=DEBUG"],
                "networks": ["testing_net"],
                "volumes": ["./server/config.ini:/config.ini"]
            },
        },
        "networks": {
            "testing_net": {
                "ipam": {"driver": "default", "config": [{"subnet": "172.25.125.0/24"}]}
            }
        },
    }

    for i in range(1, number_of_clients + 1):
        client_name = f"client{i}"
        data["services"][client_name] = {
            "container_name": client_name,
            "image": "client:latest",
            "entrypoint": "/client",
            "environment": [f"CLI_ID={i}", "CLI_LOG_LEVEL=DEBUG"],
            "networks": ["testing_net"],
            "depends_on": ["server"],
            "volumes": ["./client/config.yaml:/config.yaml"],
        }

    with open(sys.argv[1], "w") as yaml_file:
        yaml.dump(data, yaml_file, default_flow_style=False)

    print(
        f"YAML file '{sys.argv[1]}' generated successfully with {number_of_clients} clients."
    )
