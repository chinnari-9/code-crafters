import socket
import threading
import time
import argparse

# Configuration parameters with default values
config = {
    "dir": "/tmp/redis-data",
    "dbfilename": "rdbfile"
}

# In-memory key-value store with expiry handling
db = {}
expiry_db = {}

def parse_resp(request: bytes):
    """
    Parse a RESP request and extract the command and arguments.
    """
    try:
        lines = request.decode().split("\r\n")
        if lines[0].startswith("*"):  # Check for RESP array
            arg_count = int(lines[0][1:])  # Number of arguments
            args = []
            for i in range(1, len(lines) - 1, 2):
                if lines[i].startswith("$"):
                    args.append(lines[i + 1])
            if len(args) == arg_count:
                return args
        return None
    except Exception as e:
        print(f"Error parsing RESP: {e}")
        return None


def handle_client(client_socket, client_address):
    """
    Handle communication with a single client.
    """
    print(f"New thread started for client: {client_address}")
    try:
        while True:
            # Receive data from the client
            request: bytes = client_socket.recv(512)
            if not request:  # If the client closes the connection
                print(f"Client {client_address} disconnected.")
                break

            print(f"Received from {client_address}: {request.decode()}")

            # Parse the RESP request
            args = parse_resp(request)
            if not args:
                response = "-Error: Invalid RESP format\r\n"
            elif args[0].upper() == "SET":
                response = handle_set(args)
            else:
                response = "-Error: Unknown Command\r\n"

            # Send the response
            client_socket.send(response.encode())

    except Exception as e:
        print(f"Error with client {client_address}: {e}")
    finally:
        client_socket.close()
        print(f"Connection with {client_address} closed.")


def handle_set(args):
    """
    Handle the SET command with optional PX argument for expiry.
    """
    if len(args) < 4 or args[2].upper() != "PX":
        # Simple SET command without expiry
        db[args[1]] = args[2]
        response = "+OK\r\n"
    else:
        # SET with PX (expiry)
        key = args[1]
        value = args[2]
        expiry_time = int(args[3])  # in milliseconds

        # Store the value and expiry time
        db[key] = value
        expiry_db[key] = time.time() + (expiry_time / 1000)  # store expiry time in seconds

        response = "+OK\r\n"
    
    return response


def main():
    # Parse command-line arguments
    parser = argparse.ArgumentParser(description="Redis server implementation")
    parser.add_argument("--dir", type=str, default="/tmp/redis-data", help="Directory for RDB files")
    parser.add_argument("--dbfilename", type=str, default="rdbfile", help="Name of the RDB file")
    args = parser.parse_args()

    # Update configuration with command-line arguments
    config["dir"] = args.dir
    config["dbfilename"] = args.dbfilename

    print(f"Configuration: dir={config['dir']}, dbfilename={config['dbfilename']}")

    # Create the server socket and bind to port 6379
    server_socket = socket.create_server(("localhost", 6379), reuse_port=True)
    print("Server is running and waiting for connections on port 6379")

    while True:
        # Accept a new client
        client_socket, client_address = server_socket.accept()
        print(f"Accepted connection from {client_address}")

        # Create a new thread for each client
        client_thread = threading.Thread(target=handle_client, args=(client_socket, client_address))
        client_thread.start()


if __name__ == "__main__":
    main()