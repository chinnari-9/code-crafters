import socket
import threading
import time
from datetime import datetime, timedelta

# In-memory storage for key-value pairs and expiries
storage = {}
expiry_times = {}  # Stores expiration times for keys


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
            elif args[0].upper() == "PING":
                response = "+PONG\r\n"
            elif args[0].upper() == "ECHO":
                if len(args) == 2:
                    message = args[1]
                    response = f"${len(message)}\r\n{message}\r\n"
                else:
                    response = "-Error: ECHO requires exactly one argument\r\n"
            elif args[0].upper() == "SET":
                response = handle_set(args)
            elif args[0].upper() == "GET":
                response = handle_get(args)
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
    Handle the SET command with optional PX argument.
    """
    if len(args) < 3:
        return "-Error: SET requires at least two arguments\r\n"

    key, value = args[1], args[2]
    expiry = None

    if len(args) > 3 and args[3].upper() == "PX":
        if len(args) < 5:
            return "-Error: PX requires a value in milliseconds\r\n"
        try:
            expiry = int(args[4])
            expiry_time = datetime.now() + timedelta(milliseconds=expiry)
            expiry_times[key] = expiry_time
        except ValueError:
            return "-Error: PX value must be an integer\r\n"

    storage[key] = value
    if key not in expiry_times:
        expiry_times.pop(key, None)  # Clear expiry if no PX is provided
    return "+OK\r\n"


def handle_get(args):
    """
    Handle the GET command and check for expired keys.
    """
    if len(args) != 2:
        return "-Error: GET requires exactly one argument\r\n"

    key = args[1]

    # Check if key exists and has expired
    if key in expiry_times:
        if datetime.now() >= expiry_times[key]:
            del storage[key]
            del expiry_times[key]
            return "$-1\r\n"  # RESP null bulk string

    # Return the value if the key exists
    if key in storage:
        value = storage[key]
        return f"${len(value)}\r\n{value}\r\n"

    return "$-1\r\n"  # RESP null bulk string for non-existent keys


def expiry_cleaner():
    """
    Periodically clean up expired keys.
    """
    while True:
        time.sleep(0.1)  # Check every 100ms
        now = datetime.now()
        keys_to_delete = [key for key, expiry in expiry_times.items() if now >= expiry]
        for key in keys_to_delete:
            del storage[key]
            del expiry_times[key]


def main():
    # Create the server socket and bind to port 6379
    server_socket = socket.create_server(("localhost", 6379), reuse_port=True)
    print("Server is running and waiting for connections on port 6379")

    # Start the expiry cleaner thread
    cleaner_thread = threading.Thread(target=expiry_cleaner, daemon=True)
    cleaner_thread.start()

    while True:
        # Accept a new client
        client_socket, client_address = server_socket.accept()
        print(f"Accepted connection from {client_address}")

        # Create a new thread for each client
        client_thread = threading.Thread(target=handle_client, args=(client_socket, client_address))
        client_thread.start()


if __name__ == "__main__":
    main()