import socket
import threading

# In-memory storage for key-value pairs
storage = {}


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
                if len(args) == 3:
                    key, value = args[1], args[2]
                    storage[key] = value
                    response = "+OK\r\n"
                else:
                    response = "-Error: SET requires exactly two arguments\r\n"
            elif args[0].upper() == "GET":
                if len(args) == 2:
                    key = args[1]
                    if key in storage:
                        value = storage[key]
                        response = f"${len(value)}\r\n{value}\r\n"
                    else:
                        response = "$-1\r\n"  # RESP null bulk string for non-existent keys
                else:
                    response = "-Error: GET requires exactly one argument\r\n"
            else:
                response = "-Error: Unknown Command\r\n"

            # Send the response
            client_socket.send(response.encode())

    except Exception as e:
        print(f"Error with client {client_address}: {e}")
    finally:
        client_socket.close()
        print(f"Connection with {client_address} closed.")


def main():
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