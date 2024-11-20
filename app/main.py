import socket
import threading  # Import threading for handling multiple clients


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
            
            data: str = request.decode().strip()
            print(f"Received from {client_address}: {data}")

            # Handle PING command
            if "ping" in data.lower():
                response = "+PONG\r\n"
                client_socket.send(response.encode())
            else:
                # Handle unknown commands
                response = "-Error: Unknown Command\r\n"
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