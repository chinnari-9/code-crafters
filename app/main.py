import socket  # noqa: F401

def main():
    # Print debug logs (optional, visible when running tests)
    print("Logs from your program will appear here!")

    # Uncomment and fix to bind to the required port
    server_socket = socket.create_server(("localhost", 6379), reuse_port=True)
    print("Server is running and waiting for connections on port 6379")

    # Wait for a client to connect (this is required to pass the stage)
    client_socket, client_address = server_socket.accept()
    print(f"Accepted connection from {client_address}")
    
    while True:
        request: bytes = client_socket.recv(512)
        data: str = request.decode()
        # print(data)
        if "ping" in data.lower():
            client_socket.send("+PONG\r\n".encode())
            
    # Keep the program running (or close client after testing)
    client_socket.close()

if __name__ == "__main__":
    main()