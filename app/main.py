import asyncio
import argparse
import socket
from enum import Enum

# Placeholder for Constants and Data Types
class DataType(Enum):
    ARRAY = 1
    BULK_STRING = 2
    SIMPLE_ERROR = 3

class Constant:
    INVALID_COMMAND = "ERR unknown command"

# Global configuration variables
dir = None
dbfilename = None
replication = {'role': 'master', 'master_replid': '8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb', 'master_repl_offset': 0}

async def encode(data_type, data):
    """
    Simulate RESP encoding for various data types (Array, Bulk String, Error).
    """
    if data_type == DataType.ARRAY:
        return f"*{len(data)}\r\n" + ''.join([f"${len(d)}\r\n{d}\r\n" for d in data])
    elif data_type == DataType.BULK_STRING:
        return f"${len(data)}\r\n{data}\r\n"
    elif data_type == DataType.SIMPLE_ERROR:
        return f"-{data}\r\n"
    return None

async def handle_command(commands, writer):
    """
    Handle incoming commands and provide responses.
    """
    response = None
    if commands[0].upper() == 'CONFIG':
        if len(commands) >= 3:
            if commands[1].lower() == 'get' and commands[2].lower() == 'dir':
                # Respond with the 'dir' configuration
                response = await encode(DataType.ARRAY, [await encode(DataType.BULK_STRING, "dir".encode()), await encode(DataType.BULK_STRING, dir.encode())])
            elif commands[1].lower() == 'get' and commands[2].lower() == 'dbfilename':
                # Respond with the 'dbfilename' configuration
                response = await encode(DataType.ARRAY, [await encode(DataType.BULK_STRING, "dbfilename".encode()), await encode(DataType.BULK_STRING, dbfilename.encode())])
            else:
                # Invalid CONFIG GET parameter
                response = await encode(DataType.SIMPLE_ERROR, Constant.INVALID_COMMAND)
        else:
            # Invalid CONFIG GET command structure
            response = await encode(DataType.SIMPLE_ERROR, Constant.INVALID_COMMAND)

    if writer and response is not None:
        writer.write(response.encode())
        await writer.drain()

async def start_server(port):
    """
    Start the Redis server and handle client connections.
    """
    server = await asyncio.start_server(handle_client, 'localhost', port)
    print(f'Server is running and waiting for connections on port {port}')
    async with server:
        await server.serve_forever()

async def handle_client(reader, writer):
    """
    Handle communication with a single client.
    """
    data = await reader.read(100)
    message = data.decode()
    addr = writer.get_extra_info('peername')
    print(f"Received {message} from {addr}")

    # Simple command parsing (splitting by spaces)
    commands = message.strip().split()
    await handle_command(commands, writer)
    
    print(f"Closing connection with {addr}")
    writer.close()
    await writer.wait_closed()

def main():
    # Parse command-line arguments
    parser = argparse.ArgumentParser()
    parser.add_argument('-p', '--port', type=int, default=6379)
    parser.add_argument('--replicaof', nargs=2, type=str)
    parser.add_argument('--dir', type=str)
    parser.add_argument('--dbfilename', type=str)
    args = parser.parse_args()

    # Update global dir and dbfilename based on args
    global dir, dbfilename
    if args.dir:
        dir = args.dir
    if args.dbfilename:
        dbfilename = args.dbfilename

    print(f"Configuration: dir={dir}, dbfilename={dbfilename}")

    # Start the server
    asyncio.run(start_server(args.port))

if __name__ == '__main__':
    main()