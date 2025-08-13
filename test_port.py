# test_port.py
import socket

HOST = '127.0.0.1'
PORT = 9042

try:
    s = socket.create_connection((HOST, PORT), timeout=5) # 5-second timeout
    s.close()
    print(f"Successfully connected to {HOST}:{PORT}")
except socket.timeout:
    print(f"Connection to {HOST}:{PORT} timed out.")
except ConnectionRefusedError:
    print(f"Connection to {HOST}:{PORT} refused. Is Cassandra running?")
except Exception as e:
    print(f"An unexpected error occurred: {e}")