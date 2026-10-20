from SocketTCP import SocketTCP

server_address = ("10.0.2.15", 8000) # amandis 192.168.64.2
                                     # dani 10.0.2.15
server_socket = SocketTCP()

server_socket.bind(server_address)
connection_socketTCP, new_address = server_socket.accept()
print(f"[Servidor] Conexión establecida desde {new_address}")
n = 160 
parte1 = connection_socketTCP.recv(n, mode="go_back_n")
print(f"[Servidor] Parte 1 (len={len(parte1)}): {parte1}")

parte2 = connection_socketTCP.recv(n, mode="go_back_n")
print(f"[Servidor] Parte 2 (len={len(parte2)}): {parte2}")

mensaje_total = parte1 + parte2
print(f"[Servidor] Total recibido (len={len(mensaje_total)}): {mensaje_total.decode()}")

print("[Servidor] Esperando cierre de conexión...")
connection_socketTCP.recv_close()
print("[Servidor] Conexión cerrada exitosamente.")
server_socket.udp_socket.close()
