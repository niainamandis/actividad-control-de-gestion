from SocketTCP import SocketTCP

server_address = ("10.0.2.15", 8000) # amandis 192.168.64.2
                                     # dani 10.0.2.15
client_socket = SocketTCP()
client_socket.set_congestion_control(True)
client_socket.connect(server_address)
mensaje = b"A" * 50
print(f"[Cliente] Enviando {len(mensaje)} bytes con Go-Back-N...")
client_socket.send(mensaje, mode="go_back_n")
print("[Cliente] Cerrando conexión...")
client_socket.close()
print("[Cliente] Conexión cerrada exitosamente.")
