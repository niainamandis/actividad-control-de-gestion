import socket
import random
import time
from socketUDP import SocketUDP
from slidingWindowCC import SlidingWindowCC

class SocketTCP:

  HEADER_SIZE = 7
  READ_BUFF_SIZE = HEADER_SIZE + 16

  def __init__(self):
    self.udp_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    self.origin_address = None # se llena en bind
    self.dest_address = None # se llena en connect/accept
    self.seq = None # se define en el handshake
    self.expected_seq = None # para controlar duplicados recibidos

    # nuevos para stop&wait
    self.timeout = 1.5
    self.msg_len = None # se define al llegar un msje
    self.msg_received = 0
    self.rec_buff = b""

  def bind(self, address):
    self.udp_socket.bind(address)
    self.origin_address = address

  def connect(self, address):
    self.dest_address = address
    # numero random entre 0 y 100 para handshake
    self.seq = random.randint(0,100)
    seg = self._make(self.seq, syn=1)

    self.udp_socket.settimeout(self.timeout)

    # esperamos respuesta
    while True:
      self.udp_socket.sendto(seg, self.dest_address)
      try:
        seg_bytes, resp_address = self.udp_socket.recvfrom(self.READ_BUFF_SIZE)
        resp = self.parse_segment(seg_bytes)
        if resp["SYN"] == 1 and resp["ACK"] == 1:
          self.dest_address = resp_address
          self.seq += 1
          self.expected_seq = resp["SEQ"] + 1
          break
      except socket.timeout:
        pass
    self.udp_socket.settimeout(None)
    seg_2 = self._make(self.seq, ack=1)
    #--- LÍNEA A COMENTAR PARA INDUCIR PÉRDIDAS EN TEST DE CASO BORDE ---#
    self.udp_socket.sendto(seg_2, self.dest_address)

  def accept(self):
    # esperando
    while True:
      seg_bytes, client_address = self.udp_socket.recvfrom(self.READ_BUFF_SIZE)
      seg = self.parse_segment(seg_bytes)
      if seg["SYN"] == 1 and seg["ACK"] == 0:
        break

    new_sock = SocketTCP()
    new_sock.bind((self.origin_address[0], 0))
    new_sock.origin_address = new_sock.udp_socket.getsockname()
    new_sock.dest_address = client_address
    new_sock.seq = random.randint(0, 100)
    new_sock.expected_seq = seg["SEQ"] + 1

    seg_resp = self._make(new_sock.seq, syn=1, ack=1)
    new_sock.udp_socket.settimeout(self.timeout)

    # esperar el ACK = 1
    # ------ MOD CASO BORDE AQUÍ :d ------ #
    while True:
      new_sock.udp_socket.sendto(seg_resp, client_address)
      try:
        seg_bytes, addr = new_sock.udp_socket.recvfrom(self.READ_BUFF_SIZE)
        if addr != client_address: continue
        seg_2 = self.parse_segment(seg_bytes)
        if seg_2["ACK"]:
          new_sock.seq += 1
          break
        elif seg_2["SYN"] == 0 and seg_2["ACK"] == 0 and seg_2["FIN"] == 0:
          # acumula datos solo si es el seq esperado
          if seg_2["SEQ"] == new_sock.expected_seq:
            new_sock.msg_len = int.from_bytes(seg_2["DATOS"], "big")
            new_sock.expected_seq += 1
          ack_msg = self._make(new_sock.seq, ack=1)
          new_sock.udp_socket.sendto(ack_msg, new_sock.dest_address)
          new_sock.seq += 1
          break
      except socket.timeout:
        pass
    new_sock.udp_socket.settimeout(None)
    return new_sock, new_sock.origin_address

  def send(self, message, mode="stop_and_wait"):
    # elige la función según el modo
    if mode == "stop_and_wait":
      self.send_using_stop_and_wait(message)
    elif mode == "go_back_n":
      self.send_using_go_back_n(message)

  def recv(self, buff_size, mode="stop_and_wait"):
    # elige la función según el modo
    if mode == "stop_and_wait":
      return self.recv_using_stop_and_wait(buff_size)
    elif mode == "go_back_n":
      return self.recv_using_go_back_n(buff_size)

  def send_using_stop_and_wait(self, message):
    self.udp_socket.settimeout(self.timeout)
    def _send(payload):
      tmp = self._make(self.seq, datos=payload)
      while True:
        self.udp_socket.sendto(tmp, self.dest_address)
        try:
          data, addr = self.udp_socket.recvfrom(self.READ_BUFF_SIZE)
          seg = self.parse_segment(data)
          if seg["ACK"] == 1 and seg["SEQ"] == self.seq and addr == self.dest_address:
            self.seq += 1
            break
        except socket.timeout:
          pass # reintenta

    # enviar el tamaño en bytes
    msglen = len(message)
    _send(msglen.to_bytes(4, "big"))

    for i in range(0, msglen, 16):
      # trozos de 16 bytes a lo más para enviar msje
      _send(message[i:i + 16])

    self.udp_socket.settimeout(None)

  def recv_using_stop_and_wait(self, buff_size):
    # numera por segmento: expected_seq avanza de a 1
    return self._recv_data(buff_size, by_bytes=False)

  def send_using_go_back_n(self, message):
    # fija el tamaño de ventana
    window_size = 8

    # envuelve el socket ya creado en un socketudp con un solo timer
    udp = SocketUDP()
    udp.socket_udp = self.udp_socket
    udp.socket_udp.setblocking(False)
    udp.settimeout(self.timeout)

    # arma la lista de datos: largo del mensaje y trozos de 16 bytes
    msglen = len(message)
    data_list = [msglen.to_bytes(4, "big")] + [message[i:i + 16] for i in range(0, msglen, 16)]
    initial_seq = self.seq
    data_window = SlidingWindowCC(window_size, data_list, initial_seq)

    # envía la ventana inicial
    self._send_window(udp, data_window)

    # repite hasta que se confirme toda la ventana
    while data_window.get_data(0) is not None:
      try:
        answer, address = udp.recvfrom(self.READ_BUFF_SIZE)
        seg = self.parse_segment(answer)
        # valida que sea un ack del destino
        if seg["ACK"] == 1 and address == self.dest_address:
          # cuenta los segmentos confirmados por el ack acumulativo
          steps = 0
          for i in range(window_size):
            data = data_window.get_data(i)
            if data is not None and data_window.get_sequence_number(i) + len(data) <= seg["SEQ"]:
              steps += 1
            else:
              break
          if steps > 0:
            # reinicia el timer y avanza la ventana
            udp.stop_timer(timer_index=0)
            data_window.move_window(steps)
            # envía los nuevos segmentos; si no hay, reenvía el primero para reiniciar el timer
            sent_new = self._send_window(udp, data_window, window_size - steps)
            if not sent_new:
              self._send_from_window(udp, data_window, 0)
      except TimeoutError:
        # reinicia el timer y reenvía toda la ventana
        udp.stop_timer(timer_index=0)
        self._send_window(udp, data_window)

    # actualiza el seq con los bytes enviados
    self.seq = initial_seq + sum(len(d) for d in data_list)
    # devuelve el socket a modo bloqueante
    self.udp_socket.settimeout(None)

  def recv_using_go_back_n(self, buff_size):
    # numera por bytes: expected_seq avanza el largo de los datos
    return self._recv_data(buff_size, by_bytes=True)

  def _send_from_window(self, udp, window, i):
    # retorna false si el índice no tiene datos
    data = window.get_data(i)
    if data is None:
      return False
    seg = self._make(window.get_sequence_number(i), datos=data)
    udp.sendto(seg, self.dest_address, timer_index=0)
    return True

  def _send_window(self, udp, window, start=0):
    # envía los segmentos desde start y retorna si envió alguno
    sent = False
    for i in range(start, window.window_size):
      sent = self._send_from_window(udp, window, i) or sent
    return sent

  def _recv_one(self, by_bytes, is_length):
    # recibe un segmento, lo guarda si es el esperado y responde con ack
    seg = self.parse_segment(self.udp_socket.recvfrom(self.READ_BUFF_SIZE)[0])
    if seg["SYN"] or seg["ACK"] or seg["FIN"]:
      return
    # procesa solo si el seq es el esperado
    if seg["SEQ"] == self.expected_seq:
      if is_length:
        self.msg_len = int.from_bytes(seg["DATOS"], "big")
      else:
        self.rec_buff += seg["DATOS"]
        self.msg_received += len(seg["DATOS"])
      self.expected_seq += len(seg["DATOS"]) if by_bytes else 1
    # go-back-n responde con el siguiente seq esperado, stop & wait con el seq recibido
    ack_seq = self.expected_seq if by_bytes else seg["SEQ"]
    self.udp_socket.sendto(self._make(ack_seq, ack=1), self.dest_address)

  def _recv_data(self, buff_size, by_bytes):
    # reinicia el estado si el mensaje anterior ya se entregó completo
    if self.msg_len is not None and self.msg_received == self.msg_len and not self.rec_buff:
      self.msg_len = None
      self.msg_received = 0

    # espera el segmento con el largo del mensaje
    while self.msg_len is None:
      self._recv_one(by_bytes, is_length=True)

    # recibe datos hasta completar lo pedido
    recibo = min(self.msg_len - (self.msg_received - len(self.rec_buff)), buff_size)
    while len(self.rec_buff) < recibo and self.msg_received < self.msg_len:
      self._recv_one(by_bytes, is_length=False)

    # retorna los datos pedidos
    devolver = self.rec_buff[:buff_size]
    self.rec_buff = self.rec_buff[buff_size:]
    return devolver

  def close(self):
    # fin manejado por A
    fin = self._make(self.seq, fin=1)
    self.udp_socket.settimeout(self.timeout)

    # esperar fin y ack de b
    intentos = 0
    while intentos < 3:
      self.udp_socket.sendto(fin, self.dest_address)
      try:
        seg_bytes, addr = self.udp_socket.recvfrom(self.READ_BUFF_SIZE)
        seg = self.parse_segment(seg_bytes)
        if seg["ACK"] == 1 and seg["FIN"] == 1 and addr == self.dest_address:
          self.seq = seg["SEQ"] + 1
          break
      except socket.timeout:
        intentos += 1
    else:
      # 3 timeouts: asumimos que la contraparte cerró
      self.udp_socket.close()
      return

    # enviar ack final
    fin_final = self._make(self.seq, ack=1)
    for _ in range(3):
      self.udp_socket.sendto(fin_final, self.dest_address)
      time.sleep(self.timeout)
    self.udp_socket.close()

  def recv_close(self):
    # fin manejado por B
    # esperar fin de A
    while True:
      seg_bytes, addr = self.udp_socket.recvfrom(self.READ_BUFF_SIZE)
      if addr != self.dest_address: continue
      seg = self.parse_segment(seg_bytes)

      if seg["FIN"] == 1 and seg["ACK"] == 0:
        self.seq = seg["SEQ"] + 1
        break
      elif seg["FIN"] == 0 and seg["SYN"] == 0 and seg["ACK"] == 0:
        self.udp_socket.sendto(self._make(seg["SEQ"], ack=1), self.dest_address)

    # enviar fin + ack final
    fin = self._make(self.seq, ack=1, fin=1)
    self.udp_socket.settimeout(self.timeout)

    # esperar ack de A
    intentos = 0
    while intentos < 3:
      self.udp_socket.sendto(fin, self.dest_address)
      try:
        seg_bytes, addr = self.udp_socket.recvfrom(self.READ_BUFF_SIZE)
        if addr != self.dest_address: continue
        seg = self.parse_segment(seg_bytes)
        if seg["ACK"] == 1:
          self.seq = seg["SEQ"] + 1
          break
      except socket.timeout:
        intentos += 1

    self.udp_socket.settimeout(None)
    self.udp_socket.close()

  @staticmethod
  def parse_segment(segment_bytes):
    return {
      "SYN": segment_bytes[0],
      "ACK": segment_bytes[1],
      "FIN": segment_bytes[2],
      "SEQ": int.from_bytes(segment_bytes[3:7], "big"),
      "DATOS": segment_bytes[7:],
    }

  @staticmethod
  def create_segment(segment):
    header = bytes([segment["SYN"], segment["ACK"], segment["FIN"]])
    header += segment["SEQ"].to_bytes(4, "big")
    return header + segment["DATOS"]

  @staticmethod
  def _make(seq, syn=0, ack=0, fin=0, datos=b""):
    # arma un segmento con los flags indicados
    return SocketTCP.create_segment({
        "SYN": syn, "ACK": ack, "FIN": fin,
        "SEQ": seq, "DATOS": datos
        })
