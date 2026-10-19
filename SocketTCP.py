import socket
import random
import time 
from socketUDP import SocketUDP
from slidingWindowCC import SlidingWindowCC

class SocketTCP:

  HEADER_SIZE = 7

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
    buff_size = self.HEADER_SIZE + 16
    self.dest_address = address
    # numero random entre 0 y 100 para handshake
    self.seq = random.randint(0,100)
    seg = self.create_segment({
        "SYN": 1, "ACK": 0, "FIN": 0,
        "SEQ": self.seq, "DATOS": b""
        })

    self.udp_socket.settimeout(self.timeout)

    # esperamos respuesta
    while True:
      self.udp_socket.sendto(seg, self.dest_address)
      try:
        seg_bytes, resp_address = self.udp_socket.recvfrom(buff_size)
        resp = self.parse_segment(seg_bytes)
        if (resp["SYN"] == 1 and resp["ACK"] == 1):
          self.dest_address = resp_address
          self.seq += 1
          self.expected_seq = resp["SEQ"] + 1
          break
      except socket.timeout:
        pass
    self.udp_socket.settimeout(None)
    seg_2 = self.create_segment({
        "SYN": 0, "ACK": 1, "FIN": 0,
        "SEQ": self.seq, "DATOS": b""
        })
    #--- LÍNEA A COMENTAR PARA INDUCIR PÉRDIDAS EN TEST DE CASO BORDE ---#
    self.udp_socket.sendto(seg_2, self.dest_address)

  def accept(self):
    buff_size = self.HEADER_SIZE + 16
    # esperando
    while True:
      seg_bytes, client_address = self.udp_socket.recvfrom(buff_size)
      seg = self.parse_segment(seg_bytes)
      if (seg["SYN"] == 1 and seg["ACK"] == 0):
        break

    new_sock = SocketTCP()
    new_sock.bind((self.origin_address[0], 0))
    new_sock.origin_address = new_sock.udp_socket.getsockname()
    new_sock.dest_address = client_address
    new_sock.seq = random.randint(0, 100)
    new_sock.expected_seq = seg["SEQ"] + 1

    seg_resp = new_sock.create_segment({
        "SYN": 1, "ACK": 1, "FIN": 0,
        "SEQ": new_sock.seq, "DATOS": b""
        })
    new_sock.udp_socket.settimeout(self.timeout)

    # esperar el ACK = 1
    # ------ MOD CASO BORDE AQUÍ :d ------ # 
    while True:
      new_sock.udp_socket.sendto(seg_resp, client_address)
      try:
        seg_bytes, addr = new_sock.udp_socket.recvfrom(buff_size)
        if addr != client_address: continue
        seg_2 = self.parse_segment(seg_bytes)
        if (seg_2["ACK"]):
          new_sock.seq += 1
          break
        elif seg_2["SYN"] == 0 and seg_2["ACK"] == 0 and seg_2["FIN"] == 0:
            # Acumula datos solo si es el SEQ esperado
            if seg_2["SEQ"] == new_sock.expected_seq:
              new_sock.msg_len = int.from_bytes(seg_2["DATOS"], "big")
              new_sock.expected_seq += 1
            ack_msg = new_sock.create_segment({
                "SYN": 0, "ACK": 1, "FIN": 0,
                "SEQ": new_sock.seq, "DATOS": b""
            })
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

  def recv(self, buff_size, mode="stop_and_wait"):
    # elige la función según el modo
    if mode == "stop_and_wait":
      # retorna los datos recibidos
      return self.recv_using_stop_and_wait(buff_size)

  def send_using_stop_and_wait(self, message):
    self.udp_socket.settimeout(self.timeout)
    def _send(payload):
      buff_size = self.HEADER_SIZE + 16 
      tmp = self.create_segment({
          "SYN": 0, "ACK": 0, "FIN": 0,
          "SEQ": self.seq, "DATOS": payload
          })
      while True:
        self.udp_socket.sendto(tmp, self.dest_address)
        try:
          data, addr = self.udp_socket.recvfrom(buff_size)
          seg = self.parse_segment(data)
          if seg["ACK"] == 1 and seg["SEQ"] == self.seq and addr == self.dest_address:
            self.seq+=1
            break
        except socket.timeout:
          pass # reintenta

    # enviar el tamaño en bytes
    msglen = len(message)
    payload = msglen.to_bytes(4, "big")
    _send(payload)

    for i in range(0, msglen, 16):
      # trozos de 16 bytes a lo más para enviar msje
      chunk = message[i:i + 16]
      _send(chunk)

    self.udp_socket.settimeout(None)

  def recv_using_stop_and_wait(self, buff_size):
    read_buff_size = self.HEADER_SIZE + 16
    # si el mensaje anterior ya se entregó completo, empezamos uno nuevo
    if self.msg_len is not None and self.msg_received == self.msg_len and not self.rec_buff:
      self.msg_len = None
      self.msg_received = 0

    # no sabemos el tam, entonces viene en el primer msje
    if self.msg_len is None:
      while True:
        seg_bytes, addr = self.udp_socket.recvfrom(read_buff_size)
        seg = self.parse_segment(seg_bytes)

        if (seg["SYN"] == 0 and seg["ACK"] == 0 and seg["FIN"] == 0):
          # solo procesa el len si es el que esperamos
          if seg["SEQ"] == self.expected_seq:
            self.msg_len = int.from_bytes(seg["DATOS"], "big")
            self.expected_seq += 1

          # respondemos con ACK que lleva el SEQ del segmento que nos llegó
          ack = self.create_segment({
              "SYN": 0, "ACK": 1, "FIN": 0,
              "SEQ": seg["SEQ"], "DATOS": b""
          })
          self.udp_socket.sendto(ack, self.dest_address)
          break

    recibo = min(self.msg_len - (self.msg_received - len(self.rec_buff)), buff_size)
    while len(self.rec_buff) < recibo and self.msg_received < self.msg_len:
      seg_bytes, addr = self.udp_socket.recvfrom(read_buff_size)
      seg = self.parse_segment(seg_bytes)

      if (seg["SYN"] == 0 and seg["ACK"] == 0 and seg["FIN"] == 0):
        # guarda en buffer solo si el SEQ es el correcto
        if seg["SEQ"] == self.expected_seq:
          self.rec_buff += seg["DATOS"]
          self.msg_received += len(seg["DATOS"])
          self.expected_seq += 1

        # enviar ACK usando el SEQ recibido, sin importar si es nuevo o duplicado
        ack = self.create_segment({
            "SYN": 0, "ACK": 1, "FIN": 0,
            "SEQ": seg["SEQ"], "DATOS": b""
        })
        self.udp_socket.sendto(ack, self.dest_address)

    devolver = self.rec_buff[:buff_size]
    self.rec_buff = self.rec_buff[buff_size:]
    return devolver

  def close(self):
    # fin manejado por A
    buff_size = self.HEADER_SIZE + 16 
    # enviar fin a b
    fin = self.create_segment({
            "SYN": 0, "ACK": 0, "FIN": 1,
            "SEQ": self.seq, "DATOS": b""
      })
    self.udp_socket.settimeout(self.timeout)

    # esperar fin y ack de b
    intentos = 0
    while intentos < 3:
      self.udp_socket.sendto(fin, self.dest_address)
      try:
        seg_bytes, addr = self.udp_socket.recvfrom(buff_size)
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
    fin_final = self.create_segment({
            "SYN": 0, "ACK": 1, "FIN": 0,
            "SEQ": self.seq, "DATOS": b""
      })
    for _ in range(3):
      self.udp_socket.sendto(fin_final, self.dest_address)
      time.sleep(self.timeout)
    self.udp_socket.close()

  def recv_close(self):
    # fin manejado por B
    buff_size = self.HEADER_SIZE + 16 
    # esperar fin de A
    while True:
      seg_bytes, addr = self.udp_socket.recvfrom(buff_size)
      if addr != self.dest_address: continue
      seg = self.parse_segment(seg_bytes)

      if seg["FIN"] == 1 and seg["ACK"] == 0:
        self.seq = seg["SEQ"] + 1
        break
      elif seg["FIN"] == 0 and seg["SYN"] == 0 and seg["ACK"] == 0:
        ack_resend = self.create_segment({
          "SYN": 0, "ACK": 1, "FIN": 0,
          "SEQ": seg["SEQ"], "DATOS": b""
        })
        self.udp_socket.sendto(ack_resend, self.dest_address)

    # enviar fin + ack final
    fin = self.create_segment({
          "SYN": 0, "ACK": 1, "FIN": 1,
          "SEQ": self.seq, "DATOS": b""
      })
    self.udp_socket.settimeout(self.timeout)

    # esperar ack de A
    intentos = 0
    while intentos < 3:
      self.udp_socket.sendto(fin, self.dest_address)
      try:
        seg_bytes, addr = self.udp_socket.recvfrom(buff_size)
        if addr != self.dest_address: continue
        seg = self.parse_segment(seg_bytes)
        if seg["ACK"] == 1 and addr == self.dest_address:
          self.seq = seg["SEQ"] + 1
          break
      except socket.timeout:
        intentos+=1
    
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

  def send_using_go_back_n(self, message):
    # fija el tamaño de ventana
    window_size = 8
    buff_size = self.HEADER_SIZE + 16

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
    for i in range(window_size):
      data = data_window.get_data(i)
      if data is not None:
        seg = self.create_segment({
            "SYN": 0, "ACK": 0, "FIN": 0,
            "SEQ": data_window.get_sequence_number(i), "DATOS": data
            })
        udp.sendto(seg, self.dest_address, timer_index=0)

    # repite hasta que se confirme toda la ventana
    while data_window.get_data(0) is not None:
      try:
        answer, address = udp.recvfrom(buff_size)
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
            # envía los nuevos segmentos de la ventana
            sent_new = False
            for i in range(window_size - steps, window_size):
              data = data_window.get_data(i)
              if data is not None:
                seg_new = self.create_segment({
                    "SYN": 0, "ACK": 0, "FIN": 0,
                    "SEQ": data_window.get_sequence_number(i), "DATOS": data
                    })
                udp.sendto(seg_new, self.dest_address, timer_index=0)
                sent_new = True
            # reenvía el primero para reiniciar el timer si no hubo nuevos
            if not sent_new and data_window.get_data(0) is not None:
              seg_head = self.create_segment({
                  "SYN": 0, "ACK": 0, "FIN": 0,
                  "SEQ": data_window.get_sequence_number(0), "DATOS": data_window.get_data(0)
                  })
              udp.sendto(seg_head, self.dest_address, timer_index=0)
      except TimeoutError:
        # reinicia el timer y reenvía toda la ventana
        udp.stop_timer(timer_index=0)
        for i in range(window_size):
          data = data_window.get_data(i)
          if data is not None:
            seg_re = self.create_segment({
                "SYN": 0, "ACK": 0, "FIN": 0,
                "SEQ": data_window.get_sequence_number(i), "DATOS": data
                })
            udp.sendto(seg_re, self.dest_address, timer_index=0)

    # actualiza el seq con los bytes enviados
    self.seq = initial_seq + sum(len(d) for d in data_list)
    # devuelve el socket a modo bloqueante
    self.udp_socket.settimeout(None)

  def recv_using_go_back_n(self, buff_size):
    read_buff_size = self.HEADER_SIZE + 16
    # reinicia el estado si el mensaje anterior ya se entregó completo
    if self.msg_len is not None and self.msg_received == self.msg_len and not self.rec_buff:
      self.msg_len = None
      self.msg_received = 0

    # espera el segmento con el largo del mensaje
    while self.msg_len is None:
      seg_bytes, addr = self.udp_socket.recvfrom(read_buff_size)
      seg = self.parse_segment(seg_bytes)
      if seg["SYN"] == 0 and seg["ACK"] == 0 and seg["FIN"] == 0:
        # acepta solo el seq esperado
        if seg["SEQ"] == self.expected_seq:
          self.msg_len = int.from_bytes(seg["DATOS"], "big")
          self.expected_seq += len(seg["DATOS"])
        # responde con el siguiente seq esperado
        ack = self.create_segment({
            "SYN": 0, "ACK": 1, "FIN": 0,
            "SEQ": self.expected_seq, "DATOS": b""
            })
        self.udp_socket.sendto(ack, self.dest_address)

    # recibe datos hasta completar lo pedido
    recibo = min(self.msg_len - (self.msg_received - len(self.rec_buff)), buff_size)
    while len(self.rec_buff) < recibo and self.msg_received < self.msg_len:
      seg_bytes, addr = self.udp_socket.recvfrom(read_buff_size)
      seg = self.parse_segment(seg_bytes)
      if seg["SYN"] == 0 and seg["ACK"] == 0 and seg["FIN"] == 0:
        # guarda solo si el seq es el esperado
        if seg["SEQ"] == self.expected_seq:
          self.rec_buff += seg["DATOS"]
          self.msg_received += len(seg["DATOS"])
          self.expected_seq += len(seg["DATOS"])
        # responde con el siguiente seq esperado
        ack = self.create_segment({
            "SYN": 0, "ACK": 1, "FIN": 0,
            "SEQ": self.expected_seq, "DATOS": b""
            })
        self.udp_socket.sendto(ack, self.dest_address)

    # retorna los datos pedidos
    devolver = self.rec_buff[:buff_size]
    self.rec_buff = self.rec_buff[buff_size:]
    return devolver
