class CongestionControl:
  """Maneja el control de congestión (slow start y congestion avoidance)
  a partir del MSS, la cwnd, el ssthresh y el estado actual."""

  def __init__(self, MSS: int):
    """Construye el objeto con estado slow start, cwnd de 1 MSS y
    ssthresh sin definir (None)."""
    self.current_state = "slow_start"
    self.MSS = MSS
    # inicializa cwnd en 1 mss (en bytes)
    self.cwnd = MSS
    self.ssthresh = None

  def get_cwnd(self) -> int:
    """Retorna el valor de cwnd en bytes."""
    # retorna la cwnd almacenada
    return self.cwnd

  def get_MSS_in_cwnd(self) -> int:
    """Retorna cuántos MSS completos caben en cwnd."""
    # retorna la división entera entre cwnd y mss
    return int(self.cwnd // self.MSS)

  def event_ack_received(self):
    """Maneja la recepción de un ACK: aumenta cwnd según el estado y
    cambia a congestion avoidance si cwnd >= ssthresh."""
    if self.is_state_slow_start():
      # aumenta cwnd en 1 mss
      self.cwnd += self.MSS
      # cambia de estado si cwnd alcanza ssthresh
      if self.ssthresh is not None and self.cwnd >= self.ssthresh:
        self.current_state = "congestion_avoidance"
    elif self.is_state_congestion_avoidance():
      # aumenta cwnd en 1/(mss en cwnd) de mss
      self.cwnd += self.MSS * (1 / self.get_MSS_in_cwnd())

  def event_timeout(self):
    """Maneja un timeout: define ssthresh según corresponda, reinicia
    cwnd a 1 MSS y vuelve a slow start."""
    if self.is_state_slow_start():
      # define ssthresh solo la primera vez
      if self.ssthresh is None:
        self.ssthresh = int(self.cwnd / 2)
    else:
      # define ssthresh en congestion avoidance
      self.ssthresh = int(self.cwnd / 2)
    # reinicia cwnd a 1 mss
    self.cwnd = self.MSS
    # vuelve a slow start
    self.current_state = "slow_start"

  def is_state_slow_start(self) -> bool:
    """Retorna True si el estado actual es slow start."""
    # compara el estado actual
    return self.current_state == "slow_start"

  def is_state_congestion_avoidance(self) -> bool:
    """Retorna True si el estado actual es congestion avoidance."""
    # compara el estado actual
    return self.current_state == "congestion_avoidance"

  def get_ssthresh(self):
    """Retorna el valor almacenado de ssthresh."""
    # retorna ssthresh (None si no está definido)
    return self.ssthresh
