# Instrucciones de ejecución

El código implementa una versión simplificada de TCP con Go-Back-N y control de congestión (TCP Tahoe) sobre UDP.

## 1. Pruebas unitarias de CongestionControl
Para verificar el funcionamiento y las transiciones de estado de la clase de control de congestión, ejecute en la terminal lo siguiente:
```bash
python3 CongestionControl_test.py
```

## 2. Pruebas de transferencia (Go-Back-N con y sin CC)
Para probar el envío y recepción de mensajes, se deben abrir dos terminales:

### Mensaje corto (50 bytes)
1. Terminal 1 (Servidor):
```bash
python3 test_server.py
```
2. Terminal 2 (Cliente):
```bash
python3 test_cliente.py
```

### Mensaje largo (320 bytes)
1. Terminal 1 (Servidor):
```bash
python3 test_server_largo.py
```
2. Terminal 2 (Cliente):
```bash
python3 test_cliente_largo.py
```

*Nota: Asegurarse de que la variable `server_address` en los scripts del cliente y servidor esté configurada con la IP y puerto correspondientes al entorno de su ejecución, para ello modifíquela con su información.*


## 4. Simulación de pérdidas con netem
Para evaluar el comportamiento ante pérdidas en la interfaz local (`lo`):

- Activar 20% de pérdida:
```bash
sudo tc qdisc add dev lo root netem loss 20%
```

- Repetir el paso 2.

- Eliminar la regla de pérdida al finalizar:
```bash
sudo tc qdisc del dev lo root
```
