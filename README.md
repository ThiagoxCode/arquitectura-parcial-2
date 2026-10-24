# Parcial Arquitectura de Software — Pub/Sub con RabbitMQ

Escenario: **Pizzería La Nonna** toma pedidos en una GUI; cada pedido se publica como JSON
en un exchange de RabbitMQ y llega a dos suscriptores mediante colas independientes.

```
[Publicador Python/Tkinter]
        │  JSON (basic_publish, mandatory + confirms, persistente)
        ▼
[Exchange: pedidos.exchange (fanout, durable)]
        ├──► [Cola: pedidos.bd]  ──► [Suscriptor 1: Rust]       ──► PostgreSQL (campo por campo)
        └──► [Cola: pedidos.txt] ──► [Suscriptor 2: JavaScript] ──► pedidos.txt (JSON crudo)
```

| Componente          | Lenguaje          | Librerías                           |
| ------------------- | ----------------- | ----------------------------------- |
| Publicador (GUI)    | Python            | tkinter, pika                       |
| Suscriptor 1 (BD)   | Rust              | lapin, tokio, tokio-postgres, serde |
| Suscriptor 2 (.txt) | JavaScript (Node) | amqplib                             |

## Requisitos

Docker, Python 3.9+ (con Tkinter), Node 18+, Rust (stable reciente, `rustup update`).

## Ejecución (orden importante)

```bash
# 0. Infraestructura (RabbitMQ + PostgreSQL con las tablas ya creadas)
docker compose up -d
# UI de RabbitMQ: http://localhost:15672 (guest / guest)

# 1. Suscriptor 1 - Rust -> PostgreSQL (terminal 1)
cd suscriptor_bd_rust && cargo run --release

# 2. Suscriptor 2 - Node -> pedidos.txt (terminal 2)
cd suscriptor_txt_js && npm install && npm start

# 3. Publicador - GUI (terminal 3)
cd publicador && pip install -r requirements.txt && python app.py
```

> Los suscriptores van primero: son quienes crean las colas y los bindings. Un fanout sin
> colas enlazadas descarta el mensaje (el publicador lo avisa con `mandatory=True`).

## Formato del mensaje

```json
{
  "pedido_id": "uuid",
  "fecha": "2026-10-22T14:30:00+00:00",
  "cliente": { "nombre": "...", "telefono": "...", "direccion": "..." },
  "items": [
    { "producto": "Pizza Margarita", "cantidad": 2, "precio_unitario": 25000 }
  ],
  "total": 50000
}
```
