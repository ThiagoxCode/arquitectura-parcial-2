use std::{env, time::Duration};

use chrono::{DateTime, Utc};
use futures_lite::stream::StreamExt;
use lapin::{options::*, types::FieldTable, Connection, ConnectionProperties, ExchangeKind};
use serde::Deserialize;
use tokio_postgres::{Client, NoTls};
use uuid::Uuid;

const EXCHANGE: &str = "pedidos.exchange";
const QUEUE: &str = "pedidos.bd";

#[derive(Debug, Deserialize)]
struct Cliente {
    nombre: String,
    telefono: String,
    direccion: String,
}

#[derive(Debug, Deserialize)]
struct Item {
    producto: String,
    cantidad: i32,
    precio_unitario: i64,
}

#[derive(Debug, Deserialize)]
struct Pedido {
    pedido_id: Uuid,
    fecha: DateTime<Utc>,
    cliente: Cliente,
    items: Vec<Item>,
    total: i64,
}

async fn conectar_db(url: &str) -> Result<Client, tokio_postgres::Error> {
    let (client, connection) = tokio_postgres::connect(url, NoTls).await?;
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("[BD] conexión cerrada: {e}");
        }
    });
    Ok(client)
}

async fn conectar_db_con_reintentos(url: &str) -> Client {
    loop {
        match conectar_db(url).await {
            Ok(c) => return c,
            Err(e) => {
                eprintln!("[BD] no disponible ({e}), reintentando en 3s...");
                tokio::time::sleep(Duration::from_secs(3)).await;
            }
        }
    }
}

async fn conectar_rabbit(url: &str) -> Connection {
    loop {
        match Connection::connect(url, ConnectionProperties::default()).await {
            Ok(c) => return c,
            Err(e) => {
                eprintln!("[RabbitMQ] no disponible ({e}), reintentando en 3s...");
                tokio::time::sleep(Duration::from_secs(3)).await;
            }
        }
    }
}

/// Inserta pedido + ítems en una transacción. Idempotente por pedido_id.
/// Devuelve false si el pedido ya existía (reentrega).
async fn guardar(client: &mut Client, p: &Pedido) -> Result<bool, tokio_postgres::Error> {
    let tx = client.transaction().await?;
    let n = tx
        .execute(
            "INSERT INTO pedidos (pedido_id, fecha, cliente_nombre, cliente_telefono, cliente_direccion, total) \
             VALUES ($1, $2, $3, $4, $5, $6) ON CONFLICT (pedido_id) DO NOTHING",
            &[
                &p.pedido_id,
                &p.fecha,
                &p.cliente.nombre,
                &p.cliente.telefono,
                &p.cliente.direccion,
                &p.total,
            ],
        )
        .await?;
    if n == 0 {
        tx.rollback().await?;
        return Ok(false);
    }
    for it in &p.items {
        let subtotal: i64 = it.cantidad as i64 * it.precio_unitario;
        tx.execute(
            "INSERT INTO pedido_items (pedido_id, producto, cantidad, precio_unitario, subtotal) \
             VALUES ($1, $2, $3, $4, $5)",
            &[
                &p.pedido_id,
                &it.producto,
                &it.cantidad,
                &it.precio_unitario,
                &subtotal,
            ],
        )
        .await?;
    }
    tx.commit().await?;
    Ok(true)
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let amqp_url =
        env::var("RABBIT_URL").unwrap_or_else(|_| "amqp://guest:guest@localhost:5672/%2f".into());
    let db_url = env::var("DATABASE_URL")
        .unwrap_or_else(|_| "host=localhost user=postgres password=postgres dbname=pedidos".into());

    let mut db = conectar_db_con_reintentos(&db_url).await;
    let conn = conectar_rabbit(&amqp_url).await;
    let channel = conn.create_channel().await?;

    channel
        .exchange_declare(
            EXCHANGE,
            ExchangeKind::Fanout,
            ExchangeDeclareOptions {
                durable: true,
                ..Default::default()
            },
            FieldTable::default(),
        )
        .await?;
    channel
        .queue_declare(
            QUEUE,
            QueueDeclareOptions {
                durable: true,
                ..Default::default()
            },
            FieldTable::default(),
        )
        .await?;
    channel
        .queue_bind(
            QUEUE,
            EXCHANGE,
            "",
            QueueBindOptions::default(),
            FieldTable::default(),
        )
        .await?;
    channel.basic_qos(1, BasicQosOptions::default()).await?;

    let mut consumer = channel
        .basic_consume(
            QUEUE,
            "suscriptor-bd-rust",
            BasicConsumeOptions::default(),
            FieldTable::default(),
        )
        .await?;

    println!("[bd] Esperando mensajes en \"{QUEUE}\"...");

    while let Some(delivery) = consumer.next().await {
        let delivery = delivery?;

        let pedido: Pedido = match serde_json::from_slice(&delivery.data) {
            Ok(p) => p,
            Err(e) => {
                // Mensaje malformado: reencolarlo lo dejaría en bucle infinito.
                eprintln!("[bd] JSON inválido, descartado: {e}");
                delivery
                    .nack(BasicNackOptions {
                        requeue: false,
                        ..Default::default()
                    })
                    .await?;
                continue;
            }
        };

        if db.is_closed() {
            match conectar_db(&db_url).await {
                Ok(c) => db = c,
                Err(e) => {
                    eprintln!("[bd] BD caída ({e}), mensaje devuelto a la cola");
                    delivery
                        .nack(BasicNackOptions {
                            requeue: true,
                            ..Default::default()
                        })
                        .await?;
                    tokio::time::sleep(Duration::from_secs(3)).await;
                    continue;
                }
            }
        }

        match guardar(&mut db, &pedido).await {
            Ok(nuevo) => {
                delivery.ack(BasicAckOptions::default()).await?;
                if nuevo {
                    println!(
                        "[bd] Pedido {} guardado ({} ítems, total ${}) - cliente: {}",
                        pedido.pedido_id,
                        pedido.items.len(),
                        pedido.total,
                        pedido.cliente.nombre
                    );
                } else {
                    println!(
                        "[bd] Pedido {} ya existía (reentrega), ignorado",
                        pedido.pedido_id
                    );
                }
            }
            Err(e) => {
                eprintln!("[bd] Error guardando pedido {}: {e}", pedido.pedido_id);
                delivery
                    .nack(BasicNackOptions {
                        requeue: true,
                        ..Default::default()
                    })
                    .await?;
                tokio::time::sleep(Duration::from_secs(3)).await;
            }
        }
    }
    Ok(())
}
