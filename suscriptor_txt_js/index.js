const amqp = require("amqplib");
const fs = require("fs");
const path = require("path");

const RABBIT_URL = process.env.RABBIT_URL || "amqp://guest:guest@localhost:5672";
const EXCHANGE = "pedidos.exchange";
const QUEUE = "pedidos.txt";
const OUT_FILE = path.resolve(process.env.OUT_FILE || "pedidos.txt");

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

async function conectar() {
  for (;;) {
    try {
      return await amqp.connect(RABBIT_URL);
    } catch (e) {
      console.error(`[RabbitMQ] no disponible (${e.message}), reintentando en 3s...`);
      await sleep(3000);
    }
  }
}

async function main() {
  const conn = await conectar();
  conn.on("error", (e) => console.error("[RabbitMQ] error:", e.message));
  conn.on("close", () => {
    console.error("[RabbitMQ] conexión cerrada, saliendo.");
    process.exit(1);
  });

  const ch = await conn.createChannel();
  await ch.assertExchange(EXCHANGE, "fanout", { durable: true });
  await ch.assertQueue(QUEUE, { durable: true });
  await ch.bindQueue(QUEUE, EXCHANGE, "");
  await ch.prefetch(1);

  console.log(`[txt] Esperando mensajes en "${QUEUE}" -> ${OUT_FILE}`);

  await ch.consume(QUEUE, (msg) => {
    if (!msg) return;
    try {
      fs.appendFileSync(OUT_FILE, Buffer.concat([msg.content, Buffer.from("\n")]));
      ch.ack(msg);
      console.log(`[txt] Mensaje guardado (${msg.content.length} bytes)`);
    } catch (e) {
      console.error("[txt] Error escribiendo archivo:", e.message);
      ch.nack(msg, false, true);
    }
  });

  process.on("SIGINT", async () => {
    await ch.close();
    await conn.close();
    process.exit(0);
  });
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
