CREATE TABLE IF NOT EXISTS pedidos (
    pedido_id          UUID PRIMARY KEY,
    fecha              TIMESTAMPTZ NOT NULL,
    cliente_nombre     TEXT        NOT NULL,
    cliente_telefono   TEXT        NOT NULL,
    cliente_direccion  TEXT        NOT NULL,
    total              BIGINT      NOT NULL,
    recibido_en        TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS pedido_items (
    id               SERIAL PRIMARY KEY,
    pedido_id        UUID   NOT NULL REFERENCES pedidos(pedido_id) ON DELETE CASCADE,
    producto         TEXT   NOT NULL,
    cantidad         INT    NOT NULL CHECK (cantidad > 0),
    precio_unitario  BIGINT NOT NULL,
    subtotal         BIGINT NOT NULL
);
