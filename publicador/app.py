"""Publicador de pedidos - Pizzería La Nonna (Tkinter + pika)."""
import json
import os
import re
import tkinter as tk
import uuid
from datetime import datetime, timezone
from tkinter import messagebox, ttk

import pika
from pika.exceptions import AMQPError, NackError, UnroutableError

RABBIT_URL = os.getenv("RABBIT_URL", "amqp://guest:guest@localhost:5672/%2F")
EXCHANGE = "pedidos.exchange"

MENU = {
    "Pizza Margarita": 25000,
    "Pizza Pepperoni": 29000,
    "Pizza Hawaiana": 28000,
    "Pizza Cuatro Quesos": 32000,
    "Lasaña de Carne": 24000,
    "Pan de Ajo": 9000,
    "Gaseosa 1.5L": 7000,
    "Limonada Natural": 6000,
}


def publicar(pedido: dict) -> None:
    """Publica el pedido como JSON en el exchange (fanout, durable) con confirmación."""
    conn = pika.BlockingConnection(pika.URLParameters(RABBIT_URL))
    try:
        ch = conn.channel()
        ch.exchange_declare(exchange=EXCHANGE, exchange_type="fanout", durable=True)
        ch.confirm_delivery()
        cuerpo = json.dumps(pedido, ensure_ascii=False, separators=(",", ":"))
        ch.basic_publish(
            exchange=EXCHANGE,
            routing_key="",
            body=cuerpo.encode("utf-8"),
            properties=pika.BasicProperties(
                content_type="application/json",
                delivery_mode=2,
                message_id=pedido["pedido_id"],
            ),
            mandatory=True,
        )
    finally:
        conn.close()


class App(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title("Pizzería La Nonna - Toma de pedidos")
        self.resizable(False, False)
        self.items = []
        self._build()

    def _build(self):

        cli = ttk.LabelFrame(self, text="Datos del cliente")
        cli.grid(row=0, column=0, sticky="ew", padx=10, pady=(10, 4))
        self.v_nombre, self.v_tel, self.v_dir = tk.StringVar(), tk.StringVar(), tk.StringVar()
        for i, (txt, var) in enumerate(
            [("Nombre:", self.v_nombre), ("Teléfono:", self.v_tel), ("Dirección:", self.v_dir)]
        ):
            ttk.Label(cli, text=txt).grid(row=i, column=0, sticky="w", padx=8, pady=4)
            ttk.Entry(cli, textvariable=var, width=42).grid(row=i, column=1, padx=8, pady=4)

        prod = ttk.LabelFrame(self, text="Agregar producto")
        prod.grid(row=1, column=0, sticky="ew", padx=10, pady=4)
        self.v_prod = tk.StringVar(value=next(iter(MENU)))
        self.v_cant = tk.StringVar(value="1")
        ttk.Label(prod, text="Producto:").grid(row=0, column=0, sticky="w", padx=8, pady=4)
        cb = ttk.Combobox(prod, textvariable=self.v_prod, values=list(MENU), state="readonly", width=24)
        cb.grid(row=0, column=1, padx=8, pady=4)
        cb.bind("<<ComboboxSelected>>", lambda _e: self._actualizar_precio())
        self.lbl_precio = ttk.Label(prod, text="")
        self.lbl_precio.grid(row=0, column=2, padx=8, pady=4)
        ttk.Label(prod, text="Cantidad:").grid(row=1, column=0, sticky="w", padx=8, pady=4)
        ttk.Spinbox(prod, from_=1, to=50, textvariable=self.v_cant, width=6).grid(row=1, column=1, sticky="w", padx=8, pady=4)
        ttk.Button(prod, text="Agregar al pedido", command=self._agregar).grid(row=1, column=2, padx=8, pady=4)
        self._actualizar_precio()

        det = ttk.LabelFrame(self, text="Detalle del pedido")
        det.grid(row=2, column=0, sticky="ew", padx=10, pady=4)
        self.tree = ttk.Treeview(det, columns=("p", "c", "u", "s"), show="headings", height=6)
        for col, txt, w in [("p", "Producto", 190), ("c", "Cant.", 50), ("u", "Precio u.", 90), ("s", "Subtotal", 100)]:
            self.tree.heading(col, text=txt)
            self.tree.column(col, width=w, anchor="w" if col == "p" else "e")
        self.tree.grid(row=0, column=0, columnspan=2, padx=8, pady=4)
        ttk.Button(det, text="Quitar seleccionado", command=self._quitar).grid(row=1, column=0, sticky="w", padx=8, pady=4)
        self.lbl_total = ttk.Label(det, text="Total: $0", font=("TkDefaultFont", 11, "bold"))
        self.lbl_total.grid(row=1, column=1, sticky="e", padx=8, pady=4)

        bar = ttk.Frame(self)
        bar.grid(row=3, column=0, sticky="ew", padx=10, pady=(4, 10))
        ttk.Button(bar, text="Enviar pedido", command=self._enviar).pack(side="right", padx=4)
        ttk.Button(bar, text="Limpiar", command=self._limpiar).pack(side="right", padx=4)
        self.lbl_estado = ttk.Label(bar, text="Listo.")
        self.lbl_estado.pack(side="left")

    def _actualizar_precio(self):
        self.lbl_precio.config(text=f"${MENU[self.v_prod.get()]:,}")

    def _total(self):
        return sum(i["cantidad"] * i["precio_unitario"] for i in self.items)

    def _refrescar(self):
        self.tree.delete(*self.tree.get_children())
        for it in self.items:
            self.tree.insert("", "end", values=(
                it["producto"], it["cantidad"], f"${it['precio_unitario']:,}",
                f"${it['cantidad'] * it['precio_unitario']:,}",
            ))
        self.lbl_total.config(text=f"Total: ${self._total():,}")

    def _agregar(self):
        try:
            cant = int(self.v_cant.get())
        except ValueError:
            return messagebox.showerror("Cantidad inválida", "La cantidad debe ser un número entero.")
        if not 1 <= cant <= 50:
            return messagebox.showerror("Cantidad inválida", "La cantidad debe estar entre 1 y 50.")
        prod = self.v_prod.get()
        for it in self.items:
            if it["producto"] == prod:
                it["cantidad"] += cant
                break
        else:
            self.items.append({"producto": prod, "cantidad": cant, "precio_unitario": MENU[prod]})
        self.v_cant.set("1")
        self._refrescar()

    def _quitar(self):
        sel = self.tree.selection()
        if not sel:
            return
        del self.items[self.tree.index(sel[0])]
        self._refrescar()

    def _limpiar(self):
        self.v_nombre.set("")
        self.v_tel.set("")
        self.v_dir.set("")
        self.items.clear()
        self._refrescar()

    def _validar(self):
        if not self.v_nombre.get().strip():
            return "El nombre es obligatorio."
        if not re.fullmatch(r"\d{7,10}", self.v_tel.get().strip()):
            return "El teléfono debe tener entre 7 y 10 dígitos."
        if not self.v_dir.get().strip():
            return "La dirección es obligatoria."
        if not self.items:
            return "Agrega al menos un producto al pedido."
        return None

    def _enviar(self):
        error = self._validar()
        if error:
            return messagebox.showerror("Pedido inválido", error)
        pedido = {
            "pedido_id": str(uuid.uuid4()),
            "fecha": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            "cliente": {
                "nombre": self.v_nombre.get().strip(),
                "telefono": self.v_tel.get().strip(),
                "direccion": self.v_dir.get().strip(),
            },
            "items": [dict(i) for i in self.items],
            "total": self._total(),
        }
        try:
            publicar(pedido)
        except UnroutableError:
            return messagebox.showerror("Sin destino", "El exchange no tiene colas enlazadas. Inicia los suscriptores primero.")
        except NackError:
            return messagebox.showerror("Rechazado", "RabbitMQ no confirmó el mensaje.")
        except AMQPError as e:
            return messagebox.showerror("Error de RabbitMQ", f"No se pudo publicar:\n{e}")
        self.lbl_estado.config(text=f"Enviado: {pedido['pedido_id'][:8]}…")
        messagebox.showinfo("Pedido enviado", f"Pedido {pedido['pedido_id']}\nTotal: ${pedido['total']:,}")
        self._limpiar()


if __name__ == "__main__":
    App().mainloop()
