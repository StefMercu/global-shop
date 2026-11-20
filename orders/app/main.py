from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

app = FastAPI(title="Orders service", version="1.0.0")


class OrderIn(BaseModel):
    user_id: int
    item: str
    quantity: int = Field(gt=0)


class Order(OrderIn):
    id: int
    status: str = "created"


_orders: dict[int, Order] = {}
_next_id = 1


@app.get("/health")
def health():
    return {"status": "ok"}


@app.post("/orders", response_model=Order, status_code=201)
def create_order(data: OrderIn):
    global _next_id
    order = Order(id=_next_id, **data.model_dump())
    _orders[order.id] = order
    _next_id += 1
    return order


@app.get("/orders", response_model=list[Order])
def list_orders():
    return list(_orders.values())


@app.get("/orders/{order_id}", response_model=Order)
def get_order(order_id: int):
    if order_id not in _orders:
        raise HTTPException(status_code=404, detail="Order not found")
    return _orders[order_id]