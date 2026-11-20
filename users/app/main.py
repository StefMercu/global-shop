from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

app = FastAPI(title="Users service", version="1.0.0")


class UserIn(BaseModel):
    name: str
    email: str


class User(UserIn):
    id: int


_users: dict[int, User] = {}
_next_id = 1


@app.get("/health")
def health():
    return {"status": "ok"}


@app.post("/users", response_model=User, status_code=201)
def create_user(data: UserIn):
    global _next_id
    user = User(id=_next_id, **data.model_dump())
    _users[user.id] = user
    _next_id += 1
    return user


@app.get("/users", response_model=list[User])
def list_users():
    return list(_users.values())


@app.get("/users/{user_id}", response_model=User)
def get_user(user_id: int):
    if user_id not in _users:
        raise HTTPException(status_code=404, detail="User not found")
    return _users[user_id]