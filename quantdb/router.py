from typing import Literal

import uvicorn
from fastapi import FastAPI
from fastapi.middleware.wsgi import WSGIMiddleware

from quantdb.api_server import app as quantdb_flask_app

app = FastAPI()
app.mount("/quantdb", WSGIMiddleware(quantdb_flask_app))


# Root URL
@app.get("/")
def index() -> Literal["Hello"]:
    return "Hello"


# Using FastAPI instance
@app.get("/url-list")
def get_all_urls():
    url_list = [{"path": route.path, "name": route.name} for route in app.routes]
    return url_list


if __name__ == "__main__":
    uvicorn.run("router:app", host="localhost", port=8000, reload=True)
