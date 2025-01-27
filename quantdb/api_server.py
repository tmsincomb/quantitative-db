from flask_sqlalchemy import SQLAlchemy

from quantdb.api import make_app
from quantdb.utils import setPS1

setPS1(__file__)

db = SQLAlchemy()
app = make_app(db=db)


if __name__ == "__main__":
    app.run(host="127.0.0.1", port=8989, reload=True)
