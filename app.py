from fastapi import FastAPI
from fastapi_sqlalchemy import DBSessionMiddleware, db
from models import Contribution
from models import Donor
from models import Committee

# from models import Author as Con

from dotenv import dotenv_values

from dotenv import dotenv_values

config = dotenv_values("./.env")
db_password = config.get("DB_PASSWORD")

app = FastAPI()

if db_password:
    app.add_middleware(
        DBSessionMiddleware, db_url=f"postgresql://postgres:{db_password}@/finances"
    )
else:
    app.add_middleware(DBSessionMiddleware, db_url="postgresql://postgres@/finances")


@app.get("/contributions")
async def contributions():
    contributions = db.session.query(Contribution).all()
    return contributions


@app.get("/contributions/{uid}")
async def contribution(uid):
    print(uid)
    contribution = db.session.query(Contribution).get(uid)
    return contribution


@app.get("/committees")
async def contributions():
    contributions = db.session.query(Committee).all()
    return contributions


@app.get("/committees/{uid}")
async def contribution(uid):
    print(uid)
    contribution = db.session.query(Committee).get(uid)
    return contribution


@app.get("/donors")
async def contributions():
    donors = db.session.query(Donor).all()
    return donors


@app.get("/donors/{uid}")
async def contribution(uid):
    print(uid)
    donor = db.session.query(Donor).get(uid)
    return donor
