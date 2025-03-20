from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.orm import Session
from app.models.userschema import table_registry
from sqlalchemy.ext.asyncio import create_async_engine
import os

load_dotenv()

DATABASE_URL = os.getenv('DATABASE_URL_UNPOOL')
print(DATABASE_URL)

engine = create_engine(DATABASE_URL,echo=True,) 
table_registry.metadata.create_all(engine)


def getSession():
    with Session(engine) as session:
        return session

# Session = sessionmaker(bind=engine)
# session = Session()

# Base = declarative_base()
# Base.metadata.create_all(bind=engine)