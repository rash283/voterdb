from sqlalchemy import create_engine, MetaData

meta = MetaData()
engine = create_engine('postgresql+psycopg2://python:python@localhost:5432/voterdb')