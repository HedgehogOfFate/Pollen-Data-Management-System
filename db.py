import sqlalchemy as sa

def get_engine():
    return sa.create_engine('postgresql+psycopg2://user:password@localhost/dbname')

