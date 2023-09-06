from sqlalchemy import create_engine

conn_str = "sqlite+pysqlite:///test.db"

engine = create_engine(conn_str, echo=True)

print(engine)




with engine.connect() as conn:
    result = conn.execute(text("SELECT x, y FROM some_table WHERE y > :y"), {"y": 2})
    for row in result:
        print(f"x: {row.x}  y: {row.y}")