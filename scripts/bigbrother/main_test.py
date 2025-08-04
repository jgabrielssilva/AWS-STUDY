from datetime import date
from db_insert import conectar, insert_obras

conn, cursor = conectar()

insert_obras(cursor, conn, "OBRA-002", "Ipojuca, PE", date(2026, 8, 1), date(2026, 11, 7))

cursor.close()
conn.close()
