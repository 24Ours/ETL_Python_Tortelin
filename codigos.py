import pyodbc
from sqlalchemy import create_engine
import pandas as pd
from app import config

try:
    engine = create_engine("mssql+pyodbc://sa:kali21@DESKTOP-KLRPEIV/dbtortelin?driver=ODBC+Driver+17+for+SQL+Server")
    print("conexión establecida")
except Exception as ex:
    print(ex)

codigos = pd.read_sql_query("SELECT CONCAT(SUBSTRING(sabor,1,3),SUBSTRING(REVERSE(sabor),1,3),SUBSTRING(tamanio,1,3)) as codigo, sabor, tamanio from (select distinct sabor, tamanio from tabla_provisional) as fr;", engine)
codigos = codigos.values.tolist()

server = config['server']
bd = config['bd']
usuario = config['usuario']
contrasena = config['contrasena']


try:
    conexion = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL server}; SERVER='+server+'; DATABASE='+bd+'; UID='+usuario+';PWD='+contrasena)
    cur = conexion.cursor()
    print("conexion exitosa")
except Exception as ex:
    print(ex)

for codigo, sabor, tamaño in codigos:
    print(codigo, sabor, tamaño)
    cur.execute("UPDATE registro_tortelin SET codigo = ? WHERE sabor = ? and tamanio = ?;", codigo, sabor, tamaño)
    conexion.commit()

