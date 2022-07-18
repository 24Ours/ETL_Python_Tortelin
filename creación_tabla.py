import pyodbc
from sqlalchemy import create_engine
import pandas as pd
from app import config

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

cur.execute("""create table registro_tortelin(
	codigo varchar(100),
	sabor varchar(100),
	tamanio varchar(100),
	unidades_vendidas integer,
	unidades_desechadas integer,
	precio_dia integer,
	dia integer,
	semana integer,
	mes integer,
	anio integer,
	fecha varchar(100),
	dia_nombre varchar(100),
	nombre_local varchar(100),
	distrito varchar(100),
	departamento varchar(100),
	costo integer
);""")
conexion.commit()