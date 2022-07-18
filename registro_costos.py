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

cur.execute("""IF OBJECT_ID('REGISTRO_COSTOS') IS NOT NULL
BEGIN
DROP PROC REGISTRO_COSTOS
END
GO
CREATE PROCEDURE REGISTRO_COSTOS
AS
BEGIN
update registro_tortelin set costo = 37 where sabor = 'CHOCOLATE' and tamanio = 'SUPERGRANDE';
update registro_tortelin set costo = 38 where sabor = 'COCTEL DE FRUTAS' and tamanio = 'SUPERGRANDE';
update registro_tortelin set costo = 50 where sabor = 'DULCE FANTASIA' and tamanio = 'SUPERGRANDE';
update registro_tortelin set costo = 38 where sabor = 'FRESA CON DURAZNO' and tamanio = 'SUPERGRANDE';
update registro_tortelin set costo = 35 where sabor = 'GUANABANA' and tamanio = 'SUPERGRANDE';
update registro_tortelin set costo = 39 where sabor = 'LUCUMA' and tamanio = 'SUPERGRANDE';
update registro_tortelin set costo = 38 where sabor = 'MANGO' and tamanio = 'SUPERGRANDE';
update registro_tortelin set costo = 37 where sabor = 'MARACUYA' and tamanio = 'SUPERGRANDE';
update registro_tortelin set costo = 38 where sabor = 'MOKA' and tamanio = 'SUPERGRANDE';
update registro_tortelin set costo = 36 where sabor = 'SAUCO' and tamanio = 'SUPERGRANDE';
update registro_tortelin set costo = 37 where sabor = 'SELVA NEGRA' and tamanio = 'SUPERGRANDE';
update registro_tortelin set costo = 40 where sabor = 'TRES LECHES DE LUCUMA' and tamanio = 'SUPERGRANDE';
update registro_tortelin set costo = 41 where sabor = 'TRES LECHES DE VAINILLA' and tamanio = 'SUPERGRANDE';
update registro_tortelin set costo = 39 where sabor = 'SELVA BLANCA' and tamanio = 'SUPERGRANDE';
update registro_tortelin set costo = 24 where sabor = 'CHOCOFE' and tamanio = 'GRANDE';
update registro_tortelin set costo = 23 where sabor = 'CHOCOFRESA' and tamanio = 'GRANDE';
update registro_tortelin set costo = 30 where sabor = 'CHOCOLATE' and tamanio = 'GRANDE';
update registro_tortelin set costo = 31 where sabor = 'COCO' and tamanio = 'GRANDE';
update registro_tortelin set costo = 33 where sabor = 'COCTEL DE FRUTAS' and tamanio = 'GRANDE';
update registro_tortelin set costo = 45 where sabor = 'DULCE FANTASIA' and tamanio = 'GRANDE';
update registro_tortelin set costo = 32 where sabor = 'FRESA CON DURAZNO' and tamanio = 'GRANDE';
update registro_tortelin set costo = 30 where sabor = 'GUANABANA' and tamanio = 'GRANDE';
update registro_tortelin set costo = 31 where sabor = 'LUCUMA' and tamanio = 'GRANDE';
update registro_tortelin set costo = 25 where sabor = 'MANJAR' and tamanio = 'GRANDE';
update registro_tortelin set costo = 29 where sabor = 'MARACUYA' and tamanio = 'GRANDE';
update registro_tortelin set costo = 32 where sabor = 'MENTA' and tamanio = 'GRANDE';
update registro_tortelin set costo = 30 where sabor = 'MOKA' and tamanio = 'GRANDE';
update registro_tortelin set costo = 31 where sabor = 'RED VELVET' and tamanio = 'GRANDE';
update registro_tortelin set costo = 30 where sabor = 'SAUCO' and tamanio = 'GRANDE';
update registro_tortelin set costo = 30 where sabor = 'SELVA BLANCA' and tamanio = 'GRANDE';
update registro_tortelin set costo = 32 where sabor = 'SELVA NEGRA' and tamanio = 'GRANDE';
update registro_tortelin set costo = 31 where sabor = 'TORTA HELADA' and tamanio = 'GRANDE';
update registro_tortelin set costo = 42 where sabor = 'TRES LECHES DE CHOCOLATE' and tamanio = 'GRANDE';
update registro_tortelin set costo = 41 where sabor = 'TRES LECHES DE COCO' and tamanio = 'GRANDE';
update registro_tortelin set costo = 40 where sabor = 'TRES LECHES DE LUCUMA' and tamanio = 'GRANDE';
update registro_tortelin set costo = 42 where sabor = 'TRES LECHES DE MANGO' and tamanio = 'GRANDE';
update registro_tortelin set costo = 41 where sabor = 'TRES LECHES DE MARACUYA' and tamanio = 'GRANDE';
update registro_tortelin set costo = 39 where sabor = 'TRES LECHES DE MOKA' and tamanio = 'GRANDE';
update registro_tortelin set costo = 36 where sabor = 'TRES LECHES DE VAINILLA' and tamanio = 'GRANDE';
update registro_tortelin set costo = 25 where sabor = 'VANIFRESA' and tamanio = 'GRANDE';
update registro_tortelin set costo = 18 where sabor = 'CHOCOFE' and tamanio = 'MEDIANA';
update registro_tortelin set costo = 17 where sabor = 'CHOCOFRESA' and tamanio = 'MEDIANA';
update registro_tortelin set costo = 22 where sabor = 'CHOCOLATE' and tamanio = 'MEDIANA';
update registro_tortelin set costo = 20 where sabor = 'COCO' and tamanio = 'MEDIANA';
update registro_tortelin set costo = 23 where sabor = 'COCTEL DE FRUTAS' and tamanio = 'MEDIANA';
update registro_tortelin set costo = 36 where sabor = 'DULCE FANTASIA' and tamanio = 'MEDIANA';
update registro_tortelin set costo = 24 where sabor = 'FRESA CON DURAZNO' and tamanio = 'MEDIANA';
update registro_tortelin set costo = 20 where sabor = 'GUANABANA' and tamanio = 'MEDIANA';
update registro_tortelin set costo = 23 where sabor = 'LUCUMA' and tamanio = 'MEDIANA';
update registro_tortelin set costo = 22 where sabor = 'MANGO' and tamanio = 'MEDIANA';
update registro_tortelin set costo = 18 where sabor = 'MANJAR' and tamanio = 'MEDIANA';
update registro_tortelin set costo = 24 where sabor = 'MARACUYA' and tamanio = 'MEDIANA';
update registro_tortelin set costo = 20 where sabor = 'MENTA' and tamanio = 'MEDIANA';
update registro_tortelin set costo = 22 where sabor = 'MOKA' and tamanio = 'MEDIANA';
update registro_tortelin set costo = 25 where sabor = 'RED VELVET' and tamanio = 'MEDIANA';
update registro_tortelin set costo = 23 where sabor = 'SAUCO' and tamanio = 'MEDIANA';
update registro_tortelin set costo = 23 where sabor = 'SELVA BLANCA' and tamanio = 'MEDIANA';
update registro_tortelin set costo = 22 where sabor = 'SELVA NEGRA' and tamanio = 'MEDIANA';
update registro_tortelin set costo = 21 where sabor = 'TORTA HELADA' and tamanio = 'MEDIANA';
update registro_tortelin set costo = 25 where sabor = 'TRES LECHES DE CHOCOLATE' and tamanio = 'MEDIANA';
update registro_tortelin set costo = 27 where sabor = 'TRES LECHES DE LUCUMA' and tamanio = 'MEDIANA';
update registro_tortelin set costo = 26 where sabor = 'TRES LECHES DE MANGO' and tamanio = 'MEDIANA';
update registro_tortelin set costo = 28 where sabor = 'TRES LECHES DE MARACUYA' and tamanio = 'MEDIANA';
update registro_tortelin set costo = 29 where sabor = 'TRES LECHES DE MOKA' and tamanio = 'MEDIANA';
update registro_tortelin set costo = 25 where sabor = 'TRES LECHES DE VAINILLA' and tamanio = 'MEDIANA';
update registro_tortelin set costo = 19 where sabor = 'VANIFRESA' and tamanio = 'MEDIANA';
update registro_tortelin set costo = 17 where sabor = 'CHOCOLATE' and tamanio = 'PEQUEÑA';
update registro_tortelin set costo = 14 where sabor = 'COCO' and tamanio = 'PEQUEÑA';
update registro_tortelin set costo = 15 where sabor = 'COCTEL DE FRUTAS' and tamanio = 'PEQUEÑA';
update registro_tortelin set costo = 29 where sabor = 'DULCE FANTASIA' and tamanio = 'PEQUEÑA';
update registro_tortelin set costo = 16 where sabor = 'FRESA CON DURAZNO' and tamanio = 'PEQUEÑA';
update registro_tortelin set costo = 12 where sabor = 'GUANABANA' and tamanio = 'PEQUEÑA';
update registro_tortelin set costo = 15 where sabor = 'LUCUMA' and tamanio = 'PEQUEÑA';
update registro_tortelin set costo = 17 where sabor = 'MANGO' and tamanio = 'PEQUEÑA';
update registro_tortelin set costo = 16 where sabor = 'MARACUYA' and tamanio = 'PEQUEÑA';
update registro_tortelin set costo = 16 where sabor = 'MENTA' and tamanio = 'PEQUEÑA';
update registro_tortelin set costo = 15 where sabor = 'MOKA' and tamanio = 'PEQUEÑA';
update registro_tortelin set costo = 15 where sabor = 'RED VELVET' and tamanio = 'PEQUEÑA';
update registro_tortelin set costo = 17 where sabor = 'SAUCO' and tamanio = 'PEQUEÑA';
update registro_tortelin set costo = 16 where sabor = 'SELVA BLANCA' and tamanio = 'PEQUEÑA';
update registro_tortelin set costo = 16 where sabor = 'SELVA NEGRA' and tamanio = 'PEQUEÑA';
update registro_tortelin set costo = 14 where sabor = 'TORTA HELADA' and tamanio = 'PEQUEÑA';
update registro_tortelin set costo = 22 where sabor = 'TRES LECHES DE CHOCOLATE' and tamanio = 'PEQUEÑA';
update registro_tortelin set costo = 21 where sabor = 'TRES LECHES DE LUCUMA' and tamanio = 'PEQUEÑA';
update registro_tortelin set costo = 22 where sabor = 'TRES LECHES DE MANGO' and tamanio = 'PEQUEÑA';
update registro_tortelin set costo = 21 where sabor = 'TRES LECHES DE MARACUYA' and tamanio = 'PEQUEÑA';
update registro_tortelin set costo = 20 where sabor = 'TRES LECHES DE MOKA' and tamanio = 'PEQUEÑA';
update registro_tortelin set costo = 18 where sabor = 'TRES LECHES DE VAINILLA' and tamanio = 'PEQUEÑA';
END 
GO""")
conexion.commit()

cur.execute("EXEC REGISTRO_COSTOS;")
conexion.commit()