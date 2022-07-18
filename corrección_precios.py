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

cur.execute("""IF OBJECT_ID('MODIFICAR_PRECIOS') IS NOT NULL
BEGIN
DROP PROC MODIFICAR_PRECIOS
END
GO
CREATE PROCEDURE MODIFICAR_PRECIOS
AS
BEGIN
update registro_tortelin set precio_dia = 40 
where sabor = 'TRES LECHES DE CHOCOLATE' and tamanio = 'MEDIANA' and nombre_local = 'Vista Alegre' and precio_dia = 0;
update registro_tortelin set precio_dia = 28
where sabor = 'SELVA NEGRA' and tamanio = 'PEQUEÑA' and nombre_local = 'Vista Alegre' and precio_dia = 0;
update registro_tortelin set precio_dia = 45
where sabor = 'TRES LECHES DE MANGO' and tamanio = 'MEDIANA' and nombre_local = 'Santa Anita' and precio_dia = 0;
update registro_tortelin set precio_dia = 40
where sabor = 'TRES LECHES DE CHOCOLATE' and tamanio = 'MEDIANA' and nombre_local = 'Santa Anita' and precio_dia = 0;
update registro_tortelin set precio_dia = 26
where sabor = 'GUANABANA' and tamanio = 'PEQUEÑA' and nombre_local = 'Santa Anita' and precio_dia = 0;
update registro_tortelin set precio_dia = 25
where sabor = 'TORTA HELADA' and tamanio = 'PEQUEÑA' and nombre_local = 'Santa Anita' and precio_dia = 0;
update registro_tortelin set precio_dia = 30
where sabor = 'TRES LECHES DE MARACUYA' and tamanio = 'PEQUEÑA' and nombre_local = 'Santa Anita' and precio_dia = 0;
update registro_tortelin set precio_dia = 55
where sabor = 'TRES LECHES DE MARACUYA' and tamanio = 'GRANDE' and nombre_local = 'Virreyes' and precio_dia = 0;
update registro_tortelin set precio_dia = 33
where sabor = 'TRES LECHES DE MARACUYA' and tamanio = 'PEQUEÑA' and nombre_local = 'Virreyes' and precio_dia = 0;
update registro_tortelin set precio_dia = 43
where sabor = 'DULCE FANTASIA' and tamanio = 'PEQUEÑA' and nombre_local = 'Virreyes' and precio_dia = 0;
update registro_tortelin set precio_dia = 35
where sabor = 'GUANABANA' and tamanio = 'MEDIANA' and nombre_local = 'Santa Anita' and precio_dia = 0;
update registro_tortelin set precio_dia = 39
where sabor = 'TRES LECHES DE VAINILLA' and tamanio = 'MEDIANA' and nombre_local = 'Santa Anita' and precio_dia = 0;
update registro_tortelin set precio_dia = 40 
where sabor = 'TRES LECHES DE CHOCOLATE' and tamanio = 'MEDIANA' and nombre_local = 'Santa Rosa' and precio_dia = 0;
update registro_tortelin set precio_dia = 40
where sabor = 'TRES LECHES DE CHOCOLATE' and tamanio = 'MEDIANA' and nombre_local = 'Virreyes' and precio_dia = 0;
update registro_tortelin set precio_dia = 33
where sabor = 'TRES LECHES DE MANGO' and tamanio = 'PEQUEÑA' and nombre_local = 'Santa Anita' and precio_dia = 0;
update registro_tortelin set precio_dia = 55
where sabor = 'TRES LECHES DE COCO' and tamanio = 'GRANDE' and nombre_local = 'Santa Anita' and precio_dia = 0;
update registro_tortelin set precio_dia = 25
where sabor = 'COCO' and tamanio = 'PEQUEÑA' and nombre_local = 'Vista Alegre' and precio_dia = 0;
update registro_tortelin set precio_dia = 28
where sabor = 'COCO' and tamanio = 'PEQUEÑA' and nombre_local = 'Santa Rosa' and precio_dia = 0;
update registro_tortelin set precio_dia = 30
where sabor = 'MANGO' and tamanio = 'PEQUEÑA' and nombre_local = 'Santa Anita' and precio_dia = 0;
update registro_tortelin set precio_dia = 40
where sabor = 'TRES LECHES DE MOKA' and tamanio = 'MEDIANA' and nombre_local = 'Los Ángeles' and precio_dia = 3;
update registro_tortelin set precio_dia = 35
where sabor = 'MANGO' and tamanio = 'MEDIANA' and nombre_local = 'Los Ángeles' and precio_dia = 3;
update registro_tortelin set precio_dia = 33
where sabor = 'TRES LECHES DE LUCUMA' and tamanio = 'PEQUEÑA' and nombre_local = 'Los Ángeles' and precio_dia = 3;
update registro_tortelin set precio_dia = 48
where sabor = 'MOKA' and tamanio = 'GRANDE' and nombre_local = 'Santa Anita' and precio_dia = 5;
update registro_tortelin set precio_dia = 35
where sabor = 'MANGO' and tamanio = 'MEDIANA' and nombre_local = 'Los Ángeles' and precio_dia = 5;
update registro_tortelin set precio_dia = 57
where sabor = 'GUANABANA' and tamanio = 'SUPERGRANDE' and nombre_local = 'Los Ángeles' and precio_dia = 6;
update registro_tortelin set precio_dia = 58
where sabor = 'TRES LECHES DE CHOCOLATE' and tamanio = 'GRANDE' and nombre_local = 'Los Ángeles' and precio_dia = 8;
update registro_tortelin set precio_dia = 38
where sabor = 'LUCUMA' and tamanio = 'MEDIANA' and nombre_local = 'Los Ángeles' and precio_dia = 83;
END 
GO""")
conexion.commit()

cur.execute("EXEC MODIFICAR_PRECIOS;")
conexion.commit()