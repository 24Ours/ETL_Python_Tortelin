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

##Ejecutar procedures

cur.execute("EXEC Limpiar_sabor_COCO 'COCO';")
conexion.commit()

cur.execute("EXEC Limpiar_sabor_TRES_LECHES_DE_COCO 'TRES LECHES DE COCO';")
conexion.commit()

cur.execute("EXEC Limpiar_sabor_TRES_LECHES_DE_COCHOLATE 'TRES LECHES DE CHOCOLATE';")
conexion.commit()

cur.execute("EXEC Limpiar_sabor_TRES_LECHES_DE_MARACUYA 'TRES LECHES DE MARACUYA';")
conexion.commit()

cur.execute("EXEC Limpiar_sabor_MARACUYA 'MARACUYA';")
conexion.commit()

cur.execute("EXEC Limpiar_sabor_CHOCOLATE 'CHOCOLATE';")
conexion.commit()

cur.execute("EXEC Limpiar_sabor_CHOCOFRESA 'CHOCOFRESA';")
conexion.commit()

cur.execute("EXEC Limpiar_sabor_TRES_LECHES_DE_LUCUMA 'TRES LECHES DE LUCUMA';")
conexion.commit()

cur.execute("EXEC Limpiar_sabor_LUCUMA 'LUCUMA';")
conexion.commit()

cur.execute("EXEC Limpiar_sabor_TRES_LECHES_DE_MOKA 'TRES LECHES DE MOKA';")
conexion.commit()

cur.execute("EXEC Limpiar_sabor_MOKA 'MOKA';")
conexion.commit()

cur.execute("EXEC Limpiar_sabor_TRES_LECHES_DE_VAINILLA 'TRES LECHES DE VAINILLA';")
conexion.commit()

cur.execute("EXEC Limpiar_sabor_TRES_LECHES_DE_MANGO 'TRES LECHES DE MANGO';")
conexion.commit()

cur.execute("EXEC Limpiar_sabor_GUANABANA 'GUANABANA';")
conexion.commit()

cur.execute("EXEC Limpiar_sabor_FRESA_CON_DURAZNO 'FRESA CON DURAZNO';")
conexion.commit()

cur.execute("EXEC Limpiar_sabor_COCTEL_DE_FRUTAS 'COCTEL DE FRUTAS';")
conexion.commit()

cur.execute("EXEC Limpiar_sabor_RED_VELVET 'RED VELVET';")
conexion.commit()

cur.execute("EXEC Limpiar_sabor_MANJAR 'MANJAR';")
conexion.commit()

cur.execute("EXEC Limpiar_sabor_SELVA_NEGRA 'SELVA NEGRA';")
conexion.commit()

cur.execute("EXEC Limpiar_sabor_SELVA_BLANCA 'SELVA BLANCA';")
conexion.commit()

cur.execute("EXEC Limpiar_sabor_TORTA_HELADA 'TORTA HELADA';")
conexion.commit()

cur.execute("EXEC Limpiar_sabor_SAUCO 'SAUCO';")
conexion.commit()

cur.execute("EXEC ELIMINAR_NOMBRES_INCORRECTOS;")
conexion.commit()