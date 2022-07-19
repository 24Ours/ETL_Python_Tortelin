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

cur.execute("select fecha, nombre_local, tamanio, sabor from registro_tortelin group by fecha, nombre_local, tamanio, sabor having count(*) > 1")
duplicados = cur.fetchall()
print(duplicados)

for fecha, local, tamaño, sabor in duplicados:
    print(fecha, local, tamaño, sabor)
    cur.execute("select unidades_vendidas, unidades_desechadas, precio_dia from registro_tortelin where nombre_local= ? and tamanio = ? and sabor = ? and fecha = ?", local, tamaño, sabor, fecha)
    duplicado = cur.fetchall()
    print(duplicado)
    univend = duplicado[0][0] + duplicado[1][0]
    unidesech = duplicado[0][1] + duplicado[1][1]
    if duplicado[0][0] == duplicado[0][1] and duplicado[0][1] == duplicado[1][1] and duplicado[0][2] == duplicado[1][2]:
        print("completamente iguales")
    else:
        if duplicado[0][2] > duplicado[1][2]:
            precio = duplicado[0][2]
            cur.execute("update registro_tortelin set unidades_vendidas = ?, unidades_desechadas = ? where nombre_local=? and tamanio =? and sabor = ? and fecha = ? and precio_dia = ?;", univend, unidesech, local, tamaño, sabor, fecha, precio)
            cur.execute("delete from registro_tortelin where unidades_vendidas = ? and unidades_desechadas = ? and nombre_local=? and tamanio =? and sabor = ? and fecha = ? and precio_dia = ?;", duplicado[1][0], duplicado[1][1], local, tamaño, sabor, fecha, duplicado[1][2])
            conexion.commit()
        else:
            precio = duplicado[1][2]
            cur.execute("update registro_tortelin set unidades_vendidas = ?, unidades_desechadas = ? where nombre_local=? and tamanio =? and sabor = ? and fecha = ? and precio_dia = ?;", univend, unidesech, local, tamaño, sabor, fecha, precio)
            cur.execute("delete from registro_tortelin where unidades_vendidas = ? and unidades_desechadas = ? and nombre_local=? and tamanio =? and sabor = ? and fecha = ? and precio_dia = ?;", duplicado[0][0], duplicado[0][1], local, tamaño, sabor, fecha, duplicado[0][2])
            conexion.commit() 


cur.execute("EXEC ELIMINAR_REGISTROS;")
conexion.commit()