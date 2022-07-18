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

##Limpiar_sabor_COCO
cur.execute("""IF OBJECT_ID('Limpiar_sabor_COCO') IS NOT NULL
BEGIN
DROP PROC Limpiar_sabor_COCO
END
GO
CREATE PROCEDURE Limpiar_sabor_COCO
	@sabor varchar(500)
AS
BEGIN
UPDATE registro_tortelin
SET sabor = @sabor
WHERE sabor = 'COOC'
END
GO""")
conexion.commit()

##Limpiar_sabor_TRES_LECHES_DE_COCO
cur.execute("""IF OBJECT_ID('Limpiar_sabor_TRES_LECHES_DE_COCO') IS NOT NULL
BEGIN
DROP PROC Limpiar_sabor_TRES_LECHES_DE_COCO
END
GO
CREATE PROCEDURE Limpiar_sabor_TRES_LECHES_DE_COCO
	@sabor varchar(500)
AS
BEGIN
UPDATE registro_tortelin
SET sabor = @sabor
WHERE sabor ='3LECHES DE COCO'
END 
GO""")
conexion.commit()

##Limpiar_sabor_TRES_LECHES_DE_COCHOLATE
cur.execute("""IF OBJECT_ID('Limpiar_sabor_TRES_LECHES_DE_COCHOLATE') IS NOT NULL
BEGIN
DROP PROC Limpiar_sabor_TRES_LECHES_DE_COCHOLATE
END
GO
CREATE PROCEDURE Limpiar_sabor_TRES_LECHES_DE_COCHOLATE
	@sabor varchar(500)
AS
BEGIN
UPDATE registro_tortelin
SET sabor = @sabor
WHERE sabor in ('TRES LECHES CHOCO', '3L CHOCO', '3L CHYOCO', '3L CHOCOLATE', 'CHOCOMILK', ' TRES LECHES DE CHOCO', 'TRES LECHES CHOCOLATE ', '3LECHESA DE CHOCOLATE', '3L  CHOCO', '3LECHES DE choco', '3 LECHES DE CHOCOLATE ', '3l de chocolate', '3lL CHOCOLATE', 'tres lehes de chocolate ', '3l choco con desuento del dia sabado ', 'TRES LECHES DE CHOCO', '3 leches de choco', '3 LECHES CHOCO', 'TRES LECHE DE CHOCO', 'TRES LEHES DE CHOCO', '3LECHES CHOCO', 'TRES LECHES DE CHOCOCLATE', '3 LEXCHES CHOCO', '3l de choco', 'TRES LECHES DE CHOC', 'tres leches de chocolate', 'TRES LECHES DE CHOCOLAT','3LECHES DE CHOCOLATE','TRES LECHE DE CHOCOLATE', '3  LECHES DE  CHOCOLATE')
END 
GO""")
conexion.commit()

##Limpiar_sabor_TRES_LECHES_DE_MARACUYA
cur.execute("""IF OBJECT_ID('Limpiar_sabor_TRES_LECHES_DE_MARACUYA') IS NOT NULL
BEGIN
DROP PROC Limpiar_sabor_TRES_LECHES_DE_MARACUYA
END
GO
CREATE PROCEDURE Limpiar_sabor_TRES_LECHES_DE_MARACUYA
	@sabor varchar(500)
AS
BEGIN
UPDATE registro_tortelin
SET sabor = @sabor
WHERE sabor in ('TRES LECHES DEV MARACUYA', 'TRES LECHES MARACU','TRES LECHES MARAUCY','3L MARAXCUYA', '3L MARACUYA', 'TRES LECHES MARACUY', 'TRES LECHES DE MARACU', 'TRES LECHES DE MARACUY', ' TRES LECHES DE MARACUYA', '3LECHES DE MARACUYA ', '3 LECHES MARACUYA','TRES LECHES D E MARACUYA', 'trea LECHES DE MARACUYA', '3l de maracuya', 'TRES LECHES MARACUYA', 'TRES LECHES DE MARACUTYA', '3leches maracuya', '3 MARACUYA', 'TRES LECHES DE MARACUYA CON DESCUENTO ', 'TRES LECHE DE MARACUYA', '3 LECHES DE MARACUYA', 'TRES LECHES MAREACUYA', 'TRES LECHES MAARCUY', 'tres leches de maracuya', 'TRES LECHES DE MRACUYA', 'TRES LECHES DE MARACUIUYA', 'TRES LECHES DE MARACUUYA','TRES LECHES DE NARACYTA', 'TRES LECHES DE MARAUCYA', 'TRESS LECHES DE MARACUYA','TRES LECHES DE  MARACUYA')
END 
GO""")
conexion.commit()

##Limpiar_sabor_MARACUYA
cur.execute("""IF OBJECT_ID('Limpiar_sabor_MARACUYA') IS NOT NULL
BEGIN
DROP PROC Limpiar_sabor_MARACUYA
END
GO
CREATE PROCEDURE Limpiar_sabor_MARACUYA
	@sabor varchar(500)
AS
BEGIN
UPDATE registro_tortelin
SET sabor = @sabor
WHERE sabor in ('SENSACIOM MARACUYA',' MARACUYA', 'MARACUYA CON DESCUENTO', 'SENSACION DE MARACUYA', 'MARACUYA}', 'SENSACIN MARACUYA')
END 
GO""")
conexion.commit()

##Limpiar_sabor_CHOCOLATE
cur.execute("""IF OBJECT_ID('Limpiar_sabor_CHOCOLATE') IS NOT NULL
BEGIN
DROP PROC Limpiar_sabor_CHOCOLATE
END
GO
CREATE PROCEDURE Limpiar_sabor_CHOCOLATE
	@sabor varchar(500)
AS
BEGIN
UPDATE registro_tortelin
SET sabor = @sabor
WHERE sabor in ('CHOCO','CHOCOLATE CON DESCUENTO ( STICKER AMARILLO )')
END 
GO""")
conexion.commit()

##Limpiar_sabor_CHOCOFRESA
cur.execute("""IF OBJECT_ID('Limpiar_sabor_CHOCOFRESA') IS NOT NULL
BEGIN
DROP PROC Limpiar_sabor_CHOCOFRESA
END
GO
CREATE PROCEDURE Limpiar_sabor_CHOCOFRESA
	@sabor varchar(500)
AS
BEGIN
UPDATE registro_tortelin
SET sabor = @sabor
WHERE sabor in ('CHOFRESA','CHOCOFRESA (con descuento stiker naranja ')
END 
GO""")
conexion.commit()

##Limpiar_sabor_TRES_LECHES_DE_LUCUMA
cur.execute("""IF OBJECT_ID('Limpiar_sabor_TRES_LECHES_DE_LUCUMA') IS NOT NULL
BEGIN
DROP PROC Limpiar_sabor_TRES_LECHES_DE_LUCUMA
END
GO
CREATE PROCEDURE Limpiar_sabor_TRES_LECHES_DE_LUCUMA
	@sabor varchar(500)
AS
BEGIN
UPDATE registro_tortelin
SET sabor = @sabor
WHERE sabor in ('3L LUCUMA CON DESCUENTO ( STICKER AMARILLO )','TRES LECHES DE LUCUMA DESCUENTO ( STICKER ROJO ) ', 'tres leche de lucuma', 'RTRES LECHES LUCU', '3 LECHES DE LUCUMA', 'TRES LECHES LUK DSCN', 'TRES LECHES DE LUNMA', 'TRES LECHES DE LUCMA')
END 
GO""")
conexion.commit()

##Limpiar_sabor_LUCUMA
cur.execute("""IF OBJECT_ID('Limpiar_sabor_LUCUMA') IS NOT NULL
BEGIN
DROP PROC Limpiar_sabor_LUCUMA
END
GO
CREATE PROCEDURE Limpiar_sabor_LUCUMA
	@sabor varchar(500)
AS
BEGIN
UPDATE registro_tortelin
SET sabor = @sabor
WHERE sabor in ('DULZURA DE LUNMA')
END 
GO""")
conexion.commit()

##Limpiar_sabor_TRES_LECHES_DE_MOKA
cur.execute("""IF OBJECT_ID('Limpiar_sabor_TRES_LECHES_DE_MOKA') IS NOT NULL
BEGIN
DROP PROC Limpiar_sabor_TRES_LECHES_DE_MOKA
END
GO
CREATE PROCEDURE Limpiar_sabor_TRES_LECHES_DE_MOKA
	@sabor varchar(500)
AS
BEGIN
UPDATE registro_tortelin
SET sabor = @sabor
WHERE sabor in ('3L MOKA ', '3L MOKA CON DESCUESTO', 'TRES LECHES DE MOKA CON DESCUENTO ( STICKER ROJO )')
END 
GO""")
conexion.commit()

##Limpiar_sabor_MOKA
cur.execute("""IF OBJECT_ID('Limpiar_sabor_MOKA') IS NOT NULL
BEGIN
DROP PROC Limpiar_sabor_MOKA
END
GO
CREATE PROCEDURE Limpiar_sabor_MOKA
	@sabor varchar(500)
AS
BEGIN
UPDATE registro_tortelin
SET sabor = @sabor
WHERE sabor in ('MOKA DESCN PIPOS ', 'TORTA MOKA ESPECIAL', 'MOKA CON DESCUENTO')
END 
GO""")
conexion.commit()

##Limpiar_sabor_TRES_LECHES_DE_VAINILLA
cur.execute("""IF OBJECT_ID('Limpiar_sabor_TRES_LECHES_DE_VAINILLA') IS NOT NULL
BEGIN
DROP PROC Limpiar_sabor_TRES_LECHES_DE_VAINILLA
END
GO
CREATE PROCEDURE Limpiar_sabor_TRES_LECHES_DE_VAINILLA
	@sabor varchar(500)
AS
BEGIN
UPDATE registro_tortelin
SET sabor = @sabor
WHERE sabor in ('3LECHES DE VAINILLA', '3L VAINILLA  CON DESCUENTO', '3L VAINILLA', 'TRES LECHES DE VAINILLA CON DESCUENTO', '3L VAINILLA G CON DESCUENTO ( STICKER AMARILLO )', 'TRES LECHES DE VAINILLA CON DESCUENTO POR ESTAR CHANCADA')
END 
GO""")
conexion.commit()

##Limpiar_sabor_TRES_LECHES_DE_MANGO
cur.execute("""IF OBJECT_ID('Limpiar_sabor_TRES_LECHES_DE_MANGO') IS NOT NULL
BEGIN
DROP PROC Limpiar_sabor_TRES_LECHES_DE_MANGO
END
GO
CREATE PROCEDURE Limpiar_sabor_TRES_LECHES_DE_MANGO
	@sabor varchar(500)
AS
BEGIN
UPDATE registro_tortelin
SET sabor = @sabor
WHERE sabor in ('3L MANGO CON DESCUENTO ( STICKER AMARILLO )', 'TRES LECHES MANGO', '3l de mango', '3L MANGO ', 'TRES LECHES DE MANGO CON DESCUENTO ', 'TRES LECHES DE MANGOI')
END 
GO""")
conexion.commit()

##Limpiar_sabor_GUANABANA
cur.execute("""IF OBJECT_ID('Limpiar_sabor_GUANABANA') IS NOT NULL
BEGIN
DROP PROC Limpiar_sabor_GUANABANA
END
GO
CREATE PROCEDURE Limpiar_sabor_GUANABANA
	@sabor varchar(500)
AS
BEGIN
UPDATE registro_tortelin
SET sabor = @sabor
WHERE sabor in ('GUABANA', 'GUANABNAB', 'GUANABNAA', 'GUANABANA CON DESCUENTO ', 'GUANABA', 'GUANÁBANA', 'GUANABAMA', 'guanabana', 'GUANABNA','GUANBANA')
END 
GO""")
conexion.commit()

##Limpiar_sabor_FRESA_CON_DURAZNO
cur.execute("""IF OBJECT_ID('Limpiar_sabor_FRESA_CON_DURAZNO') IS NOT NULL
BEGIN
DROP PROC Limpiar_sabor_FRESA_CON_DURAZNO
END
GO
CREATE PROCEDURE Limpiar_sabor_FRESA_CON_DURAZNO
	@sabor varchar(500)
AS
BEGIN
UPDATE registro_tortelin
SET sabor = @sabor
WHERE sabor in ('FRESA CON DURAZNO QUE SE CANCELO LA MIRAD 30 SOLES ', 'FRESA CXON DURAZNO CMABIO', 'FRESA CON DURAZNO PEQ CON DESCUENTO ( ABOGADA SRA GRACIELA )', 'FRESA CON DURAZNO DESCUENTO DE 5 SOLES')
END 
GO""")
conexion.commit()

##Limpiar_sabor_COCTEL_DE_FRUTAS
cur.execute("""IF OBJECT_ID('Limpiar_sabor_COCTEL_DE_FRUTAS') IS NOT NULL
BEGIN
DROP PROC Limpiar_sabor_COCTEL_DE_FRUTAS
END
GO
CREATE PROCEDURE Limpiar_sabor_COCTEL_DE_FRUTAS
	@sabor varchar(500)
AS
BEGIN
UPDATE registro_tortelin
SET sabor = @sabor
WHERE sabor in ('COCTEL DESCUENTO DE 2 SOLES', 'COCTEL DESCUENT TRABAJADOR', ' coctel de fruta (en promocion, etiqueta anaranjada)', 'COCTEL CON DESCUENTO', 'COCTEL DESCUENTO0', 'COCKTEL DE FRUTAS', 'CCOTEL DE FRUTAS ')
END 
GO""")
conexion.commit()

##Limpiar_sabor_RED_VELVET
cur.execute("""IF OBJECT_ID('Limpiar_sabor_RED_VELVET') IS NOT NULL
BEGIN
DROP PROC Limpiar_sabor_RED_VELVET
END
GO
CREATE PROCEDURE Limpiar_sabor_RED_VELVET
	@sabor varchar(500)
AS
BEGIN
UPDATE registro_tortelin
SET sabor = @sabor
WHERE sabor in ('RED VELVET CON DESCUENTO ( STICKER NARANJA )', 'RED VELD ', 'RET VET', 'RED VELVET CON DESCUENTO DE 5 SOLES')
END 
GO""")
conexion.commit()

##Limpiar_sabor_MANJAR
cur.execute("""IF OBJECT_ID('Limpiar_sabor_MANJAR') IS NOT NULL
BEGIN
DROP PROC Limpiar_sabor_MANJAR
END
GO
CREATE PROCEDURE Limpiar_sabor_MANJAR
	@sabor varchar(500)
AS
BEGIN
UPDATE registro_tortelin
SET sabor = @sabor
WHERE sabor in ('MANJAR VENDIDA A 36 SOLES ')
END 
GO""")
conexion.commit()

##Limpiar_sabor_SELVA_NEGRA
cur.execute("""IF OBJECT_ID('Limpiar_sabor_SELVA_NEGRA') IS NOT NULL
BEGIN
DROP PROC Limpiar_sabor_SELVA_NEGRA
END
GO
CREATE PROCEDURE Limpiar_sabor_SELVA_NEGRA
	@sabor varchar(500)
AS
BEGIN
UPDATE registro_tortelin
SET sabor = @sabor
WHERE sabor in ('SELVA NEGRA  CON DESCUENTO', 'SELVA NEGRA CON DESCUENTO', 'SELVA NEGRA CON DESCUENTO SUPER GRANDE')
END 
GO""")
conexion.commit()

##Limpiar_sabor_SELVA_BLANCA
cur.execute("""IF OBJECT_ID('Limpiar_sabor_SELVA_BLANCA') IS NOT NULL
BEGIN
DROP PROC Limpiar_sabor_SELVA_BLANCA
END
GO
CREATE PROCEDURE Limpiar_sabor_SELVA_BLANCA
	@sabor varchar(500)
AS
BEGIN
UPDATE registro_tortelin
SET sabor = @sabor
WHERE sabor in ('SELVA BLANCA CON DESCUENTO ( STICKER CELESTE )', 'SELVA BLANCA CON DESCUESTO ')
END 
GO""")
conexion.commit()

##Limpiar_sabor_TORTA_HELADA
cur.execute("""IF OBJECT_ID('Limpiar_sabor_TORTA_HELADA') IS NOT NULL
BEGIN
DROP PROC Limpiar_sabor_TORTA_HELADA
END
GO
CREATE PROCEDURE Limpiar_sabor_TORTA_HELADA
	@sabor varchar(500)
AS
BEGIN
UPDATE registro_tortelin
SET sabor = @sabor
WHERE sabor in ('TORTA HELADA CON DESCUENTO ', 'TORETA HELADA')
END 
GO""")
conexion.commit()

##Limpiar_sabor_SAUCO
cur.execute("""IF OBJECT_ID('Limpiar_sabor_SAUCO') IS NOT NULL
BEGIN
DROP PROC Limpiar_sabor_SAUCO
END
GO
CREATE PROCEDURE Limpiar_sabor_SAUCO
	@sabor varchar(500)
AS
BEGIN
UPDATE registro_tortelin
SET sabor = @sabor
WHERE sabor in ('SAUCO CON DESCUENTO ( STICKER NARANJA )')
END 
GO""")
conexion.commit()

##Limpiar_sabor_SAUCO
cur.execute("""IF OBJECT_ID('Limpiar_sabor_SAUCO') IS NOT NULL
BEGIN
DROP PROC Limpiar_sabor_SAUCO
END
GO
CREATE PROCEDURE Limpiar_sabor_SAUCO
	@sabor varchar(500)
AS
BEGIN
UPDATE registro_tortelin
SET sabor = @sabor
WHERE sabor in ('SAUCO CON DESCUENTO ( STICKER NARANJA )')
END 
GO""")
conexion.commit()

##ELIMINAR_REGISTROS
cur.execute("""IF OBJECT_ID('ELIMINAR_REGISTROS') IS NOT NULL
BEGIN
DROP PROC ELIMINAR_REGISTROS
END
GO
CREATE PROCEDURE ELIMINAR_REGISTROS
AS
BEGIN
DELETE FROM registro_tortelin
WHERE sabor in ('TIRAMIZU', 'o', 'coo', 'SAN VALEMTIN ', 'TORTA DE SAN VALENTIN', 'BRUCELINA ', 'OPERA', ' ', 'TIRAMISU', 'san valentin ', '}', 'torta san valentin', 'TIRANISU')
END 
GO
""")
conexion.commit()


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

cur.execute("EXEC ELIMINAR_REGISTROS;")
conexion.commit()