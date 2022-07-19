import pickle
import os

print("Introduzca el número de carpetas de locales que desea recorrer: ")

contador = int(input())

if os.path.isfile('lista_id_folder_local.pckl') == True:
    print("abriendo pickle")
    ficha_abrir = open('lista_id_folder_local.pckl', 'rb')
    lista_carpeta = pickle.load(ficha_abrir)
    ficha_abrir.close()
    
else:
    print("Creando nuevo pickle")
    lista_carpeta = []
    ficha_carpeta = open('lista_id_folder_local.pckl', 'wb')
    pickle.dump(lista_carpeta, ficha_carpeta)
    ficha_carpeta.close()

if os.path.isfile('lista_nombre_folder_local.pckl') == True:
    print("abriendo pickle")
    ficha_abrir = open('lista_nombre_folder_local.pckl', 'rb')
    lista_nombre = pickle.load(ficha_abrir)
    ficha_abrir.close()
    
else:
    print("Creando nuevo pickle")
    lista_nombre = []
    ficha_nombre = open('lista_nombre_folder_local.pckl', 'wb')
    pickle.dump(lista_nombre, ficha_nombre)
    ficha_nombre.close()

for i in range(contador):
    print("ingrese el ID de la carpeta del local: ")
    ID_local = input()
    lista_carpeta.append(ID_local)
    print("ingrese el nombre de la carpeta del local: ")
    nombre_local = input()
    lista_nombre.append(nombre_local)
    
    
ficha_carpeta = open('lista_id_folder_local.pckl', 'wb')
pickle.dump(lista_carpeta, ficha_carpeta)
ficha_carpeta.close()

ficha_nombre = open('lista_nombre_folder_local.pckl', 'wb')
pickle.dump(lista_nombre, ficha_nombre)
ficha_nombre.close()


#------------------------GENERANDO PICKLES-------------------------------

print("Creeando Pickles Necesarios")

if os.path.isfile('lista_id_excel_ms.pckl') == False:
    lista = []
    ficha_crear = open('lista_id_excel_ms.pckl', 'wb')
    pickle.dump(lista, ficha_crear)
    ficha_crear.close()
    
if os.path.isfile('lista_id_excel_sheet.pckl') == False:
    lista = []
    ficha_crear = open('lista_id_excel_sheet.pckl', 'wb')
    pickle.dump(lista, ficha_crear)
    ficha_crear.close()
    
if os.path.isfile('lista_id_excel.pckl') == False:
    lista = []
    ficha_crear = open('lista_id_excel.pckl', 'wb')
    pickle.dump(lista, ficha_crear)
    ficha_crear.close()

if os.path.isfile('lista_id_folder_mes_automatizado.pckl') == False:
    lista = []
    ficha_crear = open('lista_id_folder_mes_automatizado.pckl', 'wb')
    pickle.dump(lista, ficha_crear)
    ficha_crear.close()

if os.path.isfile('lista_nuevos_sheet.pckl') == False:
    lista = []
    ficha_crear = open('lista_nuevos_sheet.pckl', 'wb')
    pickle.dump(lista, ficha_crear)
    ficha_crear.close()

if os.path.isfile('back_pick.pckl') == False:
    lista = []
    ficha_crear = open('back_pick.pckl', 'wb')
    pickle.dump(lista, ficha_crear)
    ficha_crear.close()
