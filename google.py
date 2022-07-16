from __future__ import print_function
from email.errors import FirstHeaderLineIsContinuationDefect
from mimetypes import MimeTypes
import pickle
import os
from google_auth_oauthlib.flow import Flow, InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload, MediaIoBaseDownload
from google.auth.transport.requests import Request
import pandas as pd
import numpy as np
import re
from sqlalchemy import create_engine
import psycopg2
import time

def Create_Service(client_secret_file, api_name, api_version, *scopes):
    print(client_secret_file, api_name, api_version, scopes, sep='-')
    CLIENT_SECRET_FILE = client_secret_file
    API_SERVICE_NAME = api_name
    API_VERSION = api_version
    SCOPES = [scope for scope in scopes[0]]
    print(SCOPES)

    cred = None

    pickle_file = f'token_{API_SERVICE_NAME}_{API_VERSION}.pickle'
    # print(pickle_file)

    if os.path.exists(pickle_file):
        with open(pickle_file, 'rb') as token:
            cred = pickle.load(token)

    if not cred or not cred.valid:
        if cred and cred.expired and cred.refresh_token:
            cred.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(CLIENT_SECRET_FILE, SCOPES)
            cred = flow.run_local_server()

        with open(pickle_file, 'wb') as token:
            pickle.dump(cred, token)

    try:
        service = build(API_SERVICE_NAME, API_VERSION, credentials=cred)
        print(API_SERVICE_NAME, 'service created successfully')
        return service
    except Exception as e:
        print('Unable to connect.')
        print(e)
        return None

#def convert_to_RFC_datetime(year=1900, month=1, day=1, hour=0, minute=0):
 #   dt = datetime.datetime(year, month, day, hour, minute, 0).isoformat() + 'Z'
 #   return dt
 
CLIENT_SECRET_FILE = 'client_secrets.json'
API_NAME = 'drive'
API_VERSION = 'v3'
SCOPES = ['https://www.googleapis.com/auth/drive']

service = Create_Service(CLIENT_SECRET_FILE, API_NAME, API_VERSION, SCOPES)

# CONVERTIR MS.EXCEL A FORMATO SPREADSHEET

from apiclient import errors
from googleapiclient.http import MediaFileUpload
# ...

def update_file(service, file_id, parent_id):
  """Update an existing file's metadata and content.

  Args:
    service: Drive API service instance.
    file_id: ID of the file to update.
    new_title: New title for the file.
    new_description: New description for the file.
    new_mime_type: New MIME type for the file.
    new_filename: Filename of the new content to upload.
    new_revision: Whether or not to create a new revision for this file.
  Returns:
    Updated file metadata if successful, None otherwise.
  """
  try:
    # First retrieve the file from the API.
    file = service.files().get(fileId=file_id).execute()

    # File's new metadata.
    file_metadata = {
        'name' : file['name'],
        'mimeType' : 'application/vnd.google-apps.spreadsheet',
        'parents': [parent_id]
    }

    # Send the request to the API.
    updated_file = service.files().copy(
        fileId=file_id,
        body=file_metadata,
        ).execute()
    return updated_file
  except errors.HttpError:
    print('a')
    return None

def delete_file(service, file_id):
    """Permanently delete a file, skipping the trash.

    Args:
    service: Drive API service instance.
    file_id: ID of the file to delete.
    """
    try:
        service.files().delete(fileId=file_id).execute()
    except errors.HttpError:
        print ('hubo un error')
    return None

#update_file(service, "1OdKERKlnok64kQWrWsDBjJ32dWl4RXJ7", "application/vnd.google-apps.spreadsheet")
#file = service.files().get(fileId='1SpDR3l86HpYyJXFH-IdrT04ACq6SJQyV').execute()
#print(file)

#BUSCANDO FOLDERS DE LOS MESES

#update_file(service, "1afIiJWnj1SJKnGNN1sg9bUq-1tGJi3BN", "11pkllolLtIucbKs9qVaINycJlsKrVMjD")

def obtener_nombre(service, file_id):
    file = service.files().get(fileId=file_id).execute()
    return file['name']

def search_folders():
    ficha1 = open('lista_id_folder_local.pckl', 'rb')
    lista_locales = pickle.load(ficha1)
    ficha1.close()
    ficha2 = open('lista_id_folder_mes_automatizado.pckl', 'rb')
    lista_meses = pickle.load(ficha2)
    ficha2.close()
    ficha_back = open('back_pick.pckl', 'rb')
    lista_back = pickle.load(ficha_back)
    ficha_back.close()
    a = 0
    for i in lista_locales:
        query_extra = " and '"+i+"'"+" in parents"
        page_token = None
        while True:
            response = service.files().list(q="mimeType='application/vnd.google-apps.folder'"+query_extra,
                                            spaces='drive',
                                            fields='nextPageToken, files(id, name)',
                                            pageToken=page_token).execute()
            for file in response.get('files', []):
                # Process change
                print (file.get('name'), file.get('id'))
                if file.get('id') not in lista_meses:
                    lista_meses.append(file.get('id'))
                    lista_back.append(file.get('id'))
                else:
                    print('ya está en el pickle de meses')
            page_token = response.get('nextPageToken', None)
            if page_token is None:
                break
    
    ficha3 = open('lista_id_folder_mes_automatizado.pckl', 'wb')
    pickle.dump(lista_meses, ficha3)
    ficha3.close()
    ficha_back = open('back_pick.pckl', 'wb')
    pickle.dump(lista_back, ficha_back)
    ficha_back.close()
    
    return print('no hay error')

#search_folders()

#LA PRIMERA FUNCIÓN A EJECUTAR ES search_folders
#search_folders()
#Con esta función se obtienen las 

def search_and_convert_excels_ms():
    ficha1 = open('lista_id_folder_mes_automatizado.pckl', 'rb')
    lista_meses = pickle.load(ficha1)
    ficha1.close()
    ficha2 = open('lista_id_excel_ms.pckl', 'rb')
    lista_excels_ms = pickle.load(ficha2)
    ficha2.close()
    ficha_back = open('back_pick.pckl', 'rb')
    lista_back = pickle.load(ficha_back)
    ficha_back.close()
    for i in lista_meses:
        query_extra = " and '"+i+"'"+" in parents"
        page_token = None
        while True:
            response = service.files().list(q="mimeType='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'"+query_extra,
                                            spaces='drive',
                                            fields='nextPageToken, files(id, name)',
                                            pageToken=page_token).execute()
            for file in response.get('files', []):
                # Proceso de conversión
                if file.get('id') not in lista_excels_ms: 
                    #Si no está en la lista, se cambia el mimetype y se agrega a la lista
                    print('se encontró un archivo nuevo')
                    lista_excels_ms.append(file.get('id'))
                    lista_back.append(file.get('id'))
                    update_file(service, file.get('id'), i) #Modificando el mimetype
                    print (file.get('id')) #Imprimiendo el ID del documento MS
                    
            page_token = response.get('nextPageToken', None)
            if page_token is None:
                break
    ficha3 = open('lista_id_excel_ms.pckl', 'wb')
    pickle.dump(lista_excels_ms, ficha3)
    ficha3.close()
    ficha_back = open('back_pick.pckl', 'wb')
    pickle.dump(lista_back, ficha_back)
    ficha_back.close()
    
    return print('no hay error')

#LA SEGUNDA FUNCIÓN A EJECUTAR ES PARA BUSCAR Y CONVERTIR DENTRO DE CADA FOLDER.

#search_and_convert_excels_ms()

""" fichero1 = open('lista_id_folder_mes.pckl', 'rb')
listas_meses_id = pickle.load(fichero1)
fichero1.close() """

""" fichero2 = open('lista_id_excel_ms.pckl', 'rb')
lista_id_excels_ms = pickle.open(fichero2)
fichero2.close() """

# ENCONTRAR LOS NUEVOS SHEETS GENERADOS Y OBTENER SUS ID PARA EL PROCESO ETL

def listar_new_excels_sheets():
    ficha1 = open('lista_id_folder_mes_automatizado.pckl', 'rb')
    lista_meses = pickle.load(ficha1)
    ficha1.close()
    ficha2 = open('lista_id_excel_sheet.pckl', 'rb')
    lista_sheets_pk = pickle.load(ficha2)
    ficha2.close()
    ficha_b = open('back_pick.pckl', 'rb')
    lista_back = pickle.load(ficha_b)
    ficha_b.close()
    lista_nuevos_sheet = []
    for i in lista_meses:
        print("esta es la carpeta: "+i)
        query_extra = " and '"+i+"'"+" in parents"
        page_token = None
        while True:
            response = service.files().list(q="mimeType='application/vnd.google-apps.spreadsheet'"+query_extra+" and trashed = false",
                                            spaces='drive',
                                            fields='nextPageToken, files(id, name)',
                                            pageToken=page_token).execute()
            for file in response.get('files', []):
                if file.get('id') not in lista_sheets_pk:
                    print("Un nuevo archivo se ha detectado para procesar")
                    print (file.get('id'))
                    #lista_sheets_pk.append(file.get('id'))
                    lista_nuevos_sheet.append(file.get('id'))
                    lista_back.append(file.get('id'))
                    for j in lista_nuevos_sheet:
                        print("lista_nuevos_sheet: "+j)
                        time.sleep(3)
            page_token = response.get('nextPageToken', None)
            if page_token is None:
                break
    ficha3 = open('lista_id_excel_sheet.pckl', 'wb')
    pickle.dump(lista_sheets_pk, ficha3) #Guardando los ID nuevos en el archivo base
    ficha3.close()
    ficha4 = open('lista_nuevos_sheet.pckl', 'wb')
    pickle.dump(lista_nuevos_sheet, ficha4)
    ficha4.close()
    ficha5 = open('back_pick.pckl', 'wb')
    pickle.dump(lista_back, ficha5)
    ficha5.close()
    return print('no hay error')

#AHORA SE EJECUTA LA FUNCIÓN PARA BUSCAR LOS NUEVOS SHEETS GENERADOS PARA ENCONTRAR SU ID
#Y SE GUARDAN ESOS VALORES

#listado_sheets_nuevos = listar_new_excels_sheets()

""" for i in listas_meses_id:
    page_token = None
    while True:
        response = service.files().list(q="ID = "+i,
                                        spaces='drive',
                                        fields='nextPageToken, files(id, name)',
                                        pageToken=page_token).execute()
        for file in response.get('files', []):
            # Process change
            print (file.get('name'), file.get('id'))
        page_token = response.get('nextPageToken', None)
        if page_token is None:
            break """

#------------------------------SEGUNDA FASE----------------------------------------------------------

import pandas as pd
import pickle
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from google.oauth2 import service_account

SERVICE_ACCOUNT_FILE = 'keys.json'
SCOPES = ['https://www.googleapis.com/auth/spreadsheets']

creds = None
creds = service_account.Credentials.from_service_account_file(
        SERVICE_ACCOUNT_FILE, scopes=SCOPES)

# The ID and range of a sample spreadsheet.

SAMPLE_SPREADSHEET_ID = '1X-P2dW582XG5E6HefoD4AJ0dPWD-miqnLwEBy47QBbw'
#.................................................
# Función de búsqueda de datos
service2 = build('sheets', 'v4', credentials=creds)

""" df = pd.DataFrame

print(df) """

def buscar_datos(rango, id_documento):
    cadena_busqueda= "REPORTE DIARIO!"+rango
    # Call the Sheets API
    sheet = service.spreadsheets()
    result = sheet.values().get(spreadsheetId=id_documento,
                                    range=cadena_busqueda).execute()
    values = result.get('values', [])
    return values


#FUNCION PARA LIMPIAR LA LISTA DE LAS LISTAS VACIAS-----------------------------------------
def limpiar_lista(values):
    lista_nueva = []
    for i in values:
        if len(i) == 21:
            lista_nueva.append(i)
        elif len(i) >= 4:
            if i[3] != '':
                falta = 21 - len(i)
                for j in range(falta):
                    i.append('')
                lista_nueva.append(i)
        
    return lista_nueva

listado_meses_x = ['01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12']
listado_dias_pasados = [0, 31, 59, 90, 120, 151, 181, 212, 243, 273, 304, 334]
listado_dias_por_semana = ['Lunes', 'Martes', 'Miércoles', 'Jueves', 'Viernes', 'Sábado', 'Domingo']
listado_inicios_anio = []

#print(int(listado_meses_x[0]))

#FUNCION PARA ENCONTRAR FECHA COMPLETA
def procesar_nombre(nombre_doc, nombre_carpeta):
    parametros_fecha = []
    sub1 = re.search(r"\d\d", nombre_doc)
    dia = sub1.group()
    sub2 = re.compile("ENERO|FEBRERO|MARZO|ABRIL|MAYO|JUNIO|JULIO|AGOSTO|SEPTIEMBRE|OCTUBRE|NOVIEMBRE|DICIEMBRE")
    mes1 = sub2.findall(nombre_carpeta)
    mes = mes1[0]
    numero_mes = ''
    if mes == 'ENERO':
       numero_mes = '01'
    elif mes == 'FEBRERO':
        numero_mes = '02'
    elif mes == 'MARZO':
        numero_mes = '03'
    elif mes == 'ABRIL':
        numero_mes = '04'
    elif mes == 'MAYO':
        numero_mes = '05'
    elif mes == 'JUNIO':
        numero_mes = '06'
    elif mes == 'JULIO':
        numero_mes = '07'
    elif mes == 'AGOSTO':
        numero_mes = '08'
    elif mes == 'SEPTIEMBRE':
        numero_mes = '09'
    elif mes == 'OCTUBRE':
        numero_mes = '10'
    elif mes == 'NOVIEMBRE':
        numero_mes = '11'
    elif mes == 'DICIEMBRE':
        numero_mes = '12'
    sub3 = re.search(r"\d\d\d\d", nombre_carpeta)
    anio = sub3.group()
    fecha_completa = dia + "-" + numero_mes + "-" + anio
    parametros_fecha.append(int(dia))
    parametros_fecha.append(int(numero_mes))
    parametros_fecha.append(int(anio))
    parametros_fecha.append(fecha_completa)
    parametro_anio = int(anio)-2021
    parametro_semanas = listado_dias_pasados[int(numero_mes)-1]+int(dia)
    cambio_mes = int(numero_mes)-1
    extra_dia = 0
    if parametro_anio == 0:
        extra_dia = 3
    elif parametro_anio == 1:
        extra_dia = 4
    elif parametro_anio == 2:
        extra_dia = 5
    numero_semana, numero_dia = divmod(parametro_semanas+extra_dia, 7)
    nombre_dia = listado_dias_por_semana[numero_dia]
    parametros_fecha.append(numero_semana+1)
    parametros_fecha.append(nombre_dia)
    return parametros_fecha

lista_excepcion = ["", "TOTAL SUPERGRANDES", "TORTAS GRANDES", "TOTAL GRANDES", "TORTAS MEDIANAS",
                   "TOTAL MEDIANAS", "TORTAS PEQUEÑAS"]

#PROCESAMIENTO DE HOJAS DE CÁLCULO
def buscar_celdas(id_documento, nombre_local, nombre_carpeta):
    sheet = service2.spreadsheets()
    datos_nuevo = []
    codigos = "REPORTE DIARIO!A4:U98"
    resultado = sheet.values().get(spreadsheetId=id_documento,
                                range=codigos).execute()
    values = resultado.get('values', [])
    lista_limpia = limpiar_lista(values)
    print(lista_limpia)
    contador = 0
    for i in lista_limpia:
        if i[3] not in lista_excepcion:
            nueva_fila = []
            nueva_fila.append(i[3]) #SABOR
            nueva_fila.append(i[0]) #TAMAÑO
            if i[9] != '' and i[19] != '': #UNIDADES VENDIDAS
                unidades = int(i[9]) + int(i[19])
                nueva_fila.append(unidades)
            elif i[9] == '' and i[19] != '':
                unidades = int(i[19])
                nueva_fila.append(unidades)
            elif i[9] != '' and i[19] == '':
                unidades = int(i[9])
                nueva_fila.append(unidades)
            else: 
                unidades = 0
                nueva_fila.append(unidades)        
            if i[8] != '' and i[18] != '': #UNIDADES RETIRADAS
                retirados = int(i[8]) + int(i[18])
                nueva_fila.append(retirados)
            elif i[8] == '' and i[18] != '':
                retirados = int(i[18])
                nueva_fila.append(retirados)
            elif i[8] != '' and i[18] == '':
                retirados = int(i[8])
                nueva_fila.append(retirados)
            else: 
                retirados = 0
                nueva_fila.append(retirados)
            
            #PRECIO DEL DIA
            nueva_fila.append(i[11])         
            #OBTENER FECHA
            fecha = obtener_nombre(service, id_documento)
            procesar_nombre(fecha, nombre_carpeta)
            funcion = procesar_nombre(fecha, nombre_carpeta)
            for i in range(len(funcion)):
                nueva_fila.append(funcion[i])
            #LOCAL
            nueva_fila.append(nombre_local)
            #DISTRITO
            if nombre_local == "Santa Rosa":
                nueva_fila.append("Ate Vitarte")
            else:
                nueva_fila.append("Santa Anita")
            #DEPARTAMENTO
            nueva_fila.append("Lima")
            contador+=1
            datos_nuevo.append(nueva_fila)
        else: 
            continue
        
        
    return datos_nuevo

def buscar_celdas_2(id_documento, nombre_local, nombre_carpeta):
    sheet = service2.spreadsheets()
    datos_nuevo = []
    codigos = "REPORTE DIARIO!A4:U98"
    resultado = sheet.values().get(spreadsheetId=id_documento,
                                range=codigos).execute()
    values = resultado.get('values', [])
    lista_limpia = limpiar_lista(values)
    contador = 0
    for i in lista_limpia:
        if i[3] not in lista_excepcion:
            nueva_fila = []
            nueva_fila.append(i[3]) #SABOR
            if i[0] == "":
                nueva_fila.append(datos_nuevo[contador-1][1])
            else:    
                nueva_fila.append(i[0]) #TAMAÑO
            if i[2] == "":
                nombre = i[3]
                tam = nueva_fila[1]
                codigo = nombre[:3] + nombre[-3:] + tam[:3]
                nueva_fila.append(codigo) #CODIGO
            else:
                nueva_fila.append(i[2]) 
            if (i[9] != '' and i[9] != ' ') and (i[19] != '' and i[19] != ' '): #UNIDADES VENDIDAS
                unidades = int(i[9]) + int(i[19])
                nueva_fila.append(unidades)
            elif (i[9] == '' or i[9] == ' ') and (i[19] != '' and i[19] != ' '):
                unidades = int(i[19])
                nueva_fila.append(unidades)
            elif (i[9] != '' and i[9] != ' ') and (i[19] == '' or i[19] == ' '):
                unidades = int(i[9])
                nueva_fila.append(unidades)
            else: 
                unidades = 0
                nueva_fila.append(unidades)        
            if (i[8] != '' and i[8] != ' ') and (i[18] != '' and i[18] != ' '): #UNIDADES RETIRADAS
                retirados = int(i[8]) + int(i[18])
                nueva_fila.append(retirados)
            elif (i[8] == '' or i[8] == ' ') and (i[18] != '' and i[18] != ' '):
                retirados = int(i[18])
                nueva_fila.append(retirados)
            elif (i[8] != '' and i[8] != ' ') and (i[18] == '' or i[18] == ' '):
                retirados = int(i[8])
                nueva_fila.append(retirados)
            else: 
                retirados = 0
                nueva_fila.append(retirados)
            
            #PRECIO DEL DIA
            if i[11] != '' and i[11] != ' ':    
                nueva_fila.append(int(float(i[11])))
            else:
                nueva_fila.append(0)         
            #OBTENER FECHA
            fecha = obtener_nombre(service, id_documento)
            procesar_nombre(fecha, nombre_carpeta)
            funcion = procesar_nombre(fecha, nombre_carpeta)
            for i in range(len(funcion)):
                nueva_fila.append(funcion[i])
            #LOCAL
            nueva_fila.append(nombre_local)
            #DISTRITO
            if nombre_local == "Santa Rosa":
                nueva_fila.append("Ate Vitarte")
            else:
                nueva_fila.append("Santa Anita")
            #DEPARTAMENTO
            nueva_fila.append("Lima")
            datos_nuevo.append(nueva_fila)
            contador+=1
        else:
            continue
    print("Se ha procesado correctamente el archivo Excel de fecha "+funcion[3]+" del local "+nombre_local)
    return datos_nuevo

""" datos = buscar_celdas(SAMPLE_SPREADSHEET_ID)
df = pd.DataFrame(data= datos)
print(df) """

def datos_etl():
    ficha1 = open('lista_id_folder_local.pckl', 'rb')
    lista_locales = pickle.load(ficha1)
    ficha1.close()
    ficha2 = open('lista_id_folder_mes_automatizado.pckl', 'rb')
    lista_meses = pickle.load(ficha2)
    ficha2.close()
    ficha4 = open('lista_nuevos_sheet.pckl', 'rb')
    lista_nuevos_sheet = pickle.load(ficha4)
    ficha4.close()
    lista_nombres_local = ["Vista Alegre", "Virreyes", "Santa Rosa", "Santa Anita", "Los Ángeles"]
    contador = 0
    datos_agregar = []
    for i in lista_locales:
        query_extra = " and '"+i+"'"+" in parents"
        page_token = None
        while True:
            response1 = service.files().list(q="mimeType='application/vnd.google-apps.folder'"+query_extra,
                                            spaces='drive',
                                            fields='nextPageToken, files(id, name)',
                                            pageToken=page_token).execute()
            for file in response1.get('files', []):
                mes_anio = file.get('name')
                query_extra_2 = " and '"+file.get('id')+"'"+" in parents"
                page_token_2 = None
                while True:
                    response2 = service.files().list(q="mimeType='application/vnd.google-apps.spreadsheet'"+query_extra_2+" and trashed = false",
                                                    spaces='drive',
                                                    fields='nextPageToken, files(id, name)',
                                                    pageToken=page_token_2).execute()
                    for file_2 in response2.get('files', []):
                        if file_2.get('id') in lista_nuevos_sheet:
                            data = buscar_celdas_2(file_2.get('id'), lista_nombres_local[contador], mes_anio)
                            datos_agregar.extend(data)                          
                    page_token_2 = response2.get('nextPageToken', None)
                    if page_token_2 is None:
                        break
                    
            page_token = response1.get('nextPageToken', None)
            if page_token is None:
                break
        contador += 1
        
    df = pd.DataFrame(data= datos_agregar, columns=["sabor", "tamanio", "codigo", "unidades_vendidas", 
                                        "unidades_desechadas", "precio_dia", "dia", "mes", 
                                        "anio", "fecha", "semana", "dia_nombre", "nombre_local", "distrito", "departamento"])

    df = df[["codigo", "sabor", "tamanio", "unidades_vendidas", 
            "unidades_desechadas", "precio_dia", "dia", "semana", "mes", 
            "anio", "fecha", "dia_nombre", "nombre_local", "distrito", "departamento"]]
    
    df.to_sql('registro_nuevo', engine, if_exists="append", index=False)
    
    return print('No ocurrieron errores')

""" datos = buscar_celdas_2("1D_9JQwhFj_yeJxF3uUgx2Lj58d0UygXcX1ICvbV7XhA", "Los Ángeles", "AGOSTO-2021")
df = pd.DataFrame(data= datos, columns=["sabor", "tamanio", "codigo", "unidades_vendidas", 
                                        "unidades_desechadas", "precio_dia", "dia", "mes", 
                                        "anio", "fecha", "semana", "dia_nombre", "nombre_local", "distrito", "departamento"])
df = df[["codigo", "sabor", "tamanio", "unidades_vendidas", 
        "unidades_desechadas", "precio_dia", "dia", "semana", "mes", 
        "anio", "fecha", "dia_nombre", "nombre_local", "distrito", "departamento"]]
print(df)

engine = create_engine('postgresql+psycopg2://postgres:12345678@localhost:5432/DBTortelin')
df.to_sql('registro_tortelin', engine, if_exists="append", index=False) """

engine = create_engine('postgresql+psycopg2://postgres:12345678@localhost:5432/DBTortelin')



#EJECUCIÓN CON EVALUACION PICKLES
def ejecutar_mes_pick(nombre_local, nombre_carpeta, id_carpeta_mes):
    data_frame_mes = []
    ficha_mes = open('lista_id_folder_mes_automatizado.pckl', 'rb')
    list_mes = pickle.load(ficha_mes)
    ficha_mes.close()
    ficha = open('lista_id_excel_ms.pckl', 'rb')
    n_lista_xlsx = pickle.load(ficha)
    ficha.close()
    query_extra = " and '"+id_carpeta_mes+"'"+" in parents"
    page_token = None
    while True:
        response = service.files().list(q="mimeType='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'"+query_extra+" and trashed = false",
                                        spaces='drive',
                                        fields='nextPageToken, files(id, name)',
                                        pageToken=page_token).execute()
        for file in response.get('files', []):
            # Proceso de conversión
            if file.get('id') not in n_lista_xlsx: 
                #Si no está en la lista, se cambia el mimetype y se agrega a la lista
                print("se ha encontrado un nuevo excel xlsx "+file.get('id')+ " de la carpeta "+id_carpeta_mes)
                n_lista_xlsx.append(file.get('id'))
                update_file(service, file.get('id'), id_carpeta_mes) #Modificando el mimetype
                print("se ha convertido el excel "+file.get('id')+ " de la carpeta "+id_carpeta_mes)
            else:
                print("el archivo excel "+ file.get('id')+" ya está en el pickle de excels xlsx, no se procesan")
        page_token = response.get('nextPageToken', None)
        if page_token is None:
            print("se ha terminado con la conversión de xlsx")
            break
    ficha = open('lista_id_excel_sheet.pckl', 'rb')
    n_lista_sheet = pickle.load(ficha)
    ficha.close()
    query_extra_2 = " and '"+id_carpeta_mes+"'"+" in parents"
    page_token_2 = None
    while True:
        response2 = service.files().list(q="mimeType='application/vnd.google-apps.spreadsheet'"+query_extra_2+" and trashed = false",
                                        spaces='drive',
                                        fields='nextPageToken, files(id, name)',
                                        pageToken=page_token_2).execute()
        for file in response2.get('files', []):
            if file.get('id') not in n_lista_sheet: 
                print("se ha encontrado un nuevo sheet de ID "+file.get('id'))
                n_lista_sheet.append(file.get('id'))
                datos = buscar_celdas_2(file.get('id'), nombre_local, nombre_carpeta)
                data_frame_mes.extend(datos)
            else:
                print("el archivo excel "+ file.get('id')+" ya está en el pickle de sheets, no se procesan")
        page_token_2 = response2.get('nextPageToken', None)
        if page_token_2 is None:
            print("se ha terminado el procesamiento de sheets")
            break
    
    df = pd.DataFrame(data= data_frame_mes, columns=["sabor", "tamanio", "codigo", "unidades_vendidas", 
                                        "unidades_desechadas", "precio_dia", "dia", "mes", 
                                        "anio", "fecha", "semana", "dia_nombre", "nombre_local", "distrito", "departamento"])

    df = df[["codigo", "sabor", "tamanio", "unidades_vendidas", 
            "unidades_desechadas", "precio_dia", "dia", "semana", "mes", 
            "anio", "fecha", "dia_nombre", "nombre_local", "distrito", "departamento"]]
    
    df.to_sql('registro_nuevo', engine, if_exists="append", index=False)
    
    list_mes.append(id_carpeta_mes)
    fichero = open('lista_id_folder_mes_automatizado.pckl', 'wb')
    pickle.dump(list_mes, fichero)
    fichero.close()
    fichero_2 = open('lista_id_excel_ms.pckl', 'wb')
    pickle.dump(n_lista_xlsx, fichero_2)
    fichero_2.close()
    fichero_3 = open('lista_id_excel_sheet.pckl', 'wb')
    pickle.dump(n_lista_sheet, fichero_3)
    fichero_3.close()
    
    return print("la carpeta "+nombre_carpeta+ " del local "+nombre_local+" se ejecutó sin problemas")

#EJECUCIÓN SIN EVALUACION PICKLES
def ejecutar_mes_nopick(nombre_local, nombre_carpeta, id_carpeta_mes):
    data_frame_mes = []
    ficha_mes = open('lista_id_folder_mes_automatizado.pckl', 'rb')
    list_mes = pickle.load(ficha_mes)
    ficha_mes.close()
    ficha = open('lista_id_excel_ms.pckl', 'rb')
    n_lista_xlsx = pickle.load(ficha)
    ficha.close()
    query_extra = " and '"+id_carpeta_mes+"'"+" in parents"
    page_token = None
    cont1 = 1
    while True:
        response = service.files().list(q="mimeType='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'"+query_extra+" and trashed = false",
                                        spaces='drive',
                                        fields='nextPageToken, files(id, name)',
                                        pageToken=page_token).execute()
        for file in response.get('files', []):
            n_lista_xlsx.append(file.get('id'))
            update_file(service, file.get('id'), id_carpeta_mes) #Modificando el mimetype
            print("se ha convertido el excel "+file.get('id')+ " de la carpeta "+id_carpeta_mes)
            print("van "+str(cont1)+ " archivos convertidos")
            cont1+=1
        page_token = response.get('nextPageToken', None)
        if page_token is None:
            print("se ha terminado con la conversión de xlsx")
            break
    ficha = open('lista_id_excel_sheet.pckl', 'rb')
    n_lista_sheet = pickle.load(ficha)
    ficha.close()
    query_extra_2 = " and '"+id_carpeta_mes+"'"+" in parents"
    page_token_2 = None
    cont2 = 1
    while True:
        response2 = service.files().list(q="mimeType='application/vnd.google-apps.spreadsheet'"+query_extra_2+" and trashed = false",
                                        spaces='drive',
                                        fields='nextPageToken, files(id, name)',
                                        pageToken=page_token_2).execute()
        for file in response2.get('files', []):
            print("se ha encontrado un sheet de ID "+file.get('id'))
            n_lista_sheet.append(file.get('id'))
            datos = buscar_celdas_2(file.get('id'), nombre_local, nombre_carpeta)
            print("van "+str(cont2)+ " archivos procesados")
            data_frame_mes.extend(datos)
            cont2+=1
        page_token_2 = response2.get('nextPageToken', None)
        if page_token_2 is None:
            print("se ha terminado el procesamiento de sheets")
            break
    
    df = pd.DataFrame(data= data_frame_mes, columns=["sabor", "tamanio", "codigo", "unidades_vendidas", 
                                        "unidades_desechadas", "precio_dia", "dia", "mes", 
                                        "anio", "fecha", "semana", "dia_nombre", "nombre_local", "distrito", "departamento"])

    df = df[["codigo", "sabor", "tamanio", "unidades_vendidas", 
            "unidades_desechadas", "precio_dia", "dia", "semana", "mes", 
            "anio", "fecha", "dia_nombre", "nombre_local", "distrito", "departamento"]]
    
    df.to_sql('registro_nuevo', engine, if_exists="append", index=False)
    
    list_mes.append(id_carpeta_mes)
    fichero = open('lista_id_folder_mes_automatizado.pckl', 'wb')
    pickle.dump(list_mes, fichero)
    fichero.close()
    fichero_2 = open('lista_id_excel_ms.pckl', 'wb')
    pickle.dump(n_lista_xlsx, fichero_2)
    fichero_2.close()
    fichero_3 = open('lista_id_excel_sheet.pckl', 'wb')
    pickle.dump(n_lista_sheet, fichero_3)
    fichero_3.close()
    
    return print("la carpeta "+nombre_carpeta+ " del local "+nombre_local+" se ejecutó sin problemas")

#ejecutar_mes_nopick("Los Ángeles", "MAYO-2021", "1qnxMKe5-KfgDbLB_PV5Fn8Frs8LLnkHH")
#ejecutar_mes_nopick("Los Ángeles", "AGOSTO-2021", "13pqzbAai6X16QzwIUJ-b46cLQAQ9K6Di")
#ejecutar_mes_nopick("Los Ángeles", "JUNIO-2021", "1l3avWSXs2-fgK2629d32M2NC-jWYJimA")
#ejecutar_mes_nopick("Los Ángeles", "JULIO-2021", "1LAALvb0VjtRDWHeyHgkdZ3CD04GZ-qhq")
#ejecutar_mes_nopick("Los Ángeles", "SEPTIEMBRE-2021", "1f241LvSpE6WZTFp8vJLc1GDZkbskMTsl")
#ejecutar_mes_nopick("Los Ángeles", "OCTUBRE-2021", "1JLvySmXEH8hG0uYFRJ3IapzPImnKlpbQ")
#ejecutar_mes_nopick("Los Ángeles", "NOVIEMBRE-2021", "16A5DR_IqyvAjlmpzNWMO1k3wRtcvQtdz")
#ejecutar_mes_nopick("Los Ángeles", "DICIEMBRE-2021", "1OPQoVqcZZJkuINgkBQ1Z4cobfHqL7tCh")
#ejecutar_mes_nopick("Los Ángeles", "ENERO-2022", "1QJ5_NHmKo5WtwbbtVzoSvvLENES-Tb2J")
#ejecutar_mes_nopick("Los Ángeles", "FEBRERO-2022", "1_HVOIOPA4SqXE-K9dIFyIO9otl4l706B")
#ejecutar_mes_nopick("Los Ángeles", "MARZO-2022", "1phXEFRbEyQ5ipMnXs6VfCpm8DxQBGsyA")
#ejecutar_mes_nopick("Los Ángeles", "ABRIL-2022", "1eXezKAoAJKXhFm9rdgElayTDIjNsp1Lc")
#ejecutar_mes_pick("Los Ángeles", "ABRIL-2022", "1eXezKAoAJKXhFm9rdgElayTDIjNsp1Lc")

#ejecutar_mes_nopick("Santa Anita", "MAYO-2021", "1sr3Q6PnrVDV7GkF7bU4ruPcmWjEGQRgh")
#ejecutar_mes_nopick("Santa Anita", "JUNIO-2021", "1Pg5tfAKoWSR0OGQnMFGDUjPS5okMm1BA")
#ejecutar_mes_nopick("Santa Anita", "JULIO-2021", "1Dyixiy_Cg88kQc2J064yx4zqPNFXw8Rx")
#ejecutar_mes_nopick("Santa Anita", "AGOSTO-2021", "1qnc9alx-zOnsP78kSOyC2fI0u_b5nbnm")
#ejecutar_mes_nopick("Santa Anita", "SEPTIEMBRE-2021", "1xtpJUBetfc-ksEdSyUwwWIJCUJkfBAq8")
#ejecutar_mes_nopick("Santa Anita", "OCTUBRE-2021", "1xYX-VbkdxZHIi7kdgSk0rxMNAmiAH1rS")
#ejecutar_mes_nopick("Santa Anita", "NOVIEMBRE-2021", "1McUMqS4-6sEXtKm-sc-wdm6cRD-dcdV1")
#ejecutar_mes_nopick("Santa Anita", "DICIEMBRE-2021", "165bfXScme1G-CIn5X060R32yFRskENma")
#ejecutar_mes_nopick("Santa Anita", "ENERO-2022", "185CMPSfcth5gVBRtuIie8TbvTB9PLMAc")
#ejecutar_mes_nopick("Santa Anita", "FEBRERO-2022", "1B4VPrIlC_uXiMd8skW32-RsCfjOq0Vsr")
#ejecutar_mes_nopick("Santa Anita", "MARZO-2022", "1C6uMk8zNZvh_9vi0qQicAsDfQX7XUqFR")
#ejecutar_mes_nopick("Santa Anita", "ABRIL-2022", "1C5Ik50sV1u8wBDELw0YM61zRSBg6CRCI")

#ejecutar_mes_nopick("Santa Rosa", "MAYO-2021", "1JJZ0_-P8ZWRydzIyCuEsCJzHYHK26H-s")
#ejecutar_mes_nopick("Santa Rosa", "JUNIO-2021", "1W3PBKTxbPzInLjxNP6N8lp192tdsjTh5")
#ejecutar_mes_nopick("Santa Rosa", "JULIO-2021", "1rcmwEKcSR2wuaknoE7lk8AJBONTdv6rm")
#ejecutar_mes_nopick("Santa Rosa", "AGOSTO-2021", "1gJaXTT9HYlkCDtxj65vd8rUmMAUW-P71")
#ejecutar_mes_nopick("Santa Rosa", "SEPTIEMBRE-2021", "1_DI0ZoxoJfmDI0XPCgXIhrrpCmRCaZ_d")
#ejecutar_mes_nopick("Santa Rosa", "OCTUBRE-2021", "1tT6Hg7AmXAR7ERzBiBjcZU9GX6UAdqVm")
#ejecutar_mes_nopick("Santa Rosa", "NOVIEMBRE-2021", "1L3b_U30z2osg184SclgzwGph02Ca0fRg")
#ejecutar_mes_nopick("Santa Rosa", "DICIEMBRE-2021", "1qide-jodvsnYE8G4QJVhgenQeN2_JTpU")
#ejecutar_mes_nopick("Santa Rosa", "ENERO-2022", "1etzNKA0mNSU34wXp1BlibnkrcXlbHmBj")
#ejecutar_mes_nopick("Santa Rosa", "FEBRERO-2022", "1A4zyzjxgqNqXPOQWrOKtyTBHDlwsVFpM")
#ejecutar_mes_nopick("Santa Rosa", "MARZO-2022", "1kiVvGFJTZluqMCnerxoHFibbKyD4oNyZ")
#ejecutar_mes_nopick("Santa Rosa", "ABRIL-2022", "1LhgcXpgEx8xa13TMhCJDSIeVkcbFvNqN")

#ejecutar_mes_nopick("Virreyes", "MAYO-2021", "1sYLFYmfy0OyNb-0ZBBvbuVNkNbj1bbSm")
#ejecutar_mes_nopick("Virreyes", "JUNIO-2021", "1g5QnWDxgMmjMMDXGdWhzAXIgyc17BSan")
#ejecutar_mes_nopick("Virreyes", "JULIO-2021", "1yts4krRcquajdHCx7Hk9lhh2uI9EUz9p")
#ejecutar_mes_nopick("Virreyes", "AGOSTO-2021", "1E2lmDyP2029LHuczVwTNU8PetDnN5w_K")
#ejecutar_mes_nopick("Virreyes", "SEPTIEMBRE-2021", "1rACCvceRfEC64NpsKGeRjKHno2Iyjhjd")
#ejecutar_mes_nopick("Virreyes", "OCTUBRE-2021", "1wpQevLMDaqru76UEQXz2IxbqAsiU0Rui")
#ejecutar_mes_nopick("Virreyes", "NOVIEMBRE-2021", "1C3V1DtnOH0iUOYYnBSKSO9h3i-auRmkc")
#ejecutar_mes_nopick("Virreyes", "DICIEMBRE-2021", "1TIJROn4LaJkpS6rUyvxCOjNIwh_fPb-f")
#ejecutar_mes_nopick("Virreyes", "ENERO-2022", "1yq455RDxjxF26Ekz2KijckB8ODA4ZjNm")
#ejecutar_mes_nopick("Virreyes", "FEBRERO-2022", "1eG2cF3Erki7XAz2nQiEJSdRemRjVWCAp")
#ejecutar_mes_nopick("Virreyes", "MARZO-2022", "11OQzkLIdMecrLZ3vbvbtfxEK70CgW25a")
#ejecutar_mes_nopick("Virreyes", "ABRIL-2022", "1_MyCloipaSxPOaEX5V024iY8i5RH5QQJ")

#ejecutar_mes_nopick("Vista Alegre", "MAYO-2021", "1WmpCUIDRJBSpVz54DmdStB-TsAIH9CuG")
#ejecutar_mes_nopick("Vista Alegre", "JUNIO-2021", "1s4zAy2DkbnLSRV_A0cIKn0oJz_0xZeU6")
#ejecutar_mes_nopick("Vista Alegre", "JULIO-2021", "1UV2BDFujiUOzRK0sv01KiMhlSF-fQEQK")
#ejecutar_mes_nopick("Vista Alegre", "AGOSTO-2021", "11pkllolLtIucbKs9qVaINycJlsKrVMjD")
#ejecutar_mes_nopick("Vista Alegre", "SEPTIEMBRE-2021", "1WIX-qZkLAYAo_jxNPuKqjavLmRcIAl65")
#ejecutar_mes_nopick("Vista Alegre", "OCTUBRE-2021", "10SPWfyq5JlZcDxoWA_mh3ZR4XRpOVg3N")
#ejecutar_mes_nopick("Vista Alegre", "NOVIEMBRE-2021", "1LalaiMCf-gCVzzrXinrguY-flojl3g3k")
#ejecutar_mes_nopick("Vista Alegre", "DICIEMBRE-2021", "17Gg6jumpDwY3hrYvY9ioyKEkqU-EV2rM")
#ejecutar_mes_nopick("Vista Alegre", "ENERO-2022", "19yT8UmFXVQXMMPktB9KlY1ZFyzuM3DRd")
#ejecutar_mes_nopick("Vista Alegre", "FEBRERO-2022", "1XS-e-QTGPK5-2Ou5Vh_mdCvRNa8kWI7h")
#ejecutar_mes_nopick("Vista Alegre", "MARZO-2022", "1p72W1PvXIiXk5GZIYNKy9vqYRWRR-y9P")
#ejecutar_mes_nopick("Vista Alegre", "ABRIL-2022", "1X0aGmiXtBKkt_TBFQxTU-Bx4kiBZ1jfq")

#--------------------------------------------------------------------------------


def mostrar_fechas_faltantes_procesar():
    lista_de_listas = []
    ficha = open('lista_id_excel_sheet.pckl', 'rb')
    lista_sheets = pickle.load(ficha)
    ficha.close()
    ficha2 = open('lista_id_folder_mes_automatizado.pckl', 'rb')
    lista_mes = pickle.load(ficha2)
    ficha2.close()
    for i in lista_mes:
        query_extra_2 = " and '"+i+"'"+" in parents"
        page_token_2 = None
        cont2 = 1
        while True:
            response2 = service.files().list(q="mimeType='application/vnd.google-apps.spreadsheet'"+query_extra_2+" and trashed = false",
                                            spaces='drive',
                                            fields='nextPageToken, files(id, name)',
                                            pageToken=page_token_2).execute()
            for file in response2.get('files', []):
                if file.get('id') not in lista_sheets:
                    print("Falta el archivo "+ file.get('name')+" y ID "+file.get('id'))
                    lista_append = file.get('id')
                    lista_de_listas.append(lista_append)
                else:
                    continue
            page_token_2 = response2.get('nextPageToken', None)
            if page_token_2 is None:
                print("se ha terminado la búsqueda de sheets faltantes en la carpeta "+i)
                break
    return lista_de_listas

#mostrar_fechas_faltantes_procesar()

def mostrar_fechas_faltantes_convertir():
    ficha = open('lista_id_excel_ms.pckl', 'rb')
    lista_ex_ms = pickle.load(ficha)
    ficha.close()
    ficha2 = open('lista_id_folder_mes_automatizado.pckl', 'rb')
    lista_mes = pickle.load(ficha2)
    ficha2.close()
    for i in lista_mes:
        query_extra_2 = " and '"+i+"'"+" in parents"
        page_token_2 = None
        cont2 = 1
        while True:
            response2 = service.files().list(q="mimeType='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'"+query_extra_2+" and trashed = false",
                                            spaces='drive',
                                            fields='nextPageToken, files(id, name)',
                                            pageToken=page_token_2).execute()
            for file in response2.get('files', []):
                if file.get('id') not in lista_ex_ms:
                    print("Falta el archivo "+ file.get('name')+" y ID "+file.get('id'))
                else:
                    continue
            page_token_2 = response2.get('nextPageToken', None)
            if page_token_2 is None:
                print("se ha terminado la búsqueda de excels faltantes en la carpeta "+i)
                break
    return print("listo")

#mostrar_fechas_faltantes_convertir()

""" if __name__ == "__main__": 
    
    
    """
    
def ejecutar_mes_noviembre_2021_santa_anita(nombre_local, nombre_carpeta, id_carpeta_mes):
    data_frame_mes = []
    query_extra_2 = " and '"+id_carpeta_mes+"'"+" in parents"
    page_token_2 = None
    cont2 = 1
    while True:
        response2 = service.files().list(q="mimeType='application/vnd.google-apps.spreadsheet'"+query_extra_2+" and trashed = false",
                                        spaces='drive',
                                        fields='nextPageToken, files(id, name)',
                                        pageToken=page_token_2).execute()
        for file in response2.get('files', []):
            print("se ha encontrado un sheet de ID "+file.get('id'))
            datos = buscar_celdas_2(file.get('id'), nombre_local, nombre_carpeta)
            print("van "+str(cont2)+ " archivos procesados")
            data_frame_mes.extend(datos)
            cont2+=1
        page_token_2 = response2.get('nextPageToken', None)
        if page_token_2 is None:
            print("se ha terminado el procesamiento de sheets")
            break
    
    df = pd.DataFrame(data= data_frame_mes, columns=["sabor", "tamanio", "codigo", "unidades_vendidas", 
                                        "unidades_desechadas", "precio_dia", "dia", "mes", 
                                        "anio", "fecha", "semana", "dia_nombre", "nombre_local", "distrito", "departamento"])

    df = df[["codigo", "sabor", "tamanio", "unidades_vendidas", 
            "unidades_desechadas", "precio_dia", "dia", "semana", "mes", 
            "anio", "fecha", "dia_nombre", "nombre_local", "distrito", "departamento"]]
    
    df.to_sql('registro_nuevo', engine, if_exists="append", index=False)
    
    return print("la carpeta "+nombre_carpeta+ " del local "+nombre_local+" se ejecutó sin problemas")

#ejecutar_mes_noviembre_2021_santa_anita("Santa Anita", "NOVIEMBRE-2021", "1McUMqS4-6sEXtKm-sc-wdm6cRD-dcdV1")

def dia_unico(id_loc, nombre_local, nombre_carpeta):
    datos = buscar_celdas_2(id_loc, nombre_local, nombre_carpeta)
    df = pd.DataFrame(data= datos, columns=["sabor", "tamanio", "codigo", "unidades_vendidas", 
                                            "unidades_desechadas", "precio_dia", "dia", "mes", 
                                            "anio", "fecha", "semana", "dia_nombre", "nombre_local", "distrito", "departamento"])

    df = df[["codigo", "sabor", "tamanio", "unidades_vendidas", 
                "unidades_desechadas", "precio_dia", "dia", "semana", "mes", 
                "anio", "fecha", "dia_nombre", "nombre_local", "distrito", "departamento"]]
        
    df.to_sql('registro_nuevo', engine, if_exists="append", index=False)
    return print("se proceso correctamente")

def datos_faltantes(nombre_loc, id_loc):
    datos_data_faltantes = []
    l_faltantes = mostrar_fechas_faltantes_procesar()
    nombre_local = nombre_loc
    id_local = id_loc
    ficha2 = open('lista_id_folder_mes_automatizado.pckl', 'rb')
    lista_meses = pickle.load(ficha2)
    ficha2.close()
    ficha4 = open('lista_id_excel_sheet.pckl', 'rb')
    lista_sheet_completo = pickle.load(ficha4)
    ficha4.close()
    query_extra = " and '"+id_local+"'"+" in parents"
    page_token = None
    while True:
        response1 = service.files().list(q="mimeType='application/vnd.google-apps.folder'"+query_extra,
                                        spaces='drive',
                                        fields='nextPageToken, files(id, name)',
                                        pageToken=page_token).execute()
        for file in response1.get('files', []):
            mes_anio = file.get('name')
            query_extra_2 = " and '"+file.get('id')+"'"+" in parents"
            page_token_2 = None
            while True:
                response2 = service.files().list(q="mimeType='application/vnd.google-apps.spreadsheet'"+query_extra_2,
                                                spaces='drive',
                                                fields='nextPageToken, files(id, name)',
                                                pageToken=page_token_2).execute()
                for file_2 in response2.get('files', []):
                    id_sheet = file_2.get('id')
                    if id_sheet in l_faltantes:
                        datos_x = buscar_celdas_2(id_sheet, nombre_local, mes_anio)
                        datos_data_faltantes.extend(datos_x)
                        lista_sheet_completo.append(id_sheet)                           
                page_token_2 = response2.get('nextPageToken', None)
                if page_token_2 is None:
                    break
            #Ignorando los archivos que ya fueron procesados.
        page_token = response1.get('nextPageToken', None)
        if page_token is None:
            break
    
    if len(datos_data_faltantes) == 0:
        print("no hay nada que agregar")
    else:
        print(datos_data_faltantes)
        df = pd.DataFrame(data= datos_data_faltantes, columns=["sabor", "tamanio", "codigo", "unidades_vendidas", 
                                        "unidades_desechadas", "precio_dia", "dia", "mes", 
                                        "anio", "fecha", "semana", "dia_nombre", "nombre_local", "distrito", "departamento"])

        df = df[["codigo", "sabor", "tamanio", "unidades_vendidas", 
                "unidades_desechadas", "precio_dia", "dia", "semana", "mes", 
                "anio", "fecha", "dia_nombre", "nombre_local", "distrito", "departamento"]]
    
        df.to_sql('registro_nuevo', engine, if_exists="append", index=False)
           
    ficha_3 = open('lista_id_excel_sheet.pckl', 'wb')
    pickle.dump(lista_sheet_completo, ficha_3)
    ficha_3.close()
    
    return print('no hay error')

#datos_faltantes("Los Ángeles", "1pcpL_gLJJlKV1gwte1OCNQHHDmS3QuHD")
#datos_faltantes("Santa Anita", "1EOVeHNuGBmPEz-4U0cgOjfDWpL7Z0P1B")
#datos_faltantes("Santa Rosa", "18cLhT9aOMf64DDjJpxThUf_eFId8E_im")
#datos_faltantes("Virreyes", "1yKVy7x85ZaS5rGkD9wRdJIwhOMvAor3I")
#datos_faltantes("Vista Alegre", "1vSsUKo-ckO_za-eY1bXkufpz_wbvQdWv")


""" def expecion(id_doc, nombre_loc, nombre_carpeta):
    dato = buscar_celdas_2(id_doc, nombre_loc, nombre_carpeta)
    df = pd.DataFrame(data= dato, columns=["sabor", "tamanio", "codigo", "unidades_vendidas", 
                                        "unidades_desechadas", "precio_dia", "dia", "mes", 
                                        "anio", "fecha", "semana", "dia_nombre", "nombre_local", "distrito", "departamento"])

    df = df[["codigo", "sabor", "tamanio", "unidades_vendidas", 
            "unidades_desechadas", "precio_dia", "dia", "semana", "mes", 
            "anio", "fecha", "dia_nombre", "nombre_local", "distrito", "departamento"]]
    
    df.to_sql('registro_nuevo', engine, if_exists="append", index=False)
    ficha = open() """

def back_pick():
    ficha_excel = open('lista_id_excel_ms.pckl', 'rb') #Abriendo pickle excels xlsx
    lista_excel = pickle.load(ficha_excel)
    ficha_excel.close()
    ficha_sheets = open('lista_id_excel_sheet.pckl', 'rb') #Abriendo pickle sheets
    lista_sheets = pickle.load(ficha_sheets)
    ficha_sheets.close()
    ficha_mes = open('lista_id_folder_mes_automatizado.pckl', 'rb') #Abriendo pickle meses
    lista_mes = pickle.load(ficha_mes)
    ficha_mes.close()
    ficha_back = open('back_pick.pckl', 'rb')
    lista_back = pickle.load(ficha_back)
    ficha_back.close()
    
    for i in lista_back:
        if i in lista_excel:
            lista_excel.remove(i)
        if i in lista_sheets:
            lista_sheets.remove(i)
        if i in lista_mes:
            lista_mes.remove(i)
                
    ficha_excel = open('lista_id_excel_ms.pckl', 'wb') #Abriendo pickle excels xlsx
    pickle.dump(lista_excel, ficha_excel)
    ficha_excel.close()
    ficha_sheets = open('lista_id_excel_sheet.pckl', 'wb') #Abriendo pickle sheets
    pickle.dump(lista_sheets, ficha_sheets)
    ficha_sheets.close()
    ficha_mes = open('lista_id_folder_mes_automatizado.pckl', 'wb') #Abriendo pickle meses
    pickle.dump(lista_mes, ficha_mes)
    ficha_mes.close()
    nueva_pick = []
    ficha_pick = open('back_pick.pckl', 'wb')
    pickle.dump(nueva_pick, ficha_pick)
    
    return None

def borrando_extras():
    ficha = open('lista_nuevos_sheet.pckl', 'rb')
    lista_sheet = pickle.load(ficha)
    ficha.close()
    lista_prov = []
    for i in lista_sheet:
        delete_file(service, i)
        lista_prov.append(i)
    for i in lista_prov:
        lista_sheet.remove(i)
    ficha2 = open('lista_nuevos_sheet.pckl', 'wb')
    pickle.dump(lista_sheet, ficha2)
    ficha2.close()

    return None

def ejecutar_proceso_completo():
    try:
        search_folders()
        search_and_convert_excels_ms()
        listar_new_excels_sheets()
        datos_etl()
        borrando_extras()
        print("Proceso Completado")
    except Exception as e:
        print(e)        
        back_pick()
        print("A ocurrido un error, se retorna al estado antes de la ejecución")
    return None
    
ejecutar_proceso_completo()

