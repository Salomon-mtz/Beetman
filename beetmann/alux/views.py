from django.http import HttpResponse

# Módulos estándar de Python
import csv
import io
import json
import math
import os
import urllib.request
from datetime import date, datetime, timedelta
import numpy as np
import time
import numpy

import flask
import flask_excel as excel
import investpy
from numpy.lib.type_check import _nan_to_num_dispatcher
import pandas as pd
import pyodbc
import requests
import xml.etree.ElementTree as et # Agregar al views / Comparar por otras librerías
import xlrd
import yfinance as yf
from dateutil import relativedelta
from flask import abort, flash, jsonify, make_response, redirect, render_template, request, send_file, send_from_directory, session, url_for
from flask_login import current_user, login_required, login_user, LoginManager, logout_user
from openpyxl import Workbook
from openpyxl.utils.dataframe import dataframe_to_rows
from werkzeug.utils import secure_filename
import math
import zipfile
from bs4 import BeautifulSoup
from zeep import Client
from . import models

import chunk
from fileinput import filename
from multiprocessing import context
from django.shortcuts import render, redirect
from django.http import HttpResponse
from django.template import loader
from django.contrib.auth import authenticate, login, logout
from django.contrib import messages
import json
from json import dumps
from django.views.decorators.csrf import csrf_exempt
import ast #para diccionario
import sqlite3
from django.contrib.auth.models import User
from django.contrib.auth.decorators import login_required   
import hashlib
# from python_webapp_flask import app
# from python_webapp_flask import cache

# import internal_libraries.conciliacion as cn
# import internal_libraries.ofertasDeCompra as odc
# import internal_libraries.preciosDelMEM as pdM
# from internal_libraries import ofertaElectrica as oE
# from internal_libraries import tarifaCFE as tCFE
# from internal_libraries import Facturacion as factur
# from internal_libraries import cincominutales as cinc
# from internal_libraries import generacionElectrica as gE
# from internal_libraries import rentabilidad as rentability


# TODO: Pasar la funcion de validarFechas de los html individuales a test.html para que todos la hereden

class VariablesGlobales():
    
    excel_lista_global = ""
    
    fechas_pdm_min = ("2018-01-01", "2018-01-01", "2018-01-01", 
                      "2017-01-11", "2018-01-01", "2018-01-01", 
                      "2016-01-29", "2016-01-27", "2016-03-23", 
                      "2017-01-11", "2017-01-11", "2017-01-13")
    
    fechas_c_min = ("1988-06-27",)
    
    fechas_d_min = ("2019-01-11", "2019-07-11", "2019-07-11", "2019-09-02")

    fechas_ea_min = ("2016-01-29",)

    fechas_gde_min = ("2016-05-01", "2016-01-27")

    fechas_odc_min = ("2016-01-29", "2016-01-27", "2016-03-23", 
                      "2017-01-01", "2017-01-01", "2020-04-23")

    fechas_odv_min = ("2016-01-29", "2016-01-27", "2016-03-23", 
                      "2017-01-11", "2017-12-01", "2017-01-27", 
                      "2016-01-29", "2017-01-11", "2018-01-01", 
                      "2018-01-01", "2016-01-29", "2016-01-27", 
                      "2016-07-20", "2017-01-27", "2017-01-27", 
                      "2019-05-04", "2016-01-29", "2016-01-27", 
                      "2017-01-27", "2017-01-27")

    fechas_sc_min = ("2018-01-01", "2017-01-11", "2019-01-01", 
                     "2019-01-01", "2019-01-02", "2019-01-01", 
                     "2019-01-01", "2019-01-01")

    fechas_t_min = ("2018-01",)

    caso_odc = 0



IVA = 1.16
    # fechas_eodc_min = ("2021-01-01",)
    # Fecha minima para envio de ofertas de compra (promedio y dia mas parecido)

def validar_formulario(r):
    """
    Validar que el usuario haya proporcionado correctamente los datos del formulario.
    En la condicional se agregan solo los datos que podrían no tener información.
    
    Si algun atributo no existe en el formulario no es tomado en cuenta, 
    pero si existe y no esta definido entonces la funcion devolvera un False.

    Parametros
    ----------
    r : werkzeug.local.LocalProxy
            El objeto "request" que contiene todos los datos que se envían del cliente al servidor

    Retorna
    -------
    booleano
        Una variable boolea na usada para informar si el usuario ha proporcionado correctamente los datos del formulario
    """

    if r.form.get("fecha") == "" or r.form.get("fecha_2") == "":
        print("HOHOHOHO")
        flash("Seleccione correctamente la(s) fecha(s)")

        return False

    return True


login_manager = LoginManager()
#login_manager.init_app(app)
login_manager.login_view = "login"

#print('-------------------------')
# Lista de usuarios, correos y contraseñas.
name=['Mercados','Administracion', 'Zubex', 'Urrea','Fandeli','Comercial']
email=['Beetmann','Administracion','Zubex','Urrea','Fandeli','Beetmann_Comercial']
password=['MercadosBD20', 'admin','Zubex2022', 'Urrea2022','Fandeli2022','ComercialBTMNN']
rol=[4,2,1,1,1,3]
users = []


#Añadir a la lista users los usuarios que creamos
for contador in range(len(name)):
    user = models.User(len(users) + 1, name[contador], email[contador], password[contador],rol[contador])
    users.append(user)


# Parche utilizado momentaneamente para definir si una persona puede o no ver una pagina
def parche_rol(rol,lista):
    if rol in lista:
        return True
    return False

# Callback requerido por flask
@login_manager.user_loader
def load_user(user_id):
    for user in users:
        if user.id == int(user_id):
            return user
    return None

def signout():
    logout_user()
    return redirect(url_for("index"))


def crear_conexion_SQL():
    """Establecer la conexión con el servidor SQL."""

    try:

        direccion_servidor = "tcp:beetmann-energy.database.windows.net"
        nombre_bd = "mercados"
        nombre_usuario = "adm"
        password = "MercadosBD20"
        cnxn = pyodbc.connect("DRIVER={ODBC Driver 18 for SQL Server};SERVER="+direccion_servidor+";DATABASE="+nombre_bd+";UID="+nombre_usuario+";PWD="+password)
        
        return cnxn

    except:
        abort(501)
        return print("Error en la conexión con la base de datos")
    

def fechas_limite(bd_tabla, columna_nombre, cnxn):

    """Proporcionar la fecha de la fila de datos mas reciente.
    
    Se busca la fecha de la ultima fila de la tabla recibida de la base de datos.

    # Pendiente...
    Parametros
    ----------
    bd_tabla : werkzeug.local.LocalProxy
            El objeto "request" que contiene todos los datos que se envían del cliente al servidor
    columna_nombre
    cnxn

    Retorna
    -------
    booleano
        Una variable booleana usada para informar si el usuario ha proporcionado correctamente los datos del formulario
    """

    consulta_sql = """
                    SELECT 
                        max({fecha}) 
                    FROM {bd_tabla}
                    """
    consulta_sql = consulta_sql.format(bd_tabla=bd_tabla, fecha=columna_nombre)
    df = pd.read_sql(consulta_sql, cnxn)
    #fecha = str(df.iloc[0][columna_nombre])[:-9] # Hack temporal para sustituir el método .strftime("%Y/%m/%d")
    #print(df.iloc[0, 0])
    fecha = df.iloc[0, 0].strftime("%Y-%m-%d")
    #fecha = df.iloc[0][1].strftime("%Y-%m-%d")

    #print(fecha)
    return fecha



def obtener_fechas_limite():
    
    cnxn = crear_conexion_SQL()

    with cnxn:
        """
        VariablesGlobales.fechas_pdm_max = (fechas_limite("[dbo].[PML(MDA) (SIN)]", "[Fecha]", cnxn), 
                                            fechas_limite("[dbo].[PML(MDA) (BCA)]", "[Fecha]", cnxn), 
                                            fechas_limite("[dbo].[PML(MDA) (BCS)]", "[Fecha]", cnxn), 
                                            fechas_limite("[dbo].[PML(MTR) (SIN)]", "[Fecha]", cnxn), 
                                            fechas_limite("[dbo].[PML(MTR) (BCA)]", "[Fecha]", cnxn), 
                                            fechas_limite("[dbo].[PML(MTR) (BCS)]", "[Fecha]", cnxn), 
                                            fechas_limite("[dbo].[PMZ(MDA) (SIN)]", "[Fecha]", cnxn), 
                                            fechas_limite("[dbo].[PMZ(MDA) (BCA)]", "[Fecha]", cnxn), 
                                            fechas_limite("[dbo].[PMZ(MDA) (BCS)]", "[Fecha]", cnxn), 
                                            fechas_limite("[dbo].[PMZ(MTR) (SIN)]", "[Fecha]", cnxn), 
                                            fechas_limite("[dbo].[PMZ(MTR) (BCA)]", "[Fecha]", cnxn), 
                                            fechas_limite("[dbo].[PMZ(MTR) (BCS)]", "[Fecha]", cnxn))
        
        

        VariablesGlobales.fechas_pdm_max = (fechas_limite("[dbo].[PMZ(MDA) (SIN)]", "[Fecha]", cnxn), 
                                            fechas_limite("[dbo].[PMZ(MTR) (SIN)]", "[Fecha]", cnxn), 
                                            fechas_limite("[dbo].[PML(MDA) (SIN)]", "[Fecha]", cnxn), 
                                            fechas_limite("[dbo].[PML(MTR) (SIN)]", "[Fecha]", cnxn))
        VariablesGlobales.fechas_t_max = (fechas_limite("[dbo].[Tarifa DIST]", "[fecha]", cnxn)[:-3],)

        today = str(date.today())
        VariablesGlobales.fechas_c_max = (today, today, today)
        VariablesGlobales.fechas_edc_max = today

        VariablesGlobales.fechas_c_max = (str(date.today()),)

        
        VariablesGlobales.fechas_d_max = (fechas_limite("[dbo].[Demanda Pronosticada]", "[Fecha]", cnxn), 
                                          fechas_limite("[dbo].[Demanda Promedio Maxima]", "[Fecha]", cnxn), 
                                          fechas_limite("[dbo].[Demanda Promedio Diaria]", "[Fecha]", cnxn), 
                                          fechas_limite("[dbo].[Demanda Maxima Instantanea]", "[Fecha]", cnxn))
        
        VariablesGlobales.fechas_ea_max = (fechas_limite("[dbo].[Energia Asignada (MDA)]", "[Fecha]", cnxn),)
        
        VariablesGlobales.fechas_gde_max = (fechas_limite("[dbo].[Energia Generada por Tipo de Tecnologia L0]", "[Fecha]", cnxn), 
                                            fechas_limite("[dbo].[Pronosticos de generacion intermitente (MDA) (SIN)]", "[Fecha]", cnxn))

        VariablesGlobales.fechas_odc_max = (fechas_limite("[dbo].[Ofertas de compra SIN]", "[Fecha]", cnxn), 
                                            fechas_limite("[dbo].[Ofertas de compra BCA]", "[Fecha]", cnxn), 
                                            fechas_limite("[dbo].[Ofertas de compra BCS]", "[Fecha]", cnxn), 
                                            fechas_limite("[dbo].[Ofertas de Importacion (MDA)]", "[Fecha]", cnxn), 
                                            fechas_limite("[dbo].[Ofertas de exportacion (MDA)]", "[Fecha]", cnxn), 
                                            fechas_limite("[dbo].[L0 Valores Netos de Importacion y Exportacion]", "[Fecha]", cnxn))
        
        VariablesGlobales.fechas_odv_max = (fechas_limite("[dbo].[O.D.V Termo (MDA) (SIN)]", "[Fecha]", cnxn), 
                                            fechas_limite("[dbo].[O.D.V Termo (MDA) (BCA)]", "[Fecha]", cnxn), 
                                            fechas_limite("[dbo].[O.D.V Termo (MDA) (BCS)]", "[Fecha]", cnxn), 
                                            fechas_limite("[dbo].[O.D.V Termo (MTR) (SIN)]", "[Fecha]", cnxn), 
                                            fechas_limite("[dbo].[O.D.V Termo (MTR) (BCA)]", "[Fecha]", cnxn), 
                                            fechas_limite("[dbo].[O.D.V Termo (MTR) (BCS)]", "[Fecha]", cnxn), 
                                            fechas_limite("[dbo].[O.D.V Hidro(MDA) (SIN)]", "[Fecha]", cnxn), 
                                            fechas_limite("[dbo].[O.D.V Hidro(MTR) (SIN)]", "[Fecha]", cnxn), 
                                            fechas_limite("[dbo].[O.D.V Hidro Diario (MDA)]", "[Fecha]", cnxn), 
                                            fechas_limite("[dbo].[O.D.V Hidro Diario (MTR)]", "[Fecha]", cnxn), 
                                            fechas_limite("[dbo].[O.D.V No Desp (MDA) (SIN)]", "[Fecha]", cnxn), 
                                            fechas_limite("[dbo].[O.D.V No Desp (MDA) (BCA)]", "[Fecha]", cnxn), 
                                            fechas_limite("[dbo].[O.D.V No Desp (MDA) (BCS)]", "[Fecha]", cnxn), 
                                            fechas_limite("[dbo].[O.D.V No Desp (MTR) (SIN)]", "[Fecha]", cnxn), 
                                            fechas_limite("[dbo].[O.D.V No Desp (MTR) (BCA)]", "[Fecha]", cnxn), 
                                            fechas_limite("[dbo].[O.D.V No Desp (MTR) (BCS)]", "[Fecha]", cnxn), 
                                            fechas_limite("[dbo].[O.D.V Intermitente (MDA) (SIN)]", "[Fecha]", cnxn), 
                                            fechas_limite("[dbo].[O.D.V Intermitente (MDA) (BCA)]", "[Fecha]", cnxn), 
                                            fechas_limite("[dbo].[O.D.V Intermitente (MTR) (SIN)]", "[Fecha]", cnxn), 
                                            fechas_limite("[dbo].[O.D.V Intermitente (MTR) (BCA)]", "[Fecha]", cnxn))

        VariablesGlobales.fechas_sc_max = (fechas_limite("[dbo].[Servicios Conexos (MDA) (SIN)]", "[Fecha]", cnxn), 
                                           fechas_limite("[dbo].[Servicios Conexos (MTR) (SIN)]", "[Fecha]", cnxn), 
                                           fechas_limite("[dbo].[Requerimiento de Servicios Conexos (MDA) (SIN)]", "[Fecha]", cnxn), 
                                           fechas_limite("[dbo].[Requerimiento de Servicios Conexos (MDA) (BCA)]", "[Fecha]", cnxn), 
                                           fechas_limite("[dbo].[Requerimiento de Servicios Conexos (MDA) (BCS)]", "[Fecha]", cnxn), 
                                           fechas_limite("[dbo].[Requerimiento de Servicios Conexos (MTR) (SIN)]", "[Fecha]", cnxn), 
                                           fechas_limite("[dbo].[Requerimiento de Servicios Conexos (MTR) (BCA)]", "[Fecha]", cnxn), 
                                           fechas_limite("[dbo].[Requerimiento de Servicios Conexos (MTR) (BCS)]", "[Fecha]", cnxn))
        
        """
    #print(VariablesGlobales.__dict__)
    
    #print('Variables configuradas')

#---------- Graficas ----------#
def formato_grafica(encabezado, resultados, consulta_asignada=None, bd_tabla=None):
    """Asignar el formato apropiado para los datos de las gráficas que se enviarán a datatable.html."""

    #print(resultados)

    columnas_a_concatenar = []
    columnas_fechas = []
    columnas_datos = []
    titulos_datos = []
    columnas = resultados.columns
    valores_eje_vertical = []
    #columna_fecha = None
    fechas_concatenadas = ""
    dataset = {}
    #print(columnas)
    
    for pos, nombre in enumerate(columnas):

        if nombre.lower().find("fecha") != -1:

            try:
                
                resultados[nombre] = resultados[nombre].dt.strftime("%d/%m/%Y")

            except Exception:

                pass
            #columna_fecha = pos
            columnas_a_concatenar.append(nombre)
            break
    
    for pos, col in enumerate(columnas):

        tmp_col = col.lower()
        #print("Primero:", resultados[col].dtypes)

        #if col.lower().find("anio") != -1 or col.lower().find("año") != -1 or col.lower().find(" año") != -1 or col.lower().find("mes") != -1 or col.lower().find("día") != -1 or col.lower().find("semana") != -1 or col.lower().find("hora") != -1 \
        #or col.lower().find("hora"):
        if tmp_col.startswith("anio") or tmp_col.startswith("año") or tmp_col.startswith(" año") or tmp_col.startswith("mes") or tmp_col.startswith(" mes") \
        or tmp_col.startswith("hora") or tmp_col.startswith(" hora") or tmp_col.startswith("semana") or tmp_col.startswith(" semana"):
            
            #if columna_fecha == None:

            columnas_a_concatenar.append(col)
            #print("Yes", col)

        elif (resultados[col].dtypes == "float64" or resultados[col].dtypes == "int64") and (col not in columnas_a_concatenar or col not in columnas_datos):

            titulos_datos.append(col)
            columnas_datos.append(resultados[col])
    
    #if columna_fecha == None:

    for pos, elem in enumerate(columnas_a_concatenar):

        parentesis = False
        #guion = True

        if elem.lower().find("semana") != -1:

            fechas_concatenadas += "(Semana "
            parentesis = True
            #guion = False

        elif elem.lower().find("hora") != -1:

            #print(resultados[elem].dtypes)
            #print(resultados[elem])

            #print(elem)
            #print(resultados[columnas[columnas_a_concatenar[elem]]])
            try:
                
                resultados[elem] = resultados[elem].apply(formInt)

            except Exception:

                pass

            fechas_concatenadas += "(Hora "
            parentesis = True
            #guion = False

        fechas_concatenadas += resultados[elem].map(str)
        #print("guion:", guion)

        if parentesis:

            fechas_concatenadas += ")"

        #if elem != (len(columnas_a_concatenar) - 1) and guion:
        if pos != (len(columnas_a_concatenar) - 1):

            fechas_concatenadas += "-"

    valores_eje_horizontal = [str(x) for x in fechas_concatenadas]

    #else:

        #valores_eje_horizontal = [str(x) for x in resultados[columnas[columna_fecha]]]


    for pos, i in enumerate(columnas_datos):
        
        valores_eje_vertical.append([float(x) for x in i])
        dataset[titulos_datos[pos]] = valores_eje_vertical[pos]

    #print(columnas_a_concatenar)
    #print(columnas)
    print(valores_eje_horizontal)
    print(dataset)

    return valores_eje_horizontal, [dataset]

    #Checar si hay una mejor opcion
    valores_eje_horizontal = 0
    titulo_eje_vertical = 0
    titulo_eje_vertical_2 = 0
    titulo_eje_vertical_3 = 0
    titulo_eje_vertical_4 = 0
    valores_eje_vertical = 0
    valores_eje_vertical_2 = 0
    valores_eje_vertical_3 = 0
    valores_eje_vertical_4 = 0

    #---------------------------------#
    #l = []

    #for elem in 
    #---------------------------------#

    # Precios de mercado
    if encabezado == "Precios de mercado":

        if consulta_asignada == "Promedio 24h (diario)":

            resultados["Hora"] = resultados["Hora"].apply(formInt)
            etiquetas = list(resultados.columns)
            titulo_eje_vertical = etiquetas[4]
            fechas_concatenadas = resultados["Día"].map(str) + "/" + resultados["Mes"].map(str)+ "/" + resultados["Año"].map(str)+ " (" + resultados["Hora"].map(str)+ " hrs)"
            valores_eje_horizontal = [str(x) for x in fechas_concatenadas]
            valores_eje_vertical = [int(x) for x in resultados[etiquetas[4]]]

        elif consulta_asignada == "Promedio 24h (mensual)":

            resultados["Hora"] = resultados["Hora"].apply(formInt)
            etiquetas = list(resultados.columns)
            titulo_eje_vertical = etiquetas[3]
            fechas_concatenadas = resultados["Mes"].map(str) + "-" + resultados["Año"].map(str) + " (" + resultados["Hora"].map(str) + " hrs)"
            valores_eje_horizontal = [str(x) for x in fechas_concatenadas]
            valores_eje_vertical= [int(x) for x in resultados[etiquetas[3]]]
        
        elif consulta_asignada == "Promedio diario":
           
            etiquetas = list(resultados.columns)
            titulo_eje_vertical = etiquetas[3]
            fechas_concatenadas = resultados["Día"].map(str) + "/" + resultados["Mes"].map(str) + "/" + resultados["Año"].map(str)
            valores_eje_horizontal = [str(x) for x in fechas_concatenadas]
            valores_eje_vertical = [int(x) for x in resultados[etiquetas[3]]]

        elif consulta_asignada == "Promedio mensual":

            etiquetas = list(resultados.columns)            
            titulo_eje_vertical = etiquetas[2]
            fechas_concatenadas = resultados["Mes"].map(str) + "/" + resultados["Año"].map(str)
            valores_eje_horizontal = [str(x) for x in fechas_concatenadas]
            valores_eje_vertical = [int(x) for x in resultados[etiquetas[2]]]

        elif bd_tabla == "PML (MDA)" or bd_tabla ==  "PML (MTR)":

            resultados["Fecha"] = resultados["Fecha"].dt.strftime("%d/%m/%Y")
            resultados["Hora"] = resultados["Hora"].apply(formInt)
            etiquetas = list(resultados.columns)
            titulo_eje_vertical = etiquetas[3]
            titulo_eje_vertical_2 = etiquetas[4]
            titulo_eje_vertical_3 = etiquetas[5]
            titulo_eje_vertical_4 = etiquetas[6]
            fechas_concatenadas = resultados["Fecha"].map(str) + " (" + resultados["Hora"].map(str) + " hrs)"
            valores_eje_horizontal = [str(x) for x in fechas_concatenadas]
            valores_eje_vertical = [int(x) for x in resultados["Precio marginal local ($/MWh)"]]
            valores_eje_vertical_2= [int(x) for x in resultados["Componente de energia ($/MWh)"]]
            valores_eje_vertical_3= [int(x) for x in resultados["Componente de perdidas ($/MWh)"]]
            valores_eje_vertical_4= [int(x) for x in resultados["Componente de congestion ($/MWh)"]]
            #Dar formato a columnas deacuerdo a contenido 
            #resultados['Componente de congestion ($/MWh)']=resultados['Componente de congestion ($/MWh)'].apply(formMoneda)
            #resultados['Componente de perdidas ($/MWh)']=resultados['Componente de perdidas ($/MWh)'].apply(formMoneda)
           # resultados['Componente de energia ($/MWh)']=resultados['Componente de energia ($/MWh)'].apply(formMoneda)
            #resultados['Precio marginal local ($/MWh)']=resultados['Precio marginal local ($/MWh)'].apply(formMoneda)            

        elif bd_tabla == "PMZ (MDA)" or bd_tabla == "PMZ (MTR)":

            resultados["Fecha"] = resultados["Fecha"].dt.strftime('%d/%m/%Y')
            resultados["Hora"] = resultados["Hora"].apply(formInt)
            etiquetas = list(resultados.columns)
            titulo_eje_vertical = etiquetas[3]
            titulo_eje_vertical_2 = etiquetas[4]
            titulo_eje_vertical_3 = etiquetas[5]
            titulo_eje_vertical_4 = etiquetas[6]
            fechas_concatenadas = resultados["Fecha"].map(str) + " (" + resultados["Hora"].map(str) + " hrs)"
            valores_eje_horizontal = [str(x) for x in fechas_concatenadas]
            valores_eje_vertical = [int(x) for x in resultados["Precio Zonal  ($/MWh)"]]
            valores_eje_vertical_2 = [int(x) for x in resultados["Componente energia  ($/MWh)"]]
            valores_eje_vertical_3 = [int(x) for x in resultados["Componente perdidas  ($/MWh)"]]
            valores_eje_vertical_4 = [int(x) for x in resultados["Componente Congestion  ($/MWh)"]]
            resultados["Componente Congestion  ($/MWh)"] = resultados["Componente Congestion  ($/MWh)"].apply(formMoneda)
            resultados["Componente perdidas  ($/MWh)"] = resultados["Componente perdidas  ($/MWh)"].apply(formMoneda)
            resultados["Componente energia  ($/MWh)"] = resultados["Componente energia  ($/MWh)"].apply(formMoneda)
            resultados["Precio Zonal  ($/MWh)"] = resultados["Precio Zonal  ($/MWh)"].apply(formMoneda) 


        return valores_eje_horizontal, titulo_eje_vertical, titulo_eje_vertical_2, titulo_eje_vertical_3, titulo_eje_vertical_4, \
                   valores_eje_vertical, valores_eje_vertical_2, valores_eje_vertical_3, valores_eje_vertical_4

    # Demanda
    elif encabezado == "Demanda":

        if bd_tabla == "Demanda Pronosticada":

            if consulta_asignada == "Datos":

                resultados["Fecha"] = resultados["Fecha"].dt.strftime("%d/%m/%Y")
                resultados[" Hora"] = resultados[" Hora"].apply(formInt)
                etiquetas = list(resultados.columns)
                titulo_eje_vertical = etiquetas[4]
                fechas_concatenadas = resultados["Fecha"].map(str) + " (" + resultados[" Hora"].map(str) + " hrs)"
                valores_eje_horizontal = [str(x) for x in fechas_concatenadas]
                valores_eje_vertical = [int(x) for x in resultados[etiquetas[4]]]
                """
                "Datos"
                "Promedio unitario horario semanal"
                "Promedio horario mensual"
                "Promedio diario mensual"
                "Promedio diario mensual por area"
                "Promedio horario mensual por area"
                "Promedio unitario horario mensual"
                "Promedio horario semanal por area"
                "Promedio horario semanal total"
                """

            elif consulta_asignada == "Promedio unitario horario semanal":

                etiquetas = list(resultados.columns)
                titulo_eje_vertical = etiquetas[3]
                fechas_concatenadas = resultados["Mes"].map(str) + "-" + resultados["Año"].map(str) + " (Semana " + resultados["Semana"].map(str) + ")"
                valores_eje_horizontal = [str(x) for x in fechas_concatenadas]
                valores_eje_vertical = [int(x) for x in resultados[etiquetas[3]]]

            elif consulta_asignada == "Promedio horario mensual":

                etiquetas = list(resultados.columns)
                titulo_eje_vertical = etiquetas[2]
                fechas_concatenadas = resultados["Mes"].map(str) + "-" + resultados["Año"].map(str)
                valores_eje_horizontal = [str(x) for x in fechas_concatenadas]
                valores_eje_vertical = [int(x) for x in resultados[etiquetas[2]]]

            elif consulta_asignada == "Promedio diario mensual":

                pass

            elif consulta_asignada == "Promedio diario mensual por area":

                pass

            elif consulta_asignada == "Promedio horario mensual por area":

                etiquetas = list(resultados.columns)
                titulo_eje_vertical = etiquetas[3]
                fechas_concatenadas = resultados["Mes"].map(str) + "-" + resultados["Año"].map(str)
                valores_eje_horizontal = [str(x) for x in fechas_concatenadas]
                valores_eje_vertical = [int(x) for x in resultados[etiquetas[3]]]

            elif consulta_asignada == "Promedio unitario horario mensual":

                etiquetas = list(resultados.columns)
                titulo_eje_vertical = etiquetas[2]
                fechas_concatenadas = resultados["Mes"].map(str) + "-" + resultados["Año"].map(str)
                valores_eje_horizontal = [str(x) for x in fechas_concatenadas]
                valores_eje_vertical = [int(x) for x in resultados[etiquetas[2]]]

            elif consulta_asignada == "Promedio horario semanal por area":

                etiquetas = list(resultados.columns)
                titulo_eje_vertical = etiquetas[4]
                fechas_concatenadas = resultados["Mes"].map(str) + "-" + resultados["Año"].map(str) + " (Semana " + resultados["Semana"].map(str) + ")"
                valores_eje_horizontal = [str(x) for x in fechas_concatenadas]
                valores_eje_vertical = [int(x) for x in resultados[etiquetas[4]]]

            elif consulta_asignada == "Promedio horario semanal total":

                # Pendiente

                #print("H")
                #print(list(resultados.columns))
                #print(resultados.iloc[:, 0])
                etiquetas = list(resultados.columns)
                titulo_eje_vertical = etiquetas[4]
                fechas_concatenadas = resultados["Mes"].map(str) + "-" + resultados["Año"].map(str) + " (Semana " + resultados["Semana"].map(str) + ")"
                valores_eje_horizontal = [str(x) for x in fechas_concatenadas]
                valores_eje_vertical = [int(x) for x in resultados[etiquetas[4]]]

        elif bd_tabla == "Demanda Maxima Diaria MIM":

            pass

        elif bd_tabla == "Demanda Promedio Diaria MIM":

            pass

        elif bd_tabla == "Demanda Maxima Instantanea CENACE":

            pass

            return valores_eje_horizontal, titulo_eje_vertical, titulo_eje_vertical_2, titulo_eje_vertical_3, titulo_eje_vertical_4, \
                    valores_eje_vertical, valores_eje_vertical_2, valores_eje_vertical_3, valores_eje_vertical_4

    # Ofertas de compra
    elif encabezado == "Ofertas de compra":

        pass

    # Ofertas de venta
    elif encabezado == "Ofertas de venta":

        pass

    # Energia asignada
    elif encabezado == "Energia asignada":

        pass

    # Generacion de energia
    elif encabezado == "Generacion de energia": 

        pass

    # PPA
    elif encabezado == "PPA":
        
        titulo = 1

       
        etiquetas = list(resultados.columns)
        
        label1 = str(etiquetas[3:4])
        label2 = str(etiquetas[2:3])
        label3 = str(etiquetas[1:2])
        label4 = 0


        labels1 = resultados["Fecha"].map(str) 
        labels= [str(x) for x in labels1]
       
        values1 = resultados[etiquetas[3]]
        values= [int(x) for x in values1]

        values2= resultados[etiquetas[2]]
        values2= [int(x) for x in values2]

        values3= resultados[etiquetas[1]]
        values3= [int(x) for x in values3]

        values4= 0

        caso = 3

    if encabezado == "Servicios conexos":

        if consulta_asignada == "Promedio horario mensual por reserva":
            etiquetas = list(resultados.columns)
                        
            label1 = etiquetas[3:4]
            label2 = 0
            label3 = 0
            label4 = 0

            #print(resultados)
            labels1 = resultados["mes"].map(str)+ "/" + resultados["anio"].map(str)
            labels= [str(x) for x in labels1]
                       
            values1 = resultados[etiquetas[3]]
            values= [int(x) for x in values1]

            values2= 0

            values3= 0

            values4= 0

            caso = 1

        elif consulta_asignada == "Promedio 24 hrs diario":
            etiquetas = list(resultados.columns)
            resultados['hora']=resultados['hora'].apply(formInt)            
            label1 = etiquetas[5:6]
            label2 = 0
            label3 = 0
            label4 = 0

            #print(resultados)
            labels1 = resultados["dia"].map(str)+ "/" + resultados["mes"].map(str)+ "/" + resultados["anio"].map(str) + " (" + resultados["hora"].map(str) + " hrs)"
            labels= [str(x) for x in labels1]
                       
            values1 = resultados[etiquetas[5]]
            values= [int(x) for x in values1]

            values2= 0

            values3= 0

            values4= 0

            caso = 1

        elif consulta_asignada == "Promedio 24 hrs mensual":
            etiquetas = list(resultados.columns)
            resultados['hora']=resultados['hora'].apply(formInt)            
            label1 = etiquetas[4:5]
            label2 = 0
            label3 = 0
            label4 = 0

            
            labels1 = resultados["mes"].map(str)+ "-" + resultados["anio"].map(str) + " (" + resultados["hora"].map(str) + " hrs)"
            labels= [str(x) for x in labels1]
                       
            values1 = resultados[etiquetas[4]]
            values= [int(x) for x in values1]

            values2= 0

            values3= 0

            values4= 0

            caso = 1

        return caso, labels, label1, label2, label3, label4, values, values2, values3, values4

    if encabezado == "WSODC":
        
            titulo = 1

           
            etiquetas = list(resultados.columns)
            
            label1 = "Datos"
            label2 = 0
            label3 = 0
            label4 = 0


            labels1 = resultados["hora"].map(str) 
            labels= [str(x) for x in labels1]
           
            values1 = resultados[etiquetas[4]]
            values= [float(x) for x in values1]

            values2= 0

            values3= 0

            values4= 0

            caso = 3


    return titulo, caso, labels, label1, label2, label3, label4, values, values2, values3, values4


def index():
    """Esta función renderiza la página templates/log.html."""

    return render_template("log.html")

# Función para obtener un usuario, se utiliza una query para obtener el usuario en la base de datos.
# Para instanciar el usuario se utiliza la funcion iloc. Debido a que obtenemos una sola fila (cada mail en nuestra tabla SQL es unico), 
# el indice para la fila siempre sera '0', el indice de la columna ira aumentando en 1 para referirse a id, nombre, email contraseña y rol.
def get_usuario(email):
    cnxn=crear_conexion_SQL()

    with cnxn:
        consulta_sql="""Select [id], [nombre], [email], 
                        CONVERT(VARCHAR(MAX), DECRYPTBYPASSPHRASE('7110c8ae51a4b5af97be6534caef90e4bb9bdcb3380af008f90b23a5d1616bf319bc298105da20fe', [password])) as password, 
                        [role]
                        From [dbo].[Users] u
                        Where u.email='{emailConsulta}'
                    """

        consulta_sql = consulta_sql.format(emailConsulta=email)
        df = pd.read_sql(consulta_sql, cnxn)
        if df.empty==True:
            return None
        else:
            user=models.User(df.iloc[0,0],df.iloc[0,1],df.iloc[0,2], df.iloc[0,3], df.iloc[0,4],carga_temporal=True)
            return user

# Función TEMPORAL que nos permite reiniciar los usuarios prueba en la base de datos.
# Fue necesario implementar esta función ya que no podemos guardar usuarios si no es por medio de la contraseña que genera
# la libreria werkzeug. Se tiene la encriptacion de la libreria werkzeug (generate_password_hash) y la encriptacion que permite
# SQL por medio de ENCRYPTBYPASSPHRASE  
def reiniciar_usuarios():
    cnxn=crear_conexion_SQL()
    cursor=cnxn.cursor()

    borrar_usuarios_guardados="truncate table [dbo].[Users]"   

    cursor.execute(borrar_usuarios_guardados)

    for usuario in users:
        reiniciar_usuarios="""
            INSERT INTO [dbo].[Users](
                [id], 
                [nombre],
                [email],
                [password],
                [role]
                )
            VALUES (
                {id},
                '{nombre}',
                '{mail}',
                ENCRYPTBYPASSPHRASE('7110c8ae51a4b5af97be6534caef90e4bb9bdcb3380af008f90b23a5d1616bf319bc298105da20fe', '{contraseña}'),
                {rol}
            )
            """
        reiniciar_usuarios=reiniciar_usuarios.format(id=usuario.id, nombre=usuario.name, mail=usuario.email,contraseña=usuario.password, rol=usuario.role)
        cursor.execute(reiniciar_usuarios)
    cursor.commit()

def signin():
    """Esta función verifica las credenciales y realiza el login."""
    if request.method == "POST":
        reiniciar_usuarios()
        usuario = request.form["username"]
        contrasena = request.form["password"]
        

        user = get_usuario(usuario)
        #Validacion correcta
        if user is not None and user.check_password(contrasena):
            login_user(user)#, remember=form.remember_me.data)
            #next_page = request.args.get('next')
            #Verificar redireccionamiento seguro
            #if not is_safe_url(next):
                #return flask.abort(400)
            
            #Si no hay pagina de redireccion
            #print(next_page)   
            #if not next_page:
                #next_page = url_for('home')

            #print(next_page)    
            return render(request, 'alux/log.html', {})
        #Validación incorrecta
    else:
        return render(request, 'alux/log.html', {})
  
  
@login_manager.unauthorized_handler
def unauthorized_callback():
    print ('El usuario será redireccionado a :' + request.path + ' despues de que se autentifique')
    return redirect('/?next=' + request.path)


@login_required
def home():
    """Esta función renderiza la página templates/home.html."""

    obtener_fechas_limite()
    
    cnxn = crear_conexion_SQL() # Crear conexión con SQL Server
    with cnxn:
        consulta_ultima_fecha_disponible="""
                                        SELECT TOP (1) [Fecha]
                                        FROM [dbo].[PMZ(MDA) (SIN)]
                                        ORDER BY Fecha DESC
                                        """
        dffecha = pd.read_sql(consulta_ultima_fecha_disponible, cnxn)
        fecha=dffecha.loc[0]['Fecha'].date()
        #print(fecha)
        #fecha="2021-06-21 00:00:00.000"

        consulta_sql = """
                         SELECT 
                            precios.[Zona de Carga] as zona
                            ,coordenadas.[Latitud]
                            ,coordenadas.[Longitud]
                            ,precios.[Promedio diario] as promedio
                           

                            From( select
                            [Fecha] 
                            ,[Zona de Carga]  
                            ,avg([Precio Zonal  ($/MWh)]) as 'Promedio diario'
                            FROM [dbo].[PMZ(MDA) (SIN)] as pmz 
                            where Fecha = '{fecha}' 
                            group by [Zona de Carga], Fecha
                            union 

                            select [Fecha] 
                            ,[Zona de Carga]  
                            ,avg([Precio Zonal  ($/MWh)]) as 'Promedio diario'
                            FROM [dbo].[PMZ(MDA) (bca)]  as bca
                                                        
                            where Fecha = '{fecha}' 
                            group by [Zona de Carga], Fecha

                            union 

                            select [Fecha] 
                            ,[Zona de Carga]  
                            ,avg([Precio Zonal  ($/MWh)]) as 'Promedio diario'
                            FROM [dbo].[PMZ(MDA) (bcs)]  as bca
                                                        
                            where Fecha = '{fecha}' 
                            group by [Zona de Carga], Fecha ) as precios join [dbo].[Zonas y Coordenadas] as coordenadas
                            on coordenadas.[Zona] = precios.[Zona de Carga] 
                        """
        consulta_sql = consulta_sql.format(fecha=fecha)
        df = pd.read_sql(consulta_sql, cnxn)
        #print(df)

        json_records = df.reset_index().to_json(orient ='records')
        data = []
        context = json.loads(json_records)
        #context = {data}
        
        dataset = dict([(i,[a,b]) for i, a,b in zip(df.promedio, df.Latitud,df.Longitud)])
        data=[dataset]
        dataset = [dataset]
        #print(dataset)


        la = df['Latitud']
        precio1 = [(x/1000000) for x in la]
        #print(precio1)

        lo = df['Longitud']
        precio1 = [(x/1000000) for x in lo]

        values2 = df["Latitud"].map(str)+ ", " + df["Longitud"].map(str)
        value = [str(x) for x in values2]

        datasetx = "'"+df['zona']+"' : "+ df['Latitud'].map(str)+ ", " + df['Longitud'].map(str)
        dataset1 = [{x} for x in datasetx]
    
        #print(dataset)
        #print(type(dataset))
        #print(current_user.name)

        consulta_sqlmaxmin = """
                SELECT 
				precios.Fecha
                ,max(precios.[Promedio diario]) as maxpromedio
                ,min(precios.[Promedio diario]) as minpromedio

                From( select
                    [Fecha] 
                ,[Zona de Carga]  
                ,avg([Precio Zonal  ($/MWh)]) as 'Promedio diario'
                FROM [dbo].[PMZ(MDA) (SIN)]
                where fecha = '{fecha}' 
                group by [Zona de Carga], Fecha ) as precios 
				group by Fecha 
                """
        consulta_sqlmaxmin = consulta_sqlmaxmin.format(fecha=fecha)
        resultado = pd.read_sql(consulta_sqlmaxmin, cnxn)
        max =int(resultado.iloc[0][1])
        min =int(resultado.iloc[0][2])
        #print(resultado)
        #print(max)
        #print(min)
        intervalo = max- min
        x1 = intervalo *.25
        x2 = intervalo *.5
        x3 = intervalo *.75
        inter1 = min + x1
        inter2 = min + x2
        inter3 = min + x3
        #print(inter1)
        #print(inter2)


       
        consulta_sql = """



select * from (SELECT top 31 PivotTable.[Estado de cuenta] as 'Fecha' ,[C00] as 'Costo de operacion' ,([P00] + tbfin.Monto + facturas.Factura) as 'Ingreso '
from 
( select [Estado de cuenta]
,right([FUF],3) as 't'
,[Monto] 
FROM [dbo].[EDC_Pagos] where Subcuenta = 'B01' ) AS SourceTable PIVOT(sum([Monto]) FOR t IN ([C00] ,
[P00] )) AS PivotTable  join  
														 
														 
(select top 31 [Estado de cuenta] 
,monto
														 
from [EDC_Pagos] where Subcuenta = 'B02'
and fuf like '%P00'
order by [Estado de cuenta] desc ) as tbfin
on PivotTable.[Estado de cuenta] = tbfin.[Estado de cuenta]
 join ( select top (31) Fecha, sum((Consumo_Total * Precio_medio_Beetmann)) as 'Factura' from Precios_Clientes group by Fecha order by fecha desc) as facturas
on facturas.Fecha = PivotTable.[Estado de cuenta]

order by 'Fecha' desc)as tabla 
order by fecha asc
														 
  
										
						
														 
  

                     
                        """
        #consulta_sql = consulta_sql.format()
        #df = pd.read_sql(consulta_sql, cnxn)

        #labels, dataset2 = formato_grafica("Precios de mercado", df)
        
        
       
       

    return render_template("index.html", dataset = dataset, context=context, fecha = fecha, max = max, min = min,inter1 = inter1, inter2 = inter2,inter3 = inter3)
    
    #print(current_user.name)
    #return render_template("index.html", title="Home Page", year=datetime.now().year)

#--------------------------------------REVISAR-----------------------------------------------------------#
def contact():

    """Esta función renderiza la página templates/contact.html."""
    mns="hola mundo"
    return render_template("404.html", mns=mns)


#--------------------------------------REVISAR-----------------------------------------------------------#
def error_handle():

    """Esta función renderiza la página templates/contact.html."""
    
    return render_template("accion_no_disponible.html")


@login_required
def upload_file():
    """Esta función renderiza la página templates/upload1.html."""

    if request.method == "POST":
        
        f = request.files["file"]
        data_xls = pd.read_excel(f)

        return data_xls.to_html(classes=["table-bordered", "table-striped", "table-hover", "table"])

    return """
    <!doctype html>
    <title>Upload an excel file</title>
    <h1>Excel file upload (csv, tsv, csvz, tsvz only)</h1>
    <form action="" method=post enctype=multipart/form-data>
    <p><input type=file name=file><input type=submit value=Upload>
    </form>
    """
    #---------------------------------REVISAR NO TENER HTML-----------------------------------------------------------------------#

#mapa con zoom "checar"
def mapaZoom():
    return render_template("dashboard/MapaZoom.html")


@login_required
def mapa():
    """Esta función renderiza la página templates/mapa.html."""
    if request.method == "POST":
        fecha = request.form['fecha']
        #print(fecha)
        cnxn = crear_conexion_SQL() # Crear conexión con SQL Server
        with cnxn:
            consulta_sql = """
 SELECT 
                            precios.[Zona de Carga] as zona
                            ,coordenadas.[Latitud]
                            ,coordenadas.[Longitud]
                            ,precios.[Promedio diario] as promedio
                           

                            From( select
[Fecha] 
,[Zona de Carga]  
,avg([Precio Zonal  ($/MWh)]) as 'Promedio diario'
FROM [dbo].[PMZ(MDA) (SIN)] as pmz 
where Fecha = '{fecha}' 
group by [Zona de Carga], Fecha
union 

select [Fecha] 
,[Zona de Carga]  
,avg([Precio Zonal  ($/MWh)]) as 'Promedio diario'
FROM [dbo].[PMZ(MDA) (bca)]  as bca
							
where Fecha = '{fecha}' 
group by [Zona de Carga], Fecha

union 

select [Fecha] 
,[Zona de Carga]  
,avg([Precio Zonal  ($/MWh)]) as 'Promedio diario'
FROM [dbo].[PMZ(MDA) (bcs)]  as bca
							
where Fecha = '{fecha}' 
group by [Zona de Carga], Fecha ) as precios join [dbo].[Zonas y Coordenadas] as coordenadas
                            on coordenadas.[Zona] = precios.[Zona de Carga] 
                            """
            consulta_sql = consulta_sql.format(fecha=fecha)
            df = pd.read_sql(consulta_sql, cnxn)

            json_records = df.reset_index().to_json(orient ='records')
            data = []
            context = json.loads(json_records)
            #context = {data}


            
            dataset = dict([(i,[a,b]) for i, a,b in zip(df.promedio, df.Latitud,df.Longitud)])
            data=[dataset]
            dataset = [dataset]


            la = df['Latitud']
            precio1 = [(x/1000000) for x in la]
            

            lo = df['Longitud']
            precio1 = [(x/1000000) for x in lo]

            values2 = df["Latitud"].map(str)+ ", " + df["Longitud"].map(str)
            value = [str(x) for x in values2]

            datasetx = "'"+df['zona']+"' : "+ df['Latitud'].map(str)+ ", " + df['Longitud'].map(str)
            dataset1 = [{x} for x in datasetx]
        


            consulta_sqlmaxmin = """
                           SELECT 
						   precios.Fecha
                            ,max(precios.[Promedio diario]) as maxpromedio
                            ,min(precios.[Promedio diario]) as minpromedio

                            From( select
                                [Fecha] 
                            ,[Zona de Carga]  
                            ,avg([Precio Zonal  ($/MWh)]) as 'Promedio diario'
                            FROM [dbo].[PMZ(MDA) (SIN)]
                            where fecha = '{fecha}' 
                            group by [Zona de Carga], Fecha ) as precios 
							group by Fecha 
                            """
            consulta_sql = consulta_sql.format(fecha=fecha)
            consulta_sqlmaxmin = consulta_sqlmaxmin.format(fecha=fecha)
            df = pd.read_sql(consulta_sql, cnxn)
            resultado = pd.read_sql(consulta_sqlmaxmin, cnxn)
            max = resultado[0][1]
            print(max)
            print(context)
        return render_template("dashboard/mapa.html", dataset = dataset, context=context)
    else:
        return render_template("dashboard/mapa.html")


@login_required
def charts():
    """Esta función renderiza la página templates/charts.html."""

    etiquetas= ['13/05/2021 (1 hrs)', '13/05/2021 (2 hrs)', '13/05/2021 (3 hrs)', '13/05/2021 (4 hrs)', '13/05/2021 (5 hrs)', '13/05/2021 (6 hrs)', '13/05/2021 (7 hrs)', '13/05/2021 (8 hrs)', '13/05/2021 (9 hrs)', '13/05/2021 (10 hrs)', '13/05/2021 (11 hrs)', '13/05/2021 (12 hrs)', '13/05/2021 (13 hrs)', '13/05/2021 (14 hrs)', '13/05/2021 (15 hrs)', '13/05/2021 (16 hrs)', '13/05/2021 (17 hrs)', '13/05/2021 (18 hrs)', '13/05/2021 (19 hrs)', '13/05/2021 (20 hrs)', '13/05/2021 (21 hrs)', '13/05/2021 (22 hrs)', '13/05/2021 (23 hrs)', '13/05/2021 (24 hrs)', '14/05/2021 (1 hrs)', '14/05/2021 (2 hrs)', '14/05/2021 (3 hrs)', '14/05/2021 (4 hrs)', '14/05/2021 (5 hrs)', '14/05/2021 (6 hrs)', '14/05/2021 (7 hrs)', '14/05/2021 (8 hrs)', '14/05/2021 (9 hrs)', '14/05/2021 (10 hrs)', '14/05/2021 (11 hrs)', '14/05/2021 (12 hrs)', '14/05/2021 (13 hrs)', '14/05/2021 (14 hrs)', '14/05/2021 (15 hrs)', '14/05/2021 (16 hrs)', '14/05/2021 (17 hrs)', '14/05/2021 (18 hrs)', '14/05/2021 (19 hrs)', '14/05/2021 (20 hrs)', '14/05/2021 (21 hrs)', '14/05/2021 (22 hrs)', '14/05/2021 (23 hrs)', '14/05/2021 (24 hrs)', '15/05/2021 (1 hrs)', '15/05/2021 (2 hrs)', '15/05/2021 (3 hrs)', '15/05/2021 (4 hrs)', '15/05/2021 (5 hrs)', '15/05/2021 (6 hrs)', '15/05/2021 (7 hrs)', '15/05/2021 (8 hrs)', '15/05/2021 (9 hrs)', '15/05/2021 (10 hrs)', '15/05/2021 (11 hrs)', '15/05/2021 (12 hrs)', '15/05/2021 (13 hrs)', '15/05/2021 (14 hrs)', '15/05/2021 (15 hrs)', '15/05/2021 (16 hrs)', '15/05/2021 (17 hrs)', '15/05/2021 (18 hrs)', '15/05/2021 (19 hrs)', '15/05/2021 (20 hrs)', '15/05/2021 (21 hrs)', '15/05/2021 (22 hrs)', '15/05/2021 (23 hrs)', '15/05/2021 (24 hrs)', '16/05/2021 (1 hrs)', '16/05/2021 (2 hrs)', '16/05/2021 (3 hrs)', '16/05/2021 (4 hrs)', '16/05/2021 (5 hrs)', '16/05/2021 (6 hrs)', '16/05/2021 (7 hrs)', '16/05/2021 (8 hrs)', '16/05/2021 (9 hrs)', '16/05/2021 (10 hrs)', '16/05/2021 (11 hrs)', '16/05/2021 (12 hrs)', '16/05/2021 (13 hrs)', '16/05/2021 (14 hrs)', '16/05/2021 (15 hrs)', '16/05/2021 (16 hrs)', '16/05/2021 (17 hrs)', '16/05/2021 (18 hrs)', '16/05/2021 (19 hrs)', '16/05/2021 (20 hrs)', '16/05/2021 (21 hrs)', '16/05/2021 (22 hrs)', '16/05/2021 (23 hrs)', '16/05/2021 (24 hrs)', '17/05/2021 (1 hrs)', '17/05/2021 (2 hrs)', '17/05/2021 (3 hrs)', '17/05/2021 (4 hrs)', '17/05/2021 (5 hrs)', '17/05/2021 (6 hrs)', '17/05/2021 (7 hrs)', '17/05/2021 (8 hrs)', '17/05/2021 (9 hrs)', '17/05/2021 (10 hrs)', '17/05/2021 (11 hrs)', '17/05/2021 (12 hrs)', '17/05/2021 (13 hrs)', '17/05/2021 (14 hrs)', '17/05/2021 (15 hrs)', '17/05/2021 (16 hrs)', '17/05/2021 (17 hrs)', '17/05/2021 (18 hrs)', '17/05/2021 (19 hrs)', '17/05/2021 (20 hrs)', '17/05/2021 (21 hrs)', '17/05/2021 (22 hrs)', '17/05/2021 (23 hrs)', '17/05/2021 (24 hrs)', '18/05/2021 (1 hrs)', '18/05/2021 (2 hrs)', '18/05/2021 (3 hrs)', '18/05/2021 (4 hrs)', '18/05/2021 (5 hrs)', '18/05/2021 (6 hrs)', '18/05/2021 (7 hrs)', '18/05/2021 (8 hrs)', '18/05/2021 (9 hrs)', '18/05/2021 (10 hrs)', '18/05/2021 (11 hrs)', '18/05/2021 (12 hrs)', '18/05/2021 (13 hrs)', '18/05/2021 (14 hrs)', '18/05/2021 (15 hrs)', '18/05/2021 (16 hrs)', '18/05/2021 (17 hrs)', '18/05/2021 (18 hrs)', '18/05/2021 (19 hrs)', '18/05/2021 (20 hrs)', '18/05/2021 (21 hrs)', '18/05/2021 (22 hrs)', '18/05/2021 (23 hrs)', '18/05/2021 (24 hrs)', '19/05/2021 (1 hrs)', '19/05/2021 (2 hrs)', '19/05/2021 (3 hrs)', '19/05/2021 (4 hrs)', '19/05/2021 (5 hrs)', '19/05/2021 (6 hrs)', '19/05/2021 (7 hrs)', '19/05/2021 (8 hrs)', '19/05/2021 (9 hrs)', '19/05/2021 (10 hrs)', '19/05/2021 (11 hrs)', '19/05/2021 (12 hrs)', '19/05/2021 (13 hrs)', '19/05/2021 (14 hrs)', '19/05/2021 (15 hrs)', '19/05/2021 (16 hrs)', '19/05/2021 (17 hrs)', '19/05/2021 (18 hrs)', '19/05/2021 (19 hrs)', '19/05/2021 (20 hrs)', '19/05/2021 (21 hrs)', '19/05/2021 (22 hrs)', '19/05/2021 (23 hrs)', '19/05/2021 (24 hrs)', '20/05/2021 (1 hrs)', '20/05/2021 (2 hrs)', '20/05/2021 (3 hrs)', '20/05/2021 (4 hrs)', '20/05/2021 (5 hrs)', '20/05/2021 (6 hrs)', '20/05/2021 (7 hrs)', '20/05/2021 (8 hrs)', '20/05/2021 (9 hrs)', '20/05/2021 (10 hrs)', '20/05/2021 (11 hrs)', '20/05/2021 (12 hrs)', '20/05/2021 (13 hrs)', '20/05/2021 (14 hrs)', '20/05/2021 (15 hrs)', '20/05/2021 (16 hrs)', '20/05/2021 (17 hrs)', '20/05/2021 (18 hrs)', '20/05/2021 (19 hrs)', '20/05/2021 (20 hrs)', '20/05/2021 (21 hrs)', '20/05/2021 (22 hrs)', '20/05/2021 (23 hrs)', '20/05/2021 (24 hrs)']
    valores1= [709, 685, 681, 687, 674, 674, 686, 690, 672, 677, 685, 687, 695, 683, 693, 674, 672, 697, 704, 690, 738, 1498, 734, 700, 681, 681, 675, 663, 672, 653, 686, 701, 637, 630, 596, 621, 611, 633, 638, 643, 693, 646, 698, 674, 680, 698, 647, 642, 781, 774, 690, 680, 650, 654, 656, 656, 638, 597, 597, 610, 611, 601, 615, 617, 667, 675, 783, 784, 1296, 1321, 1126, 757, 674, 641, 625, 664, 656, 676, 656, 636, 595, 534, 536, 535, 534, 574, 575, 587, 635, 641, 700, 702, 772, 1375, 1011, 903, 688, 675, 671, 656, 643, 653, 673, 691, 648, 641, 654, 654, 664, 629, 656, 675, 724, 690, 705, 749, 1091, 2649, 1105, 718, 644, 644, 644, 645, 638, 640, 619, 646, 661, 640, 646, 656, 640, 630, 652, 680, 668, 676, 669, 671, 1057, 2151, 922, 705, 671, 648, 654, 649, 641, 645, 658, 665, 654, 631, 710, 648, 645, 670, 673, 663, 702, 693, 702, 911, 733, 760, 712, 687, 657, 665, 669, 681, 668, 651, 647, 655, 655, 706, 708, 704, 717, 725, 726, 704, 700, 698, 727, 690, 699, 712, 684, 663]
    valores2= [680, 661, 662, 662, 652, 648, 657, 662, 642, 643, 651, 654, 659, 649, 658, 651, 647, 670, 678, 672, 707, 1441, 715, 686, 668, 668, 659, 645, 653, 636, 656, 669, 620, 610, 575, 598, 590, 609, 617, 621, 661, 618, 662, 646, 661, 677, 627, 624, 752, 755, 661, 653, 628, 635, 638, 636, 616, 574, 573, 584, 585, 578, 590, 592, 639, 649, 747, 757, 1248, 1253, 1074, 732, 648, 618, 603, 639, 630, 648, 627, 606, 563, 502, 501, 501, 501, 537, 539, 551, 596, 602, 660, 668, 732, 1307, 961, 861, 659, 649, 643, 628, 614, 623, 645, 664, 619, 606, 619, 619, 631, 599, 626, 643, 683, 657, 674, 717, 1034, 2512, 1053, 686, 619, 617, 621, 620, 612, 614, 598, 622, 626, 603, 606, 615, 605, 596, 612, 653, 634, 640, 634, 645, 1010, 2054, 883, 675, 637, 617, 623, 623, 617, 621, 630, 639, 622, 595, 606, 610, 610, 632, 634, 631, 668, 660, 667, 648, 695, 727, 683, 660, 634, 633, 639, 652, 644, 627, 621, 631, 626, 667, 666, 665, 678, 687, 687, 668, 665, 658, 687, 665, 666, 683, 656, 638]
    valores3= [28, 23, 18, 24, 21, 25, 29, 27, 30, 33, 34, 33, 35, 33, 35, 23, 25, 26, 26, 17, 30, 57, 19, 14, 12, 13, 16, 17, 18, 17, 30, 31, 17, 20, 20, 23, 20, 24, 21, 23, 32, 28, 35, 28, 19, 20, 20, 17, 29, 17, 28, 26, 21, 18, 16, 20, 22, 22, 23, 25, 25, 23, 25, 25, 27, 26, 35, 25, 45, 66, 50, 24, 25, 22, 22, 24, 25, 27, 28, 30, 32, 32, 35, 34, 33, 36, 36, 35, 38, 38, 39, 34, 39, 68, 49, 42, 29, 25, 28, 28, 28, 29, 28, 27, 29, 34, 35, 34, 32, 29, 30, 32, 40, 33, 31, 31, 56, 136, 51, 32, 24, 27, 22, 25, 25, 26, 20, 24, 35, 36, 39, 40, 35, 33, 40, 27, 34, 35, 34, 26, 47, 97, 39, 30, 34, 30, 31, 26, 23, 23, 27, 25, 31, 35, 39, 38, 35, 37, 38, 31, 34, 33, 35, 41, 38, 33, 28, 27, 22, 32, 30, 29, 23, 23, 25, 23, 29, 39, 41, 39, 39, 38, 39, 36, 34, 39, 39, 25, 33, 29, 27, 25]
    valores4= [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 2, 2, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 65, 0, 0, 0, 0, 0, 0, 0, 0, 221, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]

    #values=[values1,values2,values3,values4]
    name1="datoset1"
    name2="datoset2"
    name3="datoset3"
    name4="datoset4"
    dataset = [{name1: valores1}, {name2: valores2}, {name3: valores3},{name4: valores4}, {name4: valores4}]

    #amedata=[name1,name2,name3,name4] 
    #print(dataset)
    #print(type(dataset))

    return render_template("charts.html", labels=etiquetas, dataset=dataset)


@login_required
def upload():
    """Esta función renderiza la página templates/contact.html."""
    
    return render_template("mantenimiento_message.html")##("MODELO_mercados/inputs_modelo.html")


#######################################################################
#######################################################################
#MODELO DE MERCADOS

#######################################################################
#######################################################################


def horario(fecha, region):
    """Esta función verifica si la fecha recibida se encuentra en los horarios estándar o alternativo 
    para las diferentes regiones."""


    dias_por_restar = (1, 2, 3, 4, 5, 6, 0) # Diferencia inversa de días con el día domingo

    # Región Baja California
    if region == "BCA":

        inicio_de_horario_estandar = date(fecha.year, 5, 1) # Fecha de inicio (1 de mayo)
        diferencia_entre_dias = timedelta(days=dias_por_restar[date(fecha.year, 10, 31).weekday()]) # Días a restar al último domingo de octubre
        cierre_de_horario_estandar = date(fecha.year, 10, 31) - diferencia_entre_dias # Fecha de cierre (último domingo de octubre)

    # Región Baja California Sur y regiones Central, Noreste, Noroeste, Norte, Peninsular y Sur
    else:

        diferencia_entre_dias = timedelta(days=(6-date(fecha.year, 4, 1).weekday()))
        inicio_de_horario_estandar = date(fecha.year, 4, 1) + diferencia_entre_dias # Fecha de inicio (primer domingo de abril)
        diferencia_entre_dias = timedelta(days=dias_por_restar[date(fecha.year, 10, 31).weekday()]+1)
        cierre_de_horario_estandar = date(fecha.year, 10, 31) - diferencia_entre_dias # Fecha de cierre (sábado anterior al último domingo de octubre)

    if fecha >= inicio_de_horario_estandar and fecha < cierre_de_horario_estandar:

        # Horario estándar
        return False

    else:

        # Horario alternativo
        return True


@login_required
def data():
    """Esta función renderiza y procesa los datos de la página templates/data.html."""

    
    
    if request.method == 'POST':

        x_json = """
        {
    "general_data": {
        "precio_de_potencia": 1000000,
        "precio_de_cobertura": 40.92,
        "empresa": "Beetmann",
        "medidor": "18E2A0",
        "Nivel_de_Tension": 220,
        "SistemaI": "SIN",
        "division_tarifa_cfe": "Norte",
        "Tarifa": "GDMTH",
        "Fee": 0.24,
        "Zona de Carga (PMZ)": "JUAREZ",
        "%_Cobertura_Solar": 0.38,
        "%_Cobertura_Eólica": 0.62,
        "%_Cobertura_Total": 80
    },
    "data": {
        "lapso": "2020",
        "consumo_total": {
            "base": {
                "1": 620425,
                "2": 581156,
                "3": 655193,
                "4": 256544,
                "5": 221724,
                "6": 445335,
                "7": 630182,
                "8": 845538,
                "9": 833358,
                "10": 794965,
                "11": 669947,
                "12": 623467
            },
            "intermedio": {
                "1": 1001901,
                "2": 1012812,
                "3": 1073308,
                "4": 605800,
                "5": 484279,
                "6": 1040491,
                "7": 1394386,
                "8": 1339019,
                "9": 1284861,
                "10": 1361325,
                "11": 1034868,
                "12": 958963
            },
            "punta": {
                "1": 246277,
                "2": 151700,
                "3": 172103,
                "4": 67938,
                "5": 43664,
                "6": 102593,
                "7": 131718,
                "8": 200181,
                "9": 201754,
                "10": 230763,
                "11": 239787,
                "12": 223416
            }
        },
        "reactiva": {
            "1": 96546,
            "2": 85017,
            "3": 97786,
            "4": 96342,
            "5": 374360,
            "6": 826709,
            "7": 1220207,
            "8": 1365290,
            "9": 1290135,
            "10": 1307149,
            "11": 5295,
            "12": 256840
        },
        "demanda_maxima": {
            "base": {
                "1": 3355,
                "2": 3103,
                "3": 3220,
                "4": 2762,
                "5": 2602,
                "6": 3099,
                "7": 3599,
                "8": 3694,
                "9": 3763,
                "10": 3611,
                "11": 3632,
                "12": 3541
            },
            "intermedio": {
                "1": 3259,
                "2": 3119,
                "3": 3172,
                "4": 2657,
                "5": 2648,
                "6": 3140,
                "7": 3597,
                "8": 3690,
                "9": 3783,
                "10": 3595,
                "11": 3600,
                "12": 3996
            },
            "punta": {
                "1": 3246,
                "2": 3106,
                "3": 3150,
                "4": 2618,
                "5": 2313,
                "6": 3185,
                "7": 3418,
                "8": 3572,
                "9": 3651,
                "10": 3570,
                "11": 3457,
                "12": 3442
            }
        }
    }
}
        """

        
        print(x_json)

        x  = requests.get('https://flask-test-repositorio.azurewebsites.net', json = x_json)

        print(x)
        print("hola")
        return render_template('MODELO_mercados/inputs_modelo.html')
       

########################################################################
########################################################################

def crear_excel(excel_lista, archivo_zip=None):
    """Esta función crea hojas de cálculo a partir de uno o más diccionarios con un DataFrame e información relevante."""

    for excel in excel_lista:

        wb = Workbook()
        ws = wb.active
        ws.title = excel["hojas"][0]["titulo"]

        for fila in dataframe_to_rows(excel["hojas"][0]["dataframe"], index=False, header=True): # Transferir el DataFrame a la hoja de trabajo

            ws.append(fila)

        for pos, hoja in enumerate(excel["hojas"][1:len(excel["hojas"])]):

            ws = wb.create_sheet()
            ws.title = hoja["titulo"]

            for fila in dataframe_to_rows(hoja["dataframe"], index=False, header=True):

                ws.append(fila)

        wb.save(filename="python_webapp_flask/files/downloads/"+excel["nombre"]+".xlsx")

    # Crear un archivo
    if len(excel_lista) == 1:

        return "files/downloads/" + excel_lista[0]["nombre"] + ".xlsx"

    # Crear un zip con diversos archivos
    else:

        pass
        #return "files/" + archivo_zip + ".zip"
        #return "files/"+excel["nombre"]+".xlsx"


@login_required
def descargar_excel(*excel_lista, archivo_zip=None):
    """Esta función llama a la función crear_excel() y retorna una url de descarga."""
    
    # Función llamada desde otro archivo (.html)
    if len(excel_lista) == 0:

        return descargar_excel(excel_lista_global)

    # Función llamada desde este archivo (.py)
    else:

        archivo = crear_excel(excel_lista, archivo_zip)

    return send_file(archivo, as_attachment=True) 


@login_required
def about():
    """Esta función renderiza la página templates/about.html."""

    return render_template("about.html", title="About", year=datetime.now().year, message="Your application description page.")


@login_required
def dashboard_newsletter():
    """Esta función renderiza y procesa los datos de la página templates/dashboard/newsletter.html."""

    if request.method == "GET":
        
        return render_template("/dashboard/newsletter.html")

    elif request.method == "POST":

        consulta_asignada = request.form["consulta_asignada"] # Adquirir datos

        # Newsletter Semana 1
        if consulta_asignada == "1":

            cnxn = crear_conexion_SQL() # Crear conexión con SQL Server

            with cnxn:

                # PML Nacional diario de los últimos 2 meses completos
                tmp_fecha = date.today() - relativedelta.relativedelta(months=2) # Día de hoy 2 meses atrás
                fecha =  date(tmp_fecha.year, tmp_fecha.month, 1).strftime("%Y-%m-%d") # Día 1 de 2 meses atrás
                consulta_sql = """
                                SELECT 
                                    [Fecha] AS 'Fecha', avg([Precio marginal local ($/MWh)]) AS 'Promedio precio marginal local ($/MWh)' 
                                FROM [dbo].[PML(MDA) (SIN)] 
                                WHERE fecha >= '{fecha} 00:00:00.000' 
                                GROUP BY fecha 
                                ORDER BY Fecha ASC
                                """
                consulta_sql = consulta_sql.format(fecha=fecha)
                df = pd.read_sql(consulta_sql, cnxn)
                excel = {"nombre": "PML Nacional diario de los últimos 2 meses completos", "hojas": [{"titulo": "PML diario - 2 meses", "dataframe": df}]}
                

                # PML Nacional mensual de los últimos 36 meses completos
                tmp_fecha = date.today() - relativedelta.relativedelta(months=36) # Día de hoy 36 meses atrás
                fecha =  date(tmp_fecha.year, tmp_fecha.month, 1).strftime("%Y-%m-%d") # Día 1 de 36 meses atrás
                consulta_sql = """
                                SELECT 
                                    year([Fecha]) AS 'Año', month(fecha) AS 'Mes', avg([Precio marginal local ($/MWh)]) AS 'Promedio precio marginal local ($/MWh)' 
                                FROM [dbo].[PML(MDA) (SIN)] 
                                WHERE fecha >= '{fecha} 00:00:00.000' 
                                GROUP BY year([Fecha]), month(fecha) 
                                ORDER BY year([Fecha]) ASC , month(fecha) ASC
                                """
                consulta_sql = consulta_sql.format(fecha=fecha)
                df = pd.read_sql(consulta_sql, cnxn)
                excel_2 = {"nombre": "PML Nacional mensual de los últimos 36 meses completos", "hojas": [{"titulo":"PML mensual - 36 meses", "dataframe": df}]}

                # Demanda de Energía Eléctrica semanal de últimos 12 meses completos
                # Demanda Pronosticada
                tmp_fecha = date.today() - relativedelta.relativedelta(months=12) # Día de hoy 12 meses atrás
                fecha =  date(tmp_fecha.year, tmp_fecha.month, 1).strftime("%Y-%m-%d") # Día 1 de 12 meses atrás
                consulta_sql = """
                                SELECT 
                                    year([Fecha]) AS 'Año', month([Fecha]) AS 'Mes', datepart(ww, Fecha) AS 'Semana', 
                                    round(avg([ Pronostico (MWh)]), 2, 0) AS 'Promedio pronóstico (MWh)' 
                                FROM [dbo].[Demanda Pronosticada] 
                                WHERE fecha >= '{fecha}' 
                                GROUP BY year(Fecha), month(Fecha), datepart(ww, Fecha) 
                                ORDER BY year(fecha) ASC, datepart(ww, Fecha) ASC
                                """
                consulta_sql = consulta_sql.format(fecha=fecha)
                df = pd.read_sql(consulta_sql, cnxn)
                hoja = {"titulo": "Demanda Pronosticada", "dataframe": df}
                # Demanda Real/Promedio
                consulta_sql = """
                                SELECT 
                                    year(fecha) AS 'Año', month(fecha) AS 'Mes', datepart(ww, Fecha) AS 'Semana', round(avg(SIN), 2, 0) AS 'Promedio SIN' 
                                FROM [dbo].[Demanda Promedio Diaria] 
                                WHERE fecha >= '{fecha}' 
                                GROUP BY year(fecha), month(fecha), datepart(ww, Fecha) 
                                ORDER BY year(fecha) ASC, datepart(ww, Fecha) ASC
                                """
                consulta_sql = consulta_sql.format(fecha=fecha)
                df = pd.read_sql(consulta_sql, cnxn)
                hoja_2 = {"titulo": "Demanda Real - Promedio", "dataframe": df}
                # Demanda Máxima
                consulta_sql = """
                                SELECT 
                                    year(fecha) AS 'Año', month(fecha) AS 'Mes', datepart(ww, Fecha) AS 'Semana', round(avg(SIN), 2, 0) AS 'Promedio SIN' 
                                FROM [dbo].[Demanda Promedio Maxima] 
                                WHERE fecha >= '{fecha}' 
                                GROUP BY year(fecha), month(fecha), datepart(ww, Fecha) 
                                ORDER BY year(fecha) ASC, datepart(ww, Fecha) ASC
                                """
                consulta_sql = consulta_sql.format(fecha=fecha)
                df = pd.read_sql(consulta_sql, cnxn)
                hoja_3 = {"titulo": "Demanda Máxima", "dataframe": df}
                excel_3 = {"nombre": "Demanda de Energía Eléctrica semanal de los últimos 12 meses completos", "hojas": [hoja, hoja_2, hoja_3]}

                # PMZs por Área CENACE de últimos 2 meses completos
                # SIN
                tmp_fecha = date.today() - relativedelta.relativedelta(months=2) # Día de hoy 2 meses atrás
                fecha =  date(tmp_fecha.year, tmp_fecha.month, 1).strftime("%Y-%m-%d") # Día 1 de 2 meses atrás
                consulta_sql = """
                                SELECT 
                                    year(pmz.Fecha) AS 'Año', month(pmz.Fecha) AS 'Mes', avg(pmz.[Precio Zonal  ($/MWh)]) AS 'Promedio precio zonal ($/MWh)', area.Area AS 'Área' 
                                FROM [PMZ(MDA) (SIN)] AS pmz LEFT JOIN Areas AS area ON pmz.[Zona de Carga] = area.Zona 
                                WHERE fecha >= '{fecha}' 
                                GROUP BY year(pmz.Fecha), month(pmz.Fecha), area.Area 
                                ORDER BY area.Area ASC, year(pmz.Fecha) ASC, month(pmz.Fecha) ASC
                                """
                consulta_sql = consulta_sql.format(fecha=fecha)
                df = pd.read_sql(consulta_sql, cnxn)
                hoja = {"titulo": "PMZ SIN", "dataframe": df}
                # BCA
                consulta_sql = """
                                SELECT 
                                    year(pmz.Fecha) AS 'Año', month(pmz.Fecha) AS 'Mes', avg(pmz.[Precio Zonal  ($/MWh)]) AS 'Promedio precio zonal ($/MWh)', area.Area AS 'Área' 
                                FROM [PMZ(MDA) (BCA)] AS pmz LEFT JOIN Areas AS area ON pmz.[Zona de Carga] = area.Zona 
                                WHERE fecha >= '{fecha}' 
                                GROUP BY year(pmz.Fecha), month(pmz.Fecha), area.Area 
                                ORDER BY area.Area ASC, year(pmz.Fecha) ASC, month(pmz.Fecha) ASC
                                """
                consulta_sql = consulta_sql.format(fecha=fecha)
                df = pd.read_sql(consulta_sql, cnxn)
                hoja_2 = {"titulo": "PMZ BCA", "dataframe": df}
                # BCS
                consulta_sql = """
                                SELECT 
                                    year(pmz.Fecha) AS 'Año', month(pmz.Fecha) AS 'Mes', avg(pmz.[Precio Zonal  ($/MWh)]) AS 'Promedio precio zonal ($/MWh)', area.Area AS 'Área' 
                                FROM [PMZ(MDA) (BCS)] AS pmz LEFT JOIN Areas AS area ON pmz.[Zona de Carga] = area.Zona 
                                WHERE fecha >= '{fecha}' 
                                GROUP BY year(pmz.Fecha), month(pmz.Fecha), area.Area 
                                ORDER BY area.Area ASC, year(pmz.Fecha) ASC, month(pmz.Fecha) ASC
                                """
                consulta_sql = consulta_sql.format(fecha=fecha)
                df = pd.read_sql(consulta_sql, cnxn)
                hoja_3 = {"titulo": "PMZ BCS", "dataframe": df}
                excel_4 = {"nombre": "PMZs por Área CENACE de últimos 2 meses completos", "hojas": [hoja, hoja_2, hoja_3]}

                # Servicios Conexos mensual desde enero 2018
                consulta_sql = """
                                SELECT 
                                    year([Fecha]) AS 'Año', month([Fecha]) AS 'Mes', [Tipo de reserva] AS 'Tipo de reserva', 
                                    round(avg([Precio de la reserva ($/MW por hora)]), 2, 0) AS 'Promedio precio de la reserva ($/MWh)' 
                                FROM [dbo].[Servicios Conexos (MDA) (SIN)] 
                                WHERE fecha >= '2018-01-01 00:00:00.000' 
                                GROUP BY year(Fecha), month(Fecha), [Tipo de reserva] 
                                ORDER BY year(Fecha) ASC, month(Fecha) ASC
                                """
                df = pd.read_sql(consulta_sql, cnxn)
                excel_5 = {"nombre": "Servicios Conexos mensual desde enero 2018", "hojas": [{"titulo":"S. C. mensual - Ene 2018", "dataframe": df}]}

            return descargar_excel(excel, excel_2, excel_3, excel_4, excel_5)

        # Newsletter Semana 3
        elif consulta_asignada == '3':
            """
            Para más información sobre investpy véase esta liga: https://readthedocs.org/projects/investpy/downloads/pdf/latest/
            """

            tmp_fecha = date.today() - relativedelta.relativedelta(months=12) # Día de hoy 12 meses atrás
            tmp_fecha_2 = date.today() # Día de hoy
            fecha =  date(tmp_fecha.year, tmp_fecha.month, 1).strftime("%d/%m/%Y") # Día 1 de 12 meses atrás
            fecha_2 =  (date(tmp_fecha_2.year, tmp_fecha_2.month, 1) - relativedelta.relativedelta(days=1)).strftime("%d/%m/%Y") # Día último de 1 mes atrás

            # Crudo Brent semanal de últimos 12 meses completos
            df = investpy.commodities.get_commodity_historical_data(commodity="brent oil", from_date=fecha, to_date=fecha_2, interval="Weekly")
            df.reset_index(inplace=True)
            hoja = {"titulo": "Brent", "dataframe": df}

            # Gas Natural Henry Hub semanal de últimos 12 meses completos
            df = investpy.commodities.get_commodity_historical_data(commodity="natural gas", from_date=fecha, to_date=fecha_2, interval="Weekly")
            df.reset_index(inplace=True)
            hoja_2 = {"titulo": "Henry", "dataframe": df}

            # Carbón API2 semanal de últimos 12 meses completos
            fecha =  date(tmp_fecha.year, tmp_fecha.month, 1).strftime("%Y-%m-%d") # Día 1 de 12 meses atrás
            fecha_2 =  (date(tmp_fecha_2.year, tmp_fecha_2.month, 1) - relativedelta.relativedelta(days=1)).strftime("%Y-%m-%d") # Día último de 1 mes atrás
            df = yf.download("MTF=F", start=fecha, end=fecha_2, interval="1wk")
            df.reset_index(inplace=True)
            hoja_3 = {"titulo": "API2", "dataframe": df}

            excel = {"nombre": "Newsletter Semana 3", "hojas": [hoja, hoja_2, hoja_3]}
            
            return descargar_excel(excel)


#---------- Precios de mercado ----------#
@login_required
def dashboard_precios_de_mercado():
    """Esta función renderiza y procesa los datos de la página dashboard/precios_de_mercado.html."""
    
    if request.method == "GET":


        nodos_sin, nodos_bca, nodos_bcs, zonas_sin, zonas_bca, zonas_bcs = pdM.obtener_nodos()

        return render_template("/dashboard/precios_de_mercado.html", fechas_max='2030-01-01',#VariablesGlobales.fechas_pdm_max, 
                                                                        fechas_min='2018-01-01',#VariablesGlobales.fechas_pdm_min,
                                                                        nodos_sin=nodos_sin, nodos_bca=nodos_bca, nodos_bcs=nodos_bcs, 
                                                                        zonas_sin=zonas_sin, zonas_bca=zonas_bca, zonas_bcs=zonas_bcs)

        

    elif request.method == "POST" and validar_formulario(request):
        
        fecha = request.form["fecha"]
        fecha_2 = request.form["fecha_2"]
        bd_tabla = request.form["bd_tabla"]
        tipo_de_datos = request.form["tipo_de_datos"]
        region = request.form["region"]
        
        cnxn = crear_conexion_SQL()
        
        with cnxn:

            # TODOS
            if tipo_de_datos == "Todos":
            
                consulta_asignada = request.form["consulta_asignada"]
                excel, resultados, consulta_asignada, bd_tabla, tipo_de_datos, titulo_grafica=pdM.tipo_datos_todos(fecha, fecha_2, bd_tabla, tipo_de_datos, region, consulta_asignada)
                

            # Individual
            elif tipo_de_datos == "Individual":

                consulta_asignada = request.form["consulta_asignada_2"]

                if bd_tabla.startswith("PMZ"):

                    if region == "(SIN)":
                        zona_nodo = request.form["zona"]

                    if region == "(BCA)":
                        zona_nodo = request.form["zona2"]

                    if region == "(BCS)":
                        zona_nodo = request.form["zona3"]

                else:
                    if region == "(SIN)":
                        zona_nodo = request.form["nodo"]

                    if region == "(BCA)":
                        zona_nodo = request.form["nodo2"]

                    if region == "(BCS)":
                        zona_nodo = request.form["nodo3"]

                    

                excel, resultados, consulta_asignada, bd_tabla, tipo_de_datos, titulo_grafica, zona_nodo=pdM.tipo_datos_individual(fecha, fecha_2, bd_tabla, tipo_de_datos, region, consulta_asignada, zona_nodo)
 
        if request.form["boton_submit"] == "Consulta":
            
            global excel_lista_global
            
            excel_lista_global = excel
            etiquetas, dataset = graficaZoom("Precios de mercado", resultados, consulta_asignada, bd_tabla, tipo_de_datos)

            return render_template("dashboard/z.html", title="Precios de mercado", titulo_grafica=titulo_grafica, dataset=dataset, etiquetas=etiquetas)

        elif request.form["boton_submit"] == "Descarga":

            return descargar_excel(excel)

    else:

        return redirect(request.url)

#---------- PPA ----------#
def dashboard_PPA():

    if request.method == "GET":
        
        nodos_sin, zonas_sin=pdM.cargar_nodos_PPA()
        return render_template("/dashboard/PPA.html", nodos_sin=nodos_sin, zonas_sin=zonas_sin)

    elif request.method == "POST":


        fecha = request.form['fecha']
        fecha_2 = request.form['fecha_2']

        cobertura = request.form['cobertura']
        componenteE = str(request.form['componenteE'])
        regionC = request.form['regionC']
        regionI = request.form['regionI']

        zona = request.form["zona"]
        zonaI = request.form["zonaI"]
        nodo = request.form["nodo"]
        nodoI = request.form["nodoI"]
        print(fecha, fecha_2, cobertura, componenteE, regionC, regionI, zona, zonaI, nodo, nodoI)

        excel, resultados=pdM.consultar_PPA(fecha, fecha_2, cobertura, componenteE, regionC, regionI, zona, zonaI, nodo, nodoI)
        
        if request.form["boton_submit"] == "Consulta":
            global excel_lista_global

            
            #dataset = [{label1: values}, {label2: values2}, {label3: values3}, {label4: values4}]
            
            excel_lista_global = excel
            labels, dataset = formato_grafica("PPA", resultados, consulta_asignada=None, bd_tabla=None)
            return render_template("/datatable.html", title="PPA", labels=labels, dataset=dataset) #, resultados=df.to_html(classes=['table table-bordered dataTable" id = "dataTable'])

        elif request.form["boton_submit"] == "Descarga":
            return descargar_excel(excel)

    return render_template('/dashboard/PPA.html')
#########################PPA################################

#---------- PPA ----------#


def dashboard_CVolatilidad():

    if request.method == "GET":

        return render_template("/dashboard/calculo_volatilidad.html")

    elif request.method == "POST":


        fecha = request.form['fecha']
        fecha_2 = request.form['fecha_2']

        bd_tabla = request.form['bd_tabla']


        excel, resultados= pdM.calculo_volatilidad(fecha, fecha_2, bd_tabla)

        

        if request.form["boton_submit"] == "Consulta":
            global excel_lista_global
            excel_lista_global = excel
            return render_template("/datatable.html", title="Volatilidad", cobertura=resultados.to_html(classes=["table-bordered", "table-striped", "table-hover", "table"]))#(classes=['table table-bordered dataTable" id = "dataTable'])) #, resultados=df.to_html(classes=['table table-bordered dataTable" id = "dataTable'])

        elif request.form["boton_submit"] == "Descarga":
            return descargar_excel(excel)

    return render_template('/dashboard/PPA.html')
#########################PPA################################


def dashboard_Areas_y_Zonas():

    if request.method == "GET":
        

        area_, zona_=tCFE.get_zona_area()


        return render_template("/dashboard/areas_zonas.html",zona_=zona_,area_=area_)



    elif request.method == "POST":



        bd_tabla = request.form['bd_tabla']



        cnxn = crear_conexion_SQL()
        
        with cnxn:
            if bd_tabla == "Area":
                zona_area = request.form['area']

                resultados=tCFE.consulta_area(zona_area)


            elif bd_tabla == "Zona":
                zona_area = request.form['zona']
                resultados=tCFE.consulta_zona(zona_area)

        if request.form["boton_submit"] == "Consulta":
            
            return render_template("/datatable.html", title="Areas y Zonas", cobertura=resultados.to_html(classes=["table-bordered", "table-striped", "table-hover", "table"]))#(classes=['table table-bordered dataTable" id = "dataTable'])) #, resultados=df.to_html(classes=['table table-bordered dataTable" id = "dataTable'])


    return render_template('/areas_zonas.html')
#########################PPA################################


def verificar_contrasena():
    if request.method=='GET':
        return render_template("/dashboard/dialogoContrasena.html")
    
    if request.method=='POST':
        contrasena=request.form['pass']
        if contrasena=='ODCBeetmann':
            return redirect(url_for("dashboard_WS_ODC"))
        return redirect(url_for('contact'))


def verificar_contrasena_temporal():
    if request.method=='GET':
        return render_template("/dashboard/dialogoContrasena.html")
    
    if request.method=='POST':
        contrasena=request.form['pass']
        if contrasena=='ODCBeetmann':
            return redirect(url_for("dashboard_estados_de_cuenta"))
        return redirect(url_for('contact'))
########################################################


def dashboard_A():
    
    if request.method == "GET":
        
        return render_template("/dashboard/A.html")
    
    elif request.method == "POST":

        fechas = request.form.get("arreglo_fechas")
        fechas = fechas.split(",")
        tablas = []
        #resultados = pd.DataFrame()
        cnxn = crear_conexion_SQL()
        #print(resultados)

        with cnxn:

            for pos, fecha in enumerate(fechas):

                consulta_sql = """
                                SELECT 
                                    year([Fecha]) AS 'Año', month([Fecha]) AS 'Mes', day([Fecha]) AS 'Día', hora as 'Hora', 
                                    round(avg([Precio Zonal  ($/MWh)]), 2, 0) AS 'Promedio precio zonal ($/MWh)' 
                                FROM [dbo].[PMZ(MDA) (SIN)] 
                                WHERE fecha >= '{fecha}' AND fecha <= '{fecha}' 
                                GROUP BY year(Fecha), month(Fecha), day([Fecha]), hora  
                                ORDER BY year(Fecha) ASC, month(Fecha) ASC, day([Fecha]) ASC, hora ASC
                                """

                consulta_sql = consulta_sql.format(fecha=fecha)
                df = pd.read_sql(consulta_sql, cnxn)
                df.rename(columns = {list(df)[-1]: "(" + fecha + ") " + df.columns[-1]}, inplace=True)
                print(df)
                col = df[df.columns[-1]]

                if pos == 0:

                    resultados = pd.DataFrame(df["Año"])
                    resultados["Mes"] = df["Mes"]
                    resultados["Día"] = df["Día"]
                    resultados["Hora"] = df["Hora"]

                    resultados[df.columns[-1]] = col
                    print("YYYY:\n", resultados)
                
                else:

                    resultados[df.columns[-1]] = col
                #tablas.append(col)
                
                print(resultados)
                #if pos == 0:
                    
                
                #else:
                    #df = pd.read_sql(consulta_sql, cnxn)
                    #resultados = resultados.append(df)
                    #resultados = pd.read_sql(consulta_sql, cnxn)

                #print("Fecha: ", fecha)
                #print("df\n", df)
                #print("resultados\n", resultados)
        #print("resultados\n", resultados)

        #consulta_sql = consulta_sql.format(region=region, fecha=fecha, fecha_2=fecha_2)

        excel = {"nombre": "Precios de mercado - Test", "hojas": [{"titulo": "Test", "dataframe": resultados}]}
        titulo_grafica = "Test"

        if request.form["boton_submit"] == "Consulta":
            
            global excel_lista_global
            
            excel_lista_global = excel
            etiquetas, dataset = graficaZoom("Precios de mercado", resultados, "Promedio 24h (diario)", "PMZ (MDA)", "Individual")

            return render_template("dashboard/z.html", title="Precios de mercado", titulo_grafica=titulo_grafica, dataset=dataset, etiquetas=etiquetas)

        elif request.form["boton_submit"] == "Descarga":

            return descargar_excel(excel)


########################################################
def dashboard_facturas():
    
    if request.method == "GET":
        
        return render_template("/dashboard/consulta_de_facturas.html")
        #return render_template("/dashboard/consulta_de_facturas.html", fecha_max=VariablesGlobales.fechas_cdf_max)
    
    elif request.method == "POST" and validar_formulario(request):

        servicio = request.form["servicio"]
        sistema = request.form["sistema"]
        participante = request.form["participante"]
        fecha = request.form["fecha"]

        try:
        
            url = "https://ws01.cenace.gob.mx:8081/WSDownLoadFac/FacturaCENService.svc?singleWsdl"
            cliente = Client(url)

        except Exception:

            flash("No se pudo establecer la conexión con el servidor")

        try:
        
            # Descarga de Estado de Cuenta de Subcuenta individual
            if servicio == "FacturaCEN":
                
                subcuenta = request.form["subcuenta"]

                archivos = []
                total_tipos = 0

                if request.form.get("xml"):

                    resultado = cliente.service.GetFacturaCEN("BTMNN" , "BTMNSIN", fecha, sistema, participante, subcuenta, "X")

                    # Se accede al objeto  y se lee con el decodificador ZipFIle la cadena ("r"), que debido a que no es un archivo
                    # se utiliza la libreria io.BytesIO que indica que se esta leyendo bytes de entrada.
                    archivo_zip = zipfile.ZipFile(io.BytesIO(resultado.File_X), "r")
                    archivo_zip.extractall(path="python_webapp_flask/files/downloads")
                    lista_archivos = archivo_zip.namelist()
                    archivos.append(lista_archivos[0])
                    total_tipos += 1

                if request.form.get("pdf"):
                    
                    resultado = cliente.service.GetFacturaCEN("BTMNN" , "BTMNSIN", fecha, sistema, participante, subcuenta, "P")
                    archivo_zip = zipfile.ZipFile(io.BytesIO(resultado.File_P), "r")
                    archivo_zip.extractall(path="python_webapp_flask/files/downloads")
                    lista_archivos = archivo_zip.namelist()
                    archivos.append(lista_archivos[0])
                    total_tipos += 1

                if total_tipos == 0:

                    flash("Seleccione al menos un formato de archivo")

                    return redirect(request.url)

                elif total_tipos == 1:

                    return send_file("files/downloads/" + archivos[0], as_attachment=True) 

                else:

                    nuevo_zip_nombre = "facturas"

                    with zipfile.ZipFile("python_webapp_flask/files/downloads/" + nuevo_zip_nombre + ".zip", "w") as nuevo_zip:

                        for nuevo_archivo in archivos:

                            nuevo_zip.write("python_webapp_flask/files/downloads/" + nuevo_archivo, nuevo_archivo)

                    return send_file("files/downloads/" + nuevo_zip_nombre + ".zip", as_attachment=True)

            # Descarga de Estado de Cuenta de varias Subcuentas
            elif servicio == "FacturasCEN":

                archivos = []
                total_tipos = 0

                if request.form.get("xml"):

                    resultado = cliente.service.GetFacturasCEN("BTMNN" , "BTMNSIN", fecha, sistema, participante, "", "X")
                    
                    for cuenta in resultado["FACT"]["FacturaCEN"]:

                        archivo_zip = zipfile.ZipFile(io.BytesIO(cuenta.File_X), "r")
                        archivo_zip.extractall(path="python_webapp_flask/files/downloads")
                        lista_archivos = archivo_zip.namelist()
                        archivos.append(lista_archivos[0])

                    total_tipos += 1

                if request.form.get("pdf"):
                    
                    resultado = cliente.service.GetFacturasCEN("BTMNN" , "BTMNSIN", fecha, sistema, participante, "", "P")

                    for cuenta in resultado["FACT"]["FacturaCEN"]:

                        archivo_zip = zipfile.ZipFile(io.BytesIO(cuenta.File_P), "r")
                        archivo_zip.extractall(path="python_webapp_flask/files/downloads")
                        lista_archivos = archivo_zip.namelist()
                        archivos.append(lista_archivos[0])

                    total_tipos += 1

                if total_tipos == 0:

                    flash("Seleccione al menos un formato de archivo")

                    return redirect(request.url)

                else:

                    nuevo_zip_nombre = "Facturas"

                    with zipfile.ZipFile("python_webapp_flask/files/downloads/" + nuevo_zip_nombre + ".zip", "w") as nuevo_zip:

                        for nuevo_archivo in archivos:

                            nuevo_zip.write("python_webapp_flask/files/downloads/" + nuevo_archivo, nuevo_archivo)

                    return send_file("files/downloads/" + nuevo_zip_nombre + ".zip", as_attachment=True)

                # Después de descargarlos hay que eliminar los archivos

        except Exception:

            flash("No se encontró información de factura(s) de cuenta para los datos proporcionados")

            return redirect(request.url)

    else:

        return redirect(request.url)


########################################################
def facturacion_y_edc():
    """
    Proporcionar la visualización o descarga de los archivos correspondientes a los estados de cuenta(s) mediante
    la solicitud del Web Service del CENACE para estatods de cuenta.

    Solicitud del servicio;
        La solicitud de servicio en zeep es bajo la forma: cliente . service . método(parametros)
        
        Notas:
            - cliente es una instancia de tipo 'zeep.client.Client'
            - service es una palabra reservada para indicar que se solicita un servicio
            - método debe ser un método declarado por el proveedor del servicio SOAP, 
              y se le debe agregar sus respectivos parametros

    Parametros
    ----------
    GET y POST

    Retorna
    -------
    booleano
        Una variable booleana usada para informar si el usuario ha proporcionado correctamente los datos del formulario
    """




    """
    Solicitud del servicio;
        La solicitud de servicio en zeep es bajo la forma: cliente . service . método(parametros)
        Notas:
        - cliente es una instancia de tipo 'zeep.client.Client'
        - service es una palabra reservada para indicar que estamos solicitando un servicio
        - método debe ser un método declarado por el proveedor del servicio SOAP, 
          al método se le deberá de agregar sus respectivos parametros.
    """
    
    if request.method == "GET":
        
        return render_template("/dashboard/facturacion_y_edc.html")
        #return render_template("/dashboard/estados_de_cuenta.html", fecha_max=VariablesGlobales.fechas_edc_max)
    
    elif request.method == "POST" and validar_formulario(request):

        servicio = request.form.get("servicio")
        sistema = request.form.get("sistema")
        participante = 'C038'
        day, month, year = int(request.form["fecha"][8:10]), int(request.form["fecha"][5:7]), int(request.form["fecha"][0:4])
        fecha = datetime(year, month, day)
        current_date = fecha
        fecha_2 = datetime(int(request.form["fecha_2"][0:4]), int(request.form["fecha_2"][5:7]), int(request.form["fecha_2"][8:10])) if request.form.get("rango") else fecha

        # Facturación
        if request.form.get("herramienta") == "facturacion":

            try:            
                url = "https://ws01.cenace.gob.mx:8081/WSDownLoadFac/FacturaCENService.svc?singleWsdl"
                cliente = Client(url)

            except Exception:
                flash("No se pudo establecer la conexión con el servidor")
                return redirect(request.url)

            try:
            
                # Descarga de Estado de Cuenta de Subcuenta individual
                if servicio == "FacturaCEN":
                    
                    subcuenta = request.form.get("subcuenta")
                    archivos = []

                    if not request.form.get("xml") and not request.form.get("pdf") and not request.form.get("csv") and not request.form.get("html"):
                        flash("Seleccione al menos un formato de archivo")
                        return redirect(request.url)

                    while current_date <= fecha_2:

                        if request.form.get("xml"):
                            # Se accede al objeto  y se lee con el decodificador ZipFIle la cadena ("r"), que debido a que no es un archivo
                            # se utiliza la libreria io.BytesIO que indica que se esta leyendo bytes de entrada.
                            resultado = cliente.service.GetFacturaCEN("BTMNN" , "BTMNSIN", current_date - timedelta(days=7), sistema, participante, subcuenta, "X")
                            archivo_zip = zipfile.ZipFile(io.BytesIO(resultado.File_X), "r")
                            archivo_zip.extractall(path="python_webapp_flask/files/downloads")
                            lista_archivos = archivo_zip.namelist()
                            archivos.append(lista_archivos[0])

                        if request.form.get("pdf"):
                            resultado = cliente.service.GetFacturaCEN("BTMNN" , "BTMNSIN", current_date - timedelta(days=7), sistema, participante, subcuenta, "P")
                            archivo_zip = zipfile.ZipFile(io.BytesIO(resultado.File_P), "r")
                            archivo_zip.extractall(path="python_webapp_flask/files/downloads")
                            lista_archivos = archivo_zip.namelist()
                            archivos.append(lista_archivos[0])

                        current_date += timedelta(days=1)

                    # Only one file created (no zip needed)
                    if len(archivos) == 1:
                        return send_file("files/downloads/" + archivos[0], as_attachment=True)

                    nuevo_zip_nombre = "facturas"
                    with zipfile.ZipFile("python_webapp_flask/files/downloads/" + nuevo_zip_nombre + ".zip", "w") as nuevo_zip:
                        for nuevo_archivo in archivos:
                            nuevo_zip.write("python_webapp_flask/files/downloads/" + nuevo_archivo, nuevo_archivo)

                    return send_file("files/downloads/" + nuevo_zip_nombre + ".zip", as_attachment=True)

                # Descarga de Estado de Cuenta de varias Subcuentas
                elif servicio == "FacturasCEN":

                    archivos = []

                    if not request.form.get("xml") and not request.form.get("pdf") and not request.form.get("csv") and not request.form.get("html"):
                        flash("Seleccione al menos un formato de archivo")
                        return redirect(request.url)

                    while current_date <= fecha_2:

                        if request.form.get("xml"):
                            resultado = cliente.service.GetFacturasCEN("BTMNN" , "BTMNSIN", current_date - timedelta(days=7), sistema, participante, "", "X")
                            for cuenta in resultado["FACT"]["FacturaCEN"]:
                                archivo_zip = zipfile.ZipFile(io.BytesIO(cuenta.File_X), "r")
                                archivo_zip.extractall(path="python_webapp_flask/files/downloads")
                                lista_archivos = archivo_zip.namelist()
                                archivos.append(lista_archivos[0])

                        if request.form.get("pdf"):
                            resultado = cliente.service.GetFacturasCEN("BTMNN" , "BTMNSIN", current_date - timedelta(days=7), sistema, participante, "", "P")
                            for cuenta in resultado["FACT"]["FacturaCEN"]:
                                archivo_zip = zipfile.ZipFile(io.BytesIO(cuenta.File_P), "r")
                                archivo_zip.extractall(path="python_webapp_flask/files/downloads")
                                lista_archivos = archivo_zip.namelist()
                                archivos.append(lista_archivos[0])

                        current_date += timedelta(days=1)

                    nuevo_zip_nombre = "Facturas"
                    with zipfile.ZipFile("python_webapp_flask/files/downloads/" + nuevo_zip_nombre + ".zip", "w") as nuevo_zip:
                        for nuevo_archivo in archivos:
                            nuevo_zip.write("python_webapp_flask/files/downloads/" + nuevo_archivo, nuevo_archivo)

                    return send_file("files/downloads/" + nuevo_zip_nombre + ".zip", as_attachment=True)

            except Exception:
                flash("No se encontró información de factura(s) de cuenta para los datos proporcionados")
                return redirect(request.url)
        
        # EDC
        else:
            
            try:
                # Se declara la url del servicio y se crea el cliente del servicio
                url = "https://ws01.cenace.gob.mx:9081/WSDownLoadEdoCta/EdoCuentaService.svc?wsdl"
                #url = "https://ws01.cenace.gob.mx:8081/WSDownLoadEdoCta/EdoCuentaService.svc?wsdl" #---------> ambiente de producción.
                cliente = Client(url)
            
            except Exception:
                flash("No se pudo establecer la conexión con el servidor")
                return redirect(request.url)

            try:
                # Descarga de Estado de Cuenta de Subcuenta individual
                if servicio == "EdoCuenta":
                    
                    subcuenta = request.form.get("subcuenta")
                    archivos = []

                    if not request.form.get("xml") and not request.form.get("pdf") and not request.form.get("csv") and not request.form.get("html"):
                        flash("Seleccione al menos un formato de archivo")
                        return redirect(request.url)

                    while current_date <= fecha_2:

                        print(current_date)
                        if request.form.get("xml"):
                            # Se accede al objeto  y se lee con el decodificador ZipFIle la cadena ("r"), que debido a que no es un archivo
                            # se utiliza la libreria io.BytesIO que indica que se esta leyendo bytes de entrada.
                            resultado = cliente.service.GetEstadoCuenta("BTMNN" , "BTMNSIN", current_date - timedelta(days=7), sistema, participante, subcuenta, "X")
                            archivo_zip = zipfile.ZipFile(io.BytesIO(resultado.File_X), "r")
                            archivo_zip.extractall(path="python_webapp_flask/files/downloads")
                            lista_archivos = archivo_zip.namelist()
                            archivos.append(lista_archivos[0])

                        if request.form.get("pdf"):
                            resultado = cliente.service.GetEstadoCuenta("BTMNN" , "BTMNSIN", current_date - timedelta(days=7), sistema, participante, subcuenta, "P")
                            archivo_zip = zipfile.ZipFile(io.BytesIO(resultado.File_P), "r")
                            archivo_zip.extractall(path="python_webapp_flask/files/downloads")
                            lista_archivos = archivo_zip.namelist()
                            archivos.append(lista_archivos[0])

                        if request.form.get("csv"):
                            resultado = cliente.service.GetEstadoCuenta("BTMNN" , "BTMNSIN", current_date - timedelta(days=7), sistema, participante, subcuenta, "C")
                            archivo_zip = zipfile.ZipFile(io.BytesIO(resultado.File_C), "r")
                            archivo_zip.extractall(path="python_webapp_flask/files/downloads")
                            lista_archivos = archivo_zip.namelist()
                            archivos.append(lista_archivos[0])

                        if request.form.get("html"):
                            resultado = cliente.service.GetEstadoCuenta("BTMNN" , "BTMNSIN", current_date - timedelta(days=7), sistema, participante, subcuenta, "H")
                            archivo_zip = zipfile.ZipFile(io.BytesIO(resultado.File_H), "r")
                            archivo_zip.extractall(path="python_webapp_flask/files/downloads")
                            lista_archivos = archivo_zip.namelist()
                            archivos.append(lista_archivos[0])

                        current_date += timedelta(days=1)

                    # Only one file created (no zip needed)
                    if len(archivos) == 1:
                        return send_file("files/downloads/" + archivos[0], as_attachment=True)
                    
                    nuevo_zip_nombre = "estado_de_cuenta"
                    with zipfile.ZipFile("python_webapp_flask/files/downloads/" + nuevo_zip_nombre + ".zip", "w") as nuevo_zip:
                        for nuevo_archivo in archivos:
                            nuevo_zip.write("python_webapp_flask/files/downloads/" + nuevo_archivo, nuevo_archivo)

                    print("Yes")
                    print(archivos)
                    return send_file("files/downloads/" + nuevo_zip_nombre + ".zip", as_attachment=True)

                # Descarga de Estado de Cuenta de varias Subcuentas
                elif servicio == "EdoCuentas":

                    archivos = []

                    if not request.form.get("xml") and not request.form.get("pdf") and not request.form.get("csv") and not request.form.get("html"):
                        flash("Seleccione al menos un formato de archivo")
                        return redirect(request.url)

                    while current_date <= fecha_2:

                        if request.form.get("xml"):
                            resultado = cliente.service.GetEstadoCuentas("BTMNN" , "BTMNSIN", current_date - timedelta(days=7), sistema, participante, "", "X")
                            for cuenta in resultado["EC"]["EdoCuenta"]:
                                archivo_zip = zipfile.ZipFile(io.BytesIO(cuenta.File_X), "r")
                                archivo_zip.extractall(path="python_webapp_flask/files/downloads")
                                lista_archivos = archivo_zip.namelist()
                                archivos.append(lista_archivos[0])

                        if request.form.get("pdf"):
                            resultado = cliente.service.GetEstadoCuentas("BTMNN" , "BTMNSIN", current_date - timedelta(days=7), sistema, participante, "", "P")
                            for cuenta in resultado["EC"]["EdoCuenta"]:
                                archivo_zip = zipfile.ZipFile(io.BytesIO(cuenta.File_P), "r")
                                archivo_zip.extractall(path="python_webapp_flask/files/downloads")
                                lista_archivos = archivo_zip.namelist()
                                archivos.append(lista_archivos[0])

                        if request.form.get("csv"):
                            resultado = cliente.service.GetEstadoCuentas("BTMNN" , "BTMNSIN", current_date - timedelta(days=7), sistema, participante, "", "C")
                            for cuenta in resultado["EC"]["EdoCuenta"]:
                                archivo_zip = zipfile.ZipFile(io.BytesIO(cuenta.File_C), "r")
                                archivo_zip.extractall(path="python_webapp_flask/files/downloads")
                                lista_archivos = archivo_zip.namelist()
                                archivos.append(lista_archivos[0])

                        if request.form.get("html"):
                            resultado = cliente.service.GetEstadoCuentas("BTMNN" , "BTMNSIN", current_date - timedelta(days=7), sistema, participante, "", "H")
                            for cuenta in resultado["EC"]["EdoCuenta"]:
                                archivo_zip = zipfile.ZipFile(io.BytesIO(cuenta.File_H), "r")
                                archivo_zip.extractall(path="python_webapp_flask/files/downloads")
                                lista_archivos = archivo_zip.namelist()
                                archivos.append(lista_archivos[0])

                        current_date += timedelta(days=1)

                    nuevo_zip_nombre = "estados_de_cuenta"
                    with zipfile.ZipFile("python_webapp_flask/files/downloads/" + nuevo_zip_nombre + ".zip", "w") as nuevo_zip:
                        for nuevo_archivo in archivos:
                            nuevo_zip.write("python_webapp_flask/files/downloads/" + nuevo_archivo, nuevo_archivo)

                    return send_file("files/downloads/" + nuevo_zip_nombre + ".zip", as_attachment=True)

            except Exception:
                flash("No se encontró información de estado(s) de cuenta para los datos proporcionados")
                return redirect(request.url)
    else:

        return redirect(request.url)

########################################################


def Facturas():
    if request.method == "GET":
        fecha= datetime.today()

        dias_a_sumar = (2, 1, 0, 6, 5, 4, 3)
        fecha_pago = (fecha + timedelta(days=dias_a_sumar[fecha.weekday()]))
        fecha_pago2 = (fecha_pago + timedelta(7))
        fecha_pago = fecha_pago.strftime('%Y-%m-%d')
        
        fecha_pago2 = fecha_pago2.strftime('%Y-%m-%d')
        cnxn = crear_conexion_SQL()

        with cnxn:
            query = """

            
select  pagos.[Estado de cuenta] + 7  as 'Fecha FUECD'
,pagos.subcuenta as 'Subcuenta'
,pagos.fuf as 'FUF'
,pagos.uuid as 'UUID (Folio Fiscal)'
,pagos.folio as 'Folio Interno'
,tabla.folio_relacionado as 'Folio Relacionado'
,pagos.[Fecha de pago] as 'Fecha Limite de Pago'
,abs(pagos.[Monto + iva]) as 'Monto + Iva'
				
                
		from (SELECT 
		facturas.fuf as 'fufu',
		facturas.Folio as 'folio_relacionado'
		,[Fecha de operación] as 'fecha'
   
		FROM [dbo].[EDC_Pagos] as edc_pagos join Facturas as facturas 
		on facturas.FUF = edc_pagos.FUF
		where facturas.fuf like '%P00' )
as tabla 
			
right join (     
			 
			 
		SELECT [Estado de cuenta]
		,[Subcuenta]
		,pagos.FUF
		,format(Facturas.Folio,'#') as 'folio'
		,facturas.UUID
		,[Fecha de pago]
		,[Fecha de operación]
		,[Monto]
		,[Monto + iva]
		FROM [dbo].[EDC_Pagos] as pagos left join [dbo].[Facturas] as facturas
		on pagos.FUF = facturas.FUF
		where ([Fecha de pago] = '{fecha_pago}'
		or [Fecha de pago] = '{fecha_pago2}')
		and pagos.[FUF] like '%P%' ) 
  as pagos
on tabla.fufu = pagos.FUF
order by pagos.[Estado de cuenta] desc

                    """
            query = query.format(fecha_pago=fecha_pago,fecha_pago2=fecha_pago2)
            df = pd.read_sql(query, cnxn)

        with cnxn:
            query = """
             SELECT 'C0',
        abs(sum([Monto + iva]))
        FROM [dbo].[EDC_Pagos] where ([Fecha de pago]  = '{fecha_pago}') and FUF like '%C0%'

                    """
            query = query.format(fecha_pago=fecha_pago,fecha_pago2=fecha_pago2)
            dfC0 = pd.read_sql(query, cnxn)
            C0=dfC0.iloc[0,1]

        with cnxn:
            query = """
                    SELECT 'P0',
                    abs(sum([Monto + iva]))
                    FROM [dbo].[EDC_Pagos] where ([Fecha de pago]  = '{fecha_pago}' ) 
                   and FUF like '%P0%'
                    """
            query = query.format(fecha_pago=fecha_pago,fecha_pago2=fecha_pago2)
            dfP0 = pd.read_sql(query, cnxn)

            P0=dfP0.iloc[0,1]

        with cnxn:
            query = """
                    
    SELECT 'PC',
    abs(sum([Monto + iva]))
    FROM [dbo].[EDC_Pagos] where ([Fecha de pago]  = '{fecha_pago}' ) and FUF like '%PC%'
                    """
            query = query.format(fecha_pago=fecha_pago,fecha_pago2=fecha_pago2)
            dfPC = pd.read_sql(query, cnxn)
            PC=dfPC.iloc[0,1]

        with cnxn:
            query = """
                   SELECT 'PD',
   abs(sum([Monto + iva]))
    FROM [dbo].[EDC_Pagos] where ([Fecha de pago]  = '{fecha_pago}')  and FUF like '%PD%'
                    """
            query = query.format(fecha_pago=fecha_pago,fecha_pago2=fecha_pago2)
            dfPD = pd.read_sql(query, cnxn)
            PD=dfPD.iloc[0,1]

        with cnxn:
            query = """
   SELECT 'CC',
    abs(sum([Monto + iva]))
    FROM [dbo].[EDC_Pagos] where ([Fecha de pago]  = '{fecha_pago}' )  and FUF like '%CC%'
                    """
            query = query.format(fecha_pago=fecha_pago,fecha_pago2=fecha_pago2)
            dfCC = pd.read_sql(query, cnxn)
            CC=dfCC.iloc[0,1]

        with cnxn:
            query = """
   SELECT 'CD',
    abs(sum([Monto + iva]))
    FROM [dbo].[EDC_Pagos] where ([Fecha de pago]  = '{fecha_pago}' 
        or [Fecha de pago] =  '{fecha_pago2}')  and FUF like '%CD%'
                    """
            query = query.format(fecha_pago=fecha_pago,fecha_pago2=fecha_pago2)
            dfCD = pd.read_sql(query, cnxn)
            CD=dfCD.iloc[0,1]

            

            PC = "${:,.2f}".format(PC)
            PD = "${:,.2f}".format(PD)
            P0 = "${:,.2f}".format(P0)

#por facturar 2

        with cnxn:
            query = """
                SELECT 'C0',
        abs(sum([Monto + iva]))
        FROM [dbo].[EDC_Pagos] where ( [Fecha de pago] =  '{fecha_pago2}') and FUF like '%C0%'

                    """
            query = query.format(fecha_pago=fecha_pago,fecha_pago2=fecha_pago2)
            dfC0 = pd.read_sql(query, cnxn)
            C02=dfC0.iloc[0,1]

        with cnxn:
            query = """
                    SELECT 'P0',
                    abs(sum([Monto + iva]))
                    FROM [dbo].[EDC_Pagos] where ([Fecha de pago] =  '{fecha_pago2}') 
                    and FUF like '%P0%'
                    """
            query = query.format(fecha_pago=fecha_pago,fecha_pago2=fecha_pago2)
            dfP0 = pd.read_sql(query, cnxn)

            P02=dfP0.iloc[0,1]

        with cnxn:
            query = """
                    
    SELECT 'PC',
    abs(sum([Monto + iva]))
    FROM [dbo].[EDC_Pagos] where ([Fecha de pago] =  '{fecha_pago2}') and FUF like '%PC%'
                    """
            query = query.format(fecha_pago=fecha_pago,fecha_pago2=fecha_pago2)
            dfPC = pd.read_sql(query, cnxn)
            PC2=dfPC.iloc[0,1]
            print(dfPC)
           

        with cnxn:
            query = """
                    SELECT 'PD',
    abs(sum([Monto + iva]))
    FROM [dbo].[EDC_Pagos] where ( [Fecha de pago] =  '{fecha_pago2}')  and FUF like '%PD%'
                    """
            query = query.format(fecha_pago=fecha_pago,fecha_pago2=fecha_pago2)
            dfPD = pd.read_sql(query, cnxn)
            PD2=dfPD.iloc[0,1]

        with cnxn:
            query = """
    SELECT 'CC',
    abs(sum([Monto + iva]))
    FROM [dbo].[EDC_Pagos] where ( [Fecha de pago] =  '{fecha_pago2}')  and FUF like '%CC%'
                    """
            query = query.format(fecha_pago=fecha_pago,fecha_pago2=fecha_pago2)
            dfCC = pd.read_sql(query, cnxn)
            CC2=dfCC.iloc[0,1]

        with cnxn:
            query = """
    SELECT 'CD',
    abs(sum([Monto + iva]))
    FROM [dbo].[EDC_Pagos] where ([Fecha de pago] =  '{fecha_pago2}')  and FUF like '%CD%'
                    """
            query = query.format(fecha_pago=fecha_pago,fecha_pago2=fecha_pago2)
            dfCD = pd.read_sql(query, cnxn)
            CD2=dfCD.iloc[0,1]
                
            
            print(PD2)
            if PC2 is not None:
                   
                PC2 = "${:,.2f}".format(PC2)

            if PD2 is not None:
                   
                PD2 = "${:,.2f}".format(PD2)
            if P02 is not None:
                   
                P02 = "${:,.2f}".format(P02)





###############################################33


        with cnxn:
            query = """
             select [fecha de operacion] + 7 as 'Fecha FUECD', fuf  as 'FUF'
			, monto as 'Monto',pagos.folioo as 'Folio Interno de Facturación'
,[Fecha Pago] as 'Fecha de Pago',
            Imp_Pagado as 'Importe pagado'
from(
SELECT 
  [Estado de cuenta] as 'fecha de operacion' ,edc_pagos.fuf as 'fuf',
  factura.Folio as 'folioo',
    ABS([Monto + iva]) AS 'monto'
    FROM [dbo].[EDC_Pagos] as edc_pagos join Facturas as factura
	on edc_pagos.FUF = factura.FUF
	where [Fecha de pago] = '{fecha_pago2}' and( edc_pagos.FUF like '%P00%' or edc_pagos.FUF like '%Pd%')) as pagos 
	left join Complemento_pagos as complemento
	on pagos.folioo = complemento.Folio_Relacionado

union

select [fecha de operacion] + 7 , fuf, monto,pagos.folioo,[Fecha Pago],Imp_Pagado as 'importe pagado'

from(
SELECT 
  [Estado de cuenta] as 'fecha de operacion' ,edc_pagos.fuf as 'fuf',
  factura.Folio as 'folioo',
    ABS([Monto + iva]) AS 'monto'
    FROM [dbo].[EDC_Pagos] as edc_pagos left join CFDI_Emitidos_CENACE as factura
	on edc_pagos.FUF = factura.FUF
	where [Fecha de pago] = '{fecha_pago2}' and( 
	edc_pagos.FUF like '%cC%' )) as pagos 
	left join Complemento_pagos as complemento
	on pagos.folioo = complemento.Folio_Relacionado

                    """
            query = query.format(fecha_pago2=fecha_pago2)
            monto_C15 = pd.read_sql(query, cnxn)
            monto_cobrar15 = monto_C15['Monto'].sum()
            monto_cobrado15 = monto_C15['Importe pagado'].sum()
            monto_cobrar15 = monto_cobrar15 - monto_cobrado15

            excel_cobros15 = {"nombre": "Montos por cobrar al " + fecha_pago2 , "hojas": [{"titulo":"Hoja 1", "dataframe": monto_C15}]}
           
        with cnxn:
            query = """  SELECT [Estado de cuenta] + 7 as 'Fecha FUECD'
            ,fuf AS 'FUF',
    ABS([Monto + iva]) AS 'Monto'
	,pagos_admin.Fecha_vigencia as 'Fecha de Pago'
	,pagos_admin.Importe as 'Importe pagado'
    FROM [dbo].[EDC_Pagos] as edc_pagos 
    left join [dbo].[Pagos_Admin] as pagos_admin
	on edc_pagos.FUF = pagos_admin.Folio
	where [Fecha de pago] =  '{fecha_pago2}' and( 
	FUF like '%PC%' or FUF like '%C00%' or FUF like '%Cd%')
	order by [Estado de cuenta] asc, fuf asc 
                    """
            query = query.format(fecha_pago2=fecha_pago2)
            monto_p15 = pd.read_sql(query, cnxn)
            monto_pagar15 =monto_p15['Monto'].sum()
            monto_pagado15 = monto_p15['Importe pagado'].sum()
            monto_pagar15 = monto_pagar15 - monto_pagado15
          
           
            monto_pagar15 = "${:,.2f}".format(monto_pagar15)  
            monto_cobrar15 = "${:,.2f}".format(monto_cobrar15) 



#########################3333



        with cnxn:
            query = """ 
             select [fecha de operacion] + 7 as 'Fecha FUECD', fuf  as 'FUF'
			, monto,pagos.folioo as 'Folio Interno de Facturación'
,[Fecha Pago] as 'Fecha de Pago',
            Imp_Pagado as 'Importe pagado'
from(
SELECT 
  [Estado de cuenta] as 'fecha de operacion' ,edc_pagos.fuf as 'fuf',
  factura.Folio as 'folioo',
    ABS([Monto + iva]) AS 'monto'
    FROM [dbo].[EDC_Pagos] as edc_pagos left join Facturas as factura
	on edc_pagos.FUF = factura.FUF
	where [Fecha de pago] = '{fecha_pago}' and(  edc_pagos.FUF like '%P00%' or edc_pagos.FUF like '%Pd%')) as pagos 
	left join Complemento_pagos as complemento
	on pagos.folioo = complemento.Folio_Relacionado

union

select [fecha de operacion], fuf, monto,pagos.folioo,[Fecha Pago],Imp_Pagado as 'importe pagado'

from(
SELECT 
  [Estado de cuenta] + 7 as 'fecha de operacion' ,edc_pagos.fuf as 'fuf',
  factura.Folio as 'folioo',
    ABS([Monto + iva]) AS 'monto'
    FROM [dbo].[EDC_Pagos] as edc_pagos left join CFDI_Emitidos_CENACE as factura
	on edc_pagos.FUF = factura.FUF
	where [Fecha de pago] = '{fecha_pago}' and( 
	edc_pagos.FUF like '%cC%')) as pagos 
	left join Complemento_pagos as complemento
	on pagos.folioo = complemento.Folio_Relacionado

                    """
            query = query.format(fecha_pago=fecha_pago)
            monto_C = pd.read_sql(query, cnxn)
            monto_cobrar = monto_C['monto'].sum()
            monto_cobrado = monto_C['Importe pagado'].sum()
            monto_cobrar = monto_cobrar - monto_cobrado

            excel_cobros = {"nombre": "Montos por cobrar al " + fecha_pago , "hojas": [{"titulo":"Hoja 1", "dataframe": monto_C}]}
           
        with cnxn:
            query = """  SELECT [Estado de cuenta] + 7 as 'Fecha FUECD'
            ,fuf AS 'FUF',
    ABS([Monto + iva]) AS 'Monto'
	,pagos_admin.Fecha_vigencia as 'Fecha de Pago'
	,pagos_admin.Importe as 'Importe Pagado'
    FROM [dbo].[EDC_Pagos] as edc_pagos 
    left join [dbo].[Pagos_Admin] as pagos_admin
	on edc_pagos.FUF = pagos_admin.Folio
	where [Fecha de pago] =  '{fecha_pago}' and( 
	FUF like '%PC%' or FUF like '%C00%' or FUF like '%Cd%')
	order by [Estado de cuenta] asc, fuf asc 
                    """
            query = query.format(fecha_pago=fecha_pago)
            monto_p = pd.read_sql(query, cnxn)
            monto_pagar =monto_p['Monto'].sum()
            monto_pagado = monto_p['Importe Pagado'].sum()
            monto_pagar = monto_pagar - monto_pagado
          
           
            monto_pagar = "${:,.2f}".format(monto_pagar)  
            monto_cobrar = "${:,.2f}".format(monto_cobrar)  

        with cnxn:
            query = """
 select total.[Dia de operación],
 total.[ BTMNN],
 total.CENACE,
 tbfin.total_pagos as TBFIN,
 total.[ BTMNN] - total.CENACE + (ISNULL(tbfin.total_pagos,0) ) as 'Total Diario'

 from
 (Select pagos.EDC_pagos as 'Dia de operación'
, pagos.total_pagos as ' BTMNN', 
cobros.[total cobros] as 'CENACE'
,  pagos.total_pagos-cobros.[total cobros] as 'P&L'
from
(SELECT [Fecha de operación] as 'EDC_pagos'
      ,	sum( CASE WHEN fuf  like '%P00%' THEN  [Monto + iva]  WHEN fuf like '%PC%' THEN (-1)*[Monto + iva] WHEN fuf like '%PD%' THEN (-1)*[Monto + iva] ELSE 0 END)  as 'total_pagos'
  FROM [dbo].[EDC_Pagos] where  Subcuenta = 'b01'
  group by [Fecha de operación]  ) as pagos join 
  (SELECT [Fecha de operación] as 'EDC_cobros'
      ,	sum( CASE WHEN fuf  like '%c00%' THEN  [Monto + iva]  WHEN fuf like '%cC%' THEN (-1)*[Monto + iva] WHEN fuf like '%cD%' THEN (-1)*[Monto + iva] ELSE 0 END)  as 'total cobros'
  FROM [dbo].[EDC_Pagos] where  Subcuenta = 'b01'
  group by [Fecha de operación]  )as cobros 
  on pagos.EDC_pagos=cobros.EDC_cobros ) as total left join
 (
  SELECT [Fecha de operación] as 'EDC_pagos'
      ,	sum( CASE WHEN fuf  like '%P00%' THEN  [Monto + iva]  WHEN fuf like '%PC%' THEN (-1)*[Monto + iva] WHEN fuf like '%PD%' THEN (-1)*[Monto + iva] ELSE 0 END)  as 'total_pagos'
  FROM [dbo].[EDC_Pagos] where  Subcuenta = 'b02'
  group by [Fecha de operación]  ) as tbfin
  on tbfin.EDC_pagos = total.[Dia de operación]
  order by total.[Dia de operación] asc
                    """
            query = query.format(fecha_pago=fecha_pago)
            balance = pd.read_sql(query, cnxn)
            excel_balance = {"nombre": "Balance "  ,"hojas": [{"titulo":"Hoja 1", "dataframe": balance}]}
            
            PagosBTMNN = balance[' BTMNN'].sum()
            CobrosBTMNN = balance['CENACE'].sum()
            PLBTMNN = balance['Total Diario'].sum()  
            PLBTMNN = "${:,.2f}".format(PLBTMNN)

            excel_pagos = {"nombre": "Montos por pagar al " + fecha_pago ,"hojas": [{"titulo":"Hoja 1", "dataframe": monto_p}]}

            #return descargar_excel(excel_cobros,)
            excel_facturas_x = {"nombre": "Facturas por emitir","hojas": [{"titulo":"Hoja 1", "dataframe": df}]}

            excel_pagos15 = {"nombre": "Montos por pagar al " + fecha_pago2 ,"hojas": [{"titulo":"Hoja 1", "dataframe": monto_p15}]}

            
 
                
            global excel_pagos_g,excel_cobros_g,excel_facturas,excel_pagos_g15,excel_cobros_g15,excel_ba
            
            excel_ba = excel_balance
            excel_pagos_g = excel_pagos
            excel_cobros_g= excel_cobros
            excel_facturas = excel_facturas_x
            excel_pagos_g15 = excel_pagos15
            excel_cobros_g15= excel_cobros15



        return render_template("/dashboard/facturas.html",datos=df.to_html(classes=[ "records_list table table-striped table-bordered table-hover", "mydatatable"], justify='center', table_id='mydatatable'),
                             datoss=df.to_html(classes=[ "records_list table table-striped table-bordered table-hover"]),
                               monto_pagar=monto_pagar,
                                   monto_cobrar=monto_cobrar
                                   ,fecha_pago=fecha_pago,
                                      monto_pagar15=monto_pagar15,
                                   monto_cobrar15=monto_cobrar15
                                   ,fecha_pago2=fecha_pago2,
                                   P0=P0,C0=C0,PC=PC,PD=PD,CC=CC,CD=CD,
                                   P02=P02,C02=C02,PC2=PC2,PD2=PD2,CC2=CC2,CD2=CD2,
                                   PagosBTMNN=PagosBTMNN,
                                   CobrosBTMNN=CobrosBTMNN,
                                   PLBTMNN=PLBTMNN,
                                   descarga_F_cenace="display:none",
                                   descarga_F_participante="display:none",
                                   balance=balance.to_html(classes=["records_list table table-striped table-bordered table-hover", "mydatatable"], justify='center' , table_id='mydatatable2')
                            
                               )
    elif request.method == "POST":
        
        #fecha4 = request.form["fecha4"]
        fecha3 = request.form["fecha3"]
        fecha4 = fecha3
        fecha1 = request.form["fecha1"]
        #fecha2 = request.form["fecha2"]
        fecha2 = fecha1

        if request.form["boton_submit"] == "Facturas_Porpagar":
            fecha= datetime.today()
           

       
            dias_a_sumar = (2, 1, 0, 6, 5, 4, 3)
            fecha_pago = (fecha + timedelta(days=dias_a_sumar[fecha.weekday()]))
            fecha_pago2 = (fecha_pago + timedelta(7))
            fecha_pago = fecha_pago.strftime('%Y-%m-%d')

            fecha_pago2 = fecha_pago2.strftime('%Y-%m-%d')
            cnxn = crear_conexion_SQL()

            ###############################################33


            with cnxn:
                query = """
                 select [fecha de operacion] + 7 as 'Fecha FUECD', fuf  as 'FUF'
			    , monto as 'Monto',pagos.folioo as 'Folio Interno de Facturación'
    ,[Fecha Pago] as 'Fecha de Pago',
                Imp_Pagado as 'Importe pagado'
    from(
    SELECT 
      [Estado de cuenta] as 'fecha de operacion' ,edc_pagos.fuf as 'fuf',
      factura.Folio as 'folioo',
        ABS([Monto + iva]) AS 'monto'
        FROM [dbo].[EDC_Pagos] as edc_pagos join Facturas as factura
	    on edc_pagos.FUF = factura.FUF
	    where [Fecha de pago] = '{fecha_pago2}' and(  edc_pagos.FUF like '%P00%' or edc_pagos.FUF like '%Pd%')) as pagos 
	    left join Complemento_pagos as complemento
	    on pagos.folioo = complemento.Folio_Relacionado

    union

    select [fecha de operacion] + 7 , fuf, monto,pagos.folioo,[Fecha Pago],Imp_Pagado as 'importe pagado'

    from(
    SELECT 
      [Estado de cuenta] as 'fecha de operacion' ,edc_pagos.fuf as 'fuf',
      factura.Folio as 'folioo',
        ABS([Monto + iva]) AS 'monto'
        FROM [dbo].[EDC_Pagos] as edc_pagos left join CFDI_Emitidos_CENACE as factura
	    on edc_pagos.FUF = factura.FUF
	    where [Fecha de pago] = '{fecha_pago2}' and( 
	    edc_pagos.FUF like '%cC%' )) as pagos 
	    left join Complemento_pagos as complemento
	    on pagos.folioo = complemento.Folio_Relacionado

                        """
                query = query.format(fecha_pago2=fecha_pago2)
                monto_C15 = pd.read_sql(query, cnxn)
                monto_cobrar15 = monto_C15['Monto'].sum()
                monto_cobrado15 = monto_C15['Importe pagado'].sum()
                monto_cobrar15 = monto_cobrar15 - monto_cobrado15

                excel_cobros15 = {"nombre": "Montos por cobrar al " + fecha_pago2 , "hojas": [{"titulo":"Hoja 1", "dataframe": monto_C15}]}
           
            with cnxn:
                query = """  SELECT [Estado de cuenta] + 7 as 'Fecha FUECD'
                ,fuf AS 'FUF',
        ABS([Monto + iva]) AS 'Monto'
	    ,pagos_admin.Fecha_vigencia as 'Fecha de Pago'
	    ,pagos_admin.Importe as 'Importe pagado'
        FROM [dbo].[EDC_Pagos] as edc_pagos 
        left join [dbo].[Pagos_Admin] as pagos_admin
	    on edc_pagos.FUF = pagos_admin.Folio
	    where [Fecha de pago] =  '{fecha_pago2}' and( 
	    FUF like '%PC%' or FUF like '%C00%' or FUF like '%Cd%')
	    order by [Estado de cuenta] asc, fuf asc 
                        """
                query = query.format(fecha_pago2=fecha_pago2)
                monto_p15 = pd.read_sql(query, cnxn)
                monto_pagar15 =monto_p15['Monto'].sum()
                monto_pagado15 = monto_p15['Importe pagado'].sum()
                monto_pagar15 = monto_pagar15 - monto_pagado15
          
           
                monto_pagar15 = "${:,.2f}".format(monto_pagar15)  
                monto_cobrar15 = "${:,.2f}".format(monto_cobrar15) 




#########################3333

            with cnxn:
                query = """

            
    select  pagos.[Estado de cuenta] + 7  as 'Fecha FUECD'
    ,pagos.subcuenta as 'Subcuenta'
    ,pagos.fuf as 'FUF'
    ,pagos.uuid as 'UUID (Folio Fiscal)'
    ,pagos.folio as 'Folio Interno'
    ,tabla.folio_relacionado as 'Folio Relacionado'
    ,pagos.[Fecha de pago] as 'Fecha Limite de Pago'
    ,abs(pagos.[Monto + iva]) as 'Monto + Iva'
				
                
		    from (SELECT 
		    facturas.fuf as 'fufu',
		    facturas.Folio as 'folio_relacionado'
		    ,[Fecha de operación] as 'fecha'
   
		    FROM [dbo].[EDC_Pagos] as edc_pagos join Facturas as facturas 
		    on facturas.FUF = edc_pagos.FUF
		    where facturas.fuf like '%P00' )
    as tabla 
			
    right join (     
			 
			 
		    SELECT [Estado de cuenta]
		    ,[Subcuenta]
		    ,pagos.FUF
		    ,format(Facturas.Folio,'#') as 'folio'
		    ,facturas.UUID
		    ,[Fecha de pago]
		    ,[Fecha de operación]
		    ,[Monto]
		    ,[Monto + iva]
		    FROM [dbo].[EDC_Pagos] as pagos left join [dbo].[Facturas] as facturas
		    on pagos.FUF = facturas.FUF
		    where ([Fecha de pago] = '{fecha_pago}'
		    or [Fecha de pago] = '{fecha_pago2}')
		    and pagos.[FUF] like '%P%' ) 
      as pagos
    on tabla.fufu = pagos.FUF
    order by pagos.[Estado de cuenta] desc

                        """
                query = query.format(fecha_pago=fecha_pago,fecha_pago2=fecha_pago2)
                df = pd.read_sql(query, cnxn)

            with cnxn:
                query = """
                 SELECT 'C0',
            abs(sum([Monto + iva]))
            FROM [dbo].[EDC_Pagos] where ([Fecha de pago]  = '{fecha_pago}') and FUF like '%C0%'

                        """
                query = query.format(fecha_pago=fecha_pago,fecha_pago2=fecha_pago2)
                dfC0 = pd.read_sql(query, cnxn)
                C0=dfC0.iloc[0,1]

            with cnxn:
                query = """
                        SELECT 'P0',
                        abs(sum([Monto + iva]))
                        FROM [dbo].[EDC_Pagos] where ([Fecha de pago]  = '{fecha_pago}' ) 
                       and FUF like '%P0%'
                        """
                query = query.format(fecha_pago=fecha_pago,fecha_pago2=fecha_pago2)
                dfP0 = pd.read_sql(query, cnxn)

                P0=dfP0.iloc[0,1]

            with cnxn:
                query = """
                    
        SELECT 'PC',
        abs(sum([Monto + iva]))
        FROM [dbo].[EDC_Pagos] where ([Fecha de pago]  = '{fecha_pago}' ) and FUF like '%PC%'
                        """
                query = query.format(fecha_pago=fecha_pago,fecha_pago2=fecha_pago2)
                dfPC = pd.read_sql(query, cnxn)
                PC=dfPC.iloc[0,1]

            with cnxn:
                query = """
                       SELECT 'PD',
       abs(sum([Monto + iva]))
        FROM [dbo].[EDC_Pagos] where ([Fecha de pago]  = '{fecha_pago}')  and FUF like '%PD%'
                        """
                query = query.format(fecha_pago=fecha_pago,fecha_pago2=fecha_pago2)
                dfPD = pd.read_sql(query, cnxn)
                PD=dfPD.iloc[0,1]

            with cnxn:
                query = """
       SELECT 'CC',
        abs(sum([Monto + iva]))
        FROM [dbo].[EDC_Pagos] where ([Fecha de pago]  = '{fecha_pago}' )  and FUF like '%CC%'
                        """
                query = query.format(fecha_pago=fecha_pago,fecha_pago2=fecha_pago2)
                dfCC = pd.read_sql(query, cnxn)
                CC=dfCC.iloc[0,1]

            with cnxn:
                query = """
       SELECT 'CD',
        abs(sum([Monto + iva]))
        FROM [dbo].[EDC_Pagos] where ([Fecha de pago]  = '{fecha_pago}' 
            or [Fecha de pago] =  '{fecha_pago2}')  and FUF like '%CD%'
                        """
                query = query.format(fecha_pago=fecha_pago,fecha_pago2=fecha_pago2)
                dfCD = pd.read_sql(query, cnxn)
                CD=dfCD.iloc[0,1]

            

                PC = "${:,.2f}".format(PC)
                PD = "${:,.2f}".format(PD)
                P0 = "${:,.2f}".format(P0)

#por facturar 2

            with cnxn:
                query = """
                 SELECT 'C0',
            abs(sum([Monto + iva]))
            FROM [dbo].[EDC_Pagos] where ( [Fecha de pago] =  '{fecha_pago2}') and FUF like '%C0%'

                        """
                query = query.format(fecha_pago=fecha_pago,fecha_pago2=fecha_pago2)
                dfC0 = pd.read_sql(query, cnxn)
                C02=dfC0.iloc[0,1]

            with cnxn:
                query = """
                        SELECT 'P0',
                        abs(sum([Monto + iva]))
                        FROM [dbo].[EDC_Pagos] where ([Fecha de pago] =  '{fecha_pago2}') 
                       and FUF like '%P0%'
                        """
                query = query.format(fecha_pago=fecha_pago,fecha_pago2=fecha_pago2)
                dfP0 = pd.read_sql(query, cnxn)

                P02=dfP0.iloc[0,1]

            with cnxn:
                query = """
                    
        SELECT 'PC',
        abs(sum([Monto + iva]))
        FROM [dbo].[EDC_Pagos] where ([Fecha de pago] =  '{fecha_pago2}') and FUF like '%PC%'
                        """
                query = query.format(fecha_pago=fecha_pago,fecha_pago2=fecha_pago2)
                dfPC = pd.read_sql(query, cnxn)
                PC2=dfPC.iloc[0,1]
                print(dfPC)
           

            with cnxn:
                query = """
                       SELECT 'PD',
       abs(sum([Monto + iva]))
        FROM [dbo].[EDC_Pagos] where ( [Fecha de pago] =  '{fecha_pago2}')  and FUF like '%PD%'
                        """
                query = query.format(fecha_pago=fecha_pago,fecha_pago2=fecha_pago2)
                dfPD = pd.read_sql(query, cnxn)
                PD2=dfPD.iloc[0,1]

            with cnxn:
                query = """
       SELECT 'CC',
        abs(sum([Monto + iva]))
        FROM [dbo].[EDC_Pagos] where ( [Fecha de pago] =  '{fecha_pago2}')  and FUF like '%CC%'
                        """
                query = query.format(fecha_pago=fecha_pago,fecha_pago2=fecha_pago2)
                dfCC = pd.read_sql(query, cnxn)
                CC2=dfCC.iloc[0,1]

            with cnxn:
                query = """
       SELECT 'CD',
        abs(sum([Monto + iva]))
        FROM [dbo].[EDC_Pagos] where ([Fecha de pago] =  '{fecha_pago2}')  and FUF like '%CD%'
                        """
                query = query.format(fecha_pago=fecha_pago,fecha_pago2=fecha_pago2)
                dfCD = pd.read_sql(query, cnxn)
                CD2=dfCD.iloc[0,1]
                
            
                print(PD2)
                if PC2 is not None:
                   
                    PC2 = "${:,.2f}".format(PC2)

                if PD2 is not None:
                   
                    PD2 = "${:,.2f}".format(PD2)
                if P02 is not None:
                   
                    P02 = "${:,.2f}".format(P02)

            with cnxn:
                query = """ 
                 select [fecha de operacion] + 7 as 'Fecha FUECD', fuf  as 'FUF'
			    , monto,pagos.folioo as 'Folio Interno de Facturación'
    ,[Fecha Pago] as 'Fecha de Pago',
                Imp_Pagado as 'Importe pagado'
    from(
    SELECT 
      [Estado de cuenta] as 'fecha de operacion' ,edc_pagos.fuf as 'fuf',
      factura.Folio as 'folioo',
        ABS([Monto + iva]) AS 'monto'
        FROM [dbo].[EDC_Pagos] as edc_pagos left join Facturas as factura
	    on edc_pagos.FUF = factura.FUF
	    where [Fecha de pago] = '{fecha_pago}' and(  edc_pagos.FUF like '%P00%' or edc_pagos.FUF like '%Pd%')) as pagos 
	    left join Complemento_pagos as complemento
	    on pagos.folioo = complemento.Folio_Relacionado

    union

    select [fecha de operacion], fuf, monto,pagos.folioo,[Fecha Pago],Imp_Pagado as 'importe pagado'

    from(
    SELECT 
      [Estado de cuenta] + 7 as 'fecha de operacion' ,edc_pagos.fuf as 'fuf',
      factura.Folio as 'folioo',
        ABS([Monto + iva]) AS 'monto'
        FROM [dbo].[EDC_Pagos] as edc_pagos left join CFDI_Emitidos_CENACE as factura
	    on edc_pagos.FUF = factura.FUF
	    where [Fecha de pago] = '{fecha_pago}' and( 
	    edc_pagos.FUF like '%cC%' )) as pagos 
	    left join Complemento_pagos as complemento
	    on pagos.folioo = complemento.Folio_Relacionado

                        """
                query = query.format(fecha_pago=fecha_pago)
                monto_C = pd.read_sql(query, cnxn)
                monto_cobrar = monto_C['monto'].sum()
                monto_cobrado = monto_C['Importe pagado'].sum()
                monto_cobrar = monto_cobrar - monto_cobrado

                excel_cobros = {"nombre": "Montos por cobrar al " + fecha_pago , "hojas": [{"titulo":"Hoja 1", "dataframe": monto_C}]}
           
            with cnxn:
                query = """  SELECT [Estado de cuenta] + 7 as 'Fecha FUECD'
                ,fuf AS 'FUF',
        ABS([Monto + iva]) AS 'Monto'
	    ,pagos_admin.Fecha_vigencia as 'Fecha de Pago'
	    ,pagos_admin.Importe as 'Importe Pagado'
        FROM [dbo].[EDC_Pagos] as edc_pagos 
        left join [dbo].[Pagos_Admin] as pagos_admin
	    on edc_pagos.FUF = pagos_admin.Folio
	    where [Fecha de pago] =  '{fecha_pago}' and( 
	    FUF like '%PC%' or FUF like '%C00%' or FUF like '%Cd%')
	    order by [Estado de cuenta] asc, fuf asc 
                        """
                query = query.format(fecha_pago=fecha_pago)
                monto_p = pd.read_sql(query, cnxn)
                monto_pagar =monto_p['Monto'].sum()
                monto_pagado = monto_p['Importe Pagado'].sum()
                monto_pagar = monto_pagar - monto_pagado
          
           
                monto_pagar = "${:,.2f}".format(monto_pagar)  
                monto_cobrar = "${:,.2f}".format(monto_cobrar)  

            with cnxn:
                query = """
 select total.[Dia de operación],
 total.[ BTMNN],
 total.CENACE,
 tbfin.total_pagos as TBFIN,
 total.[ BTMNN] - total.CENACE + (ISNULL(tbfin.total_pagos,0) ) as 'Total Diario'

 from
 (Select pagos.EDC_pagos as 'Dia de operación'
, pagos.total_pagos as ' BTMNN', 
cobros.[total cobros] as 'CENACE'
,  pagos.total_pagos-cobros.[total cobros] as 'P&L'
from
(SELECT [Fecha de operación] as 'EDC_pagos'
      ,	sum( CASE WHEN fuf  like '%P00%' THEN  [Monto + iva]  WHEN fuf like '%PC%' THEN (-1)*[Monto + iva] WHEN fuf like '%PD%' THEN (-1)*[Monto + iva] ELSE 0 END)  as 'total_pagos'
  FROM [dbo].[EDC_Pagos] where  Subcuenta = 'b01'
  group by [Fecha de operación]  ) as pagos join 
  (SELECT [Fecha de operación] as 'EDC_cobros'
      ,	sum( CASE WHEN fuf  like '%c00%' THEN  [Monto + iva]  WHEN fuf like '%cC%' THEN (-1)*[Monto + iva] WHEN fuf like '%cD%' THEN (-1)*[Monto + iva] ELSE 0 END)  as 'total cobros'
  FROM [dbo].[EDC_Pagos] where  Subcuenta = 'b01'
  group by [Fecha de operación]  )as cobros 
  on pagos.EDC_pagos=cobros.EDC_cobros ) as total left join
 (
  SELECT [Fecha de operación] as 'EDC_pagos'
      ,	sum( CASE WHEN fuf  like '%P00%' THEN  [Monto + iva]  WHEN fuf like '%PC%' THEN (-1)*[Monto + iva] WHEN fuf like '%PD%' THEN (-1)*[Monto + iva] ELSE 0 END)  as 'total_pagos'
  FROM [dbo].[EDC_Pagos] where  Subcuenta = 'b02'
  group by [Fecha de operación]  ) as tbfin
  on tbfin.EDC_pagos = total.[Dia de operación]
  order by total.[Dia de operación] asc
                        """
                query = query.format(fecha_pago=fecha_pago)
                balance = pd.read_sql(query, cnxn)
                excel_balance = {"nombre": "Balance "  ,"hojas": [{"titulo":"Hoja 1", "dataframe": balance}]}
            
                PagosBTMNN = balance[' BTMNN'].sum()
                CobrosBTMNN = balance['CENACE'].sum()
                PLBTMNN = balance['Total Diario'].sum()  
                PLBTMNN = "${:,.2f}".format(PLBTMNN)

                excel_pagos = {"nombre": "Montos por pagar al " + fecha_pago ,"hojas": [{"titulo":"Hoja 1", "dataframe": monto_p}]}

                #return descargar_excel(excel_cobros,)
                excel_facturas_x = {"nombre": "Facturas por emitir","hojas": [{"titulo":"Hoja 1", "dataframe": df}]}

                excel_pagos15 = {"nombre": "Montos por pagar al " + fecha_pago2 ,"hojas": [{"titulo":"Hoja 1", "dataframe": monto_p15}]}

            
 
                
                
                excel_ba = excel_balance
                excel_pagos_g = excel_pagos
                excel_cobros_g= excel_cobros
                excel_facturas = excel_facturas_x
                excel_pagos_g15 = excel_pagos15
                excel_cobros_g15= excel_cobros15

                

            cnxn = crear_conexion_SQL()
            with cnxn:
                query = """
Select 
edc + 7 as 'Fecha de Operacion',
[fecha de pago] as 'Fecha de Pago',
subcuenta as 'Subcuenta',
fuf as 'FUF',
uuid as 'UUID Factura',
todo.folio as 'Folio Interno',
folio_relacionado as 'Folio Relacionado',
abs(monto) as 'Monto',
Fecha_vigencia as 'Fecha de Pago FOP',
Importe as 'Importe Pagado',
todo.pago_complemento as 'Fecha de Pago', 
todo.uuid2 as 'UUID Complemento de Pago',
[complemento folio] as 'Folio del Complemento de Pago',
todo.[importe pagado] as 'Monto en el Complemento de Pago'

from(  
	   select facturas_particpante.edc as 'edc',
	   facturas_particpante.[Fecha de pago] as 'fecha de pago',
  facturas_particpante.subcuenta as 'subcuenta',
  facturas_particpante.fuf as 'fuf',
  facturas_particpante.uuid as'uuid',
  facturas_particpante.folio as 'folio',
  facturas_particpante.folio_relacionado as 'folio_relacionado',
  facturas_particpante.[Monto + iva] as'monto',
  complemento.folio as 'complemento folio',
  complemento.Folio_Relacionado as 'folio relacionado',
  complemento.Imp_Pagado as 'importe pagado',
  complemento.UUID as 'uuid2',
  complemento.[Fecha Pago] as 'pago_complemento'
  
  
  from 
	   (select pagos.edc
                ,pagos.subcuenta
                ,pagos.fuf
                ,pagos.uuid
				,pagos.folio
				,tabla.folio_relacionado
				,pagos.[Monto + iva],
				pagos.[Fecha de pago]
                
    from (SELECT 
	subcuenta as 'subcuenta',
                facturas.fuf as 'fufu',
				facturas.Folio as 'folio_relacionado'
				,[Fecha de operación] as 'fecha'
   
            FROM [dbo].[EDC_Pagos] as edc_pagos join [CFDI_Emitidos_CENACE] as facturas 
            on facturas.FUF = edc_pagos.FUF
			where facturas.fuf like '%c00' )as tabla 
			
			join (SELECT 
                [Monto + iva]
	            ,facturas.Folio as 'folio'
			,[Estado de cuenta] as 'edc'
                ,[Subcuenta] as 'subcuenta'
                ,[edc_pagos].fuf as 'fuf'
                ,[UUID] as 'uuid'
                
                ,[Fecha de operación] as 'operacion'
   
	            ,facturas.Fecha as 'Fecha de factura',
				edc_pagos.[Fecha de pago] as 'Fecha de pago'
	            
            FROM [dbo].[EDC_Pagos] as edc_pagos join [CFDI_Emitidos_CENACE] as facturas 
            on facturas.FUF = edc_pagos.FUF
			where facturas.fuf like '%cd%' or facturas.fuf like '%c00'
			) as pagos
			on tabla.fecha = pagos.operacion
            where pagos.[Fecha de pago] >= '{fecha3}' and pagos.[Fecha de pago] <= '{fecha4}' )
			as facturas_particpante
			left join [Complemento_pagos] as complemento
	on facturas_particpante.uuid = complemento.UUID_relacionado
	)as todo left join [dbo].[Pagos_Admin] as pagos_admin
	on todo.fuf = pagos_admin.Folio

	union 

	   Select 
edc + 7 as 'Fecha de Operacion',
[fecha de pago] as 'Fecha de Pago',
subcuenta as 'Subcuenta',
fuf as 'FUF',
uuid as 'UUID Factura',
todo.folio as 'Folio Interno',
folio_relacionado as 'Folio Relacionado',
abs(monto) as 'Monto',
Fecha_vigencia as 'Fecha de Pago FOP',
Importe as 'Importe Pagado',
todo.pago_complemento as 'Fecha de Pago',
todo.uuid2 as 'UUID Complemento de Pago',
[complemento folio] as 'Folio del Complemento de Pago',
todo.[importe pagado] as 'Monto en el Complemento de Pago'




from(  
	   select facturas_particpante.edc as 'edc',
	   facturas_particpante.[Fecha de pago] as 'fecha de pago',
  facturas_particpante.subcuenta as 'subcuenta',
  facturas_particpante.fuf as 'fuf',
  facturas_particpante.uuid as'uuid',
  facturas_particpante.folio as 'folio',
  facturas_particpante.folio_relacionado as 'folio_relacionado',
  facturas_particpante.[Monto + iva] as'monto',
  complemento.folio as 'complemento folio',
  complemento.Folio_Relacionado as 'folio relacionado',
  complemento.Imp_Pagado as 'importe pagado',
  complemento.UUID as 'uuid2',
   complemento.[Fecha Pago] as 'pago_complemento'
  
  
  from 
	   (select pagos.edc
                ,pagos.subcuenta
                ,pagos.fuf
                ,pagos.uuid
				,pagos.folio
				,tabla.folio_relacionado
				,pagos.[Monto + iva],
				pagos.[Fecha de pago]
                
    from (SELECT 
                facturas.fuf as 'fufu',
				facturas.Folio as 'folio_relacionado'
				,[Fecha de operación] as 'fecha'
   
            FROM [dbo].[EDC_Pagos] as edc_pagos join [Facturas] as facturas 
            on facturas.FUF = edc_pagos.FUF
			where facturas.fuf like '%P00' )as tabla 
			
			join (SELECT 
                [Monto + iva]
	            ,facturas.Folio as 'folio'
			,[Estado de cuenta] as 'edc'
                ,[Subcuenta] as 'subcuenta'
                ,[edc_pagos].fuf as 'fuf'
                ,[UUID] as 'uuid'
                
                ,[Fecha de operación] as 'operacion'
   
	            ,facturas.Fecha as 'Fecha de factura',
				edc_pagos.[Fecha de pago] as 'Fecha de pago'
	            
            FROM [dbo].[EDC_Pagos] as edc_pagos join Facturas as facturas 
            on facturas.FUF = edc_pagos.FUF
			where facturas.fuf like '%Pc%' ) as pagos
			on tabla.fecha = pagos.operacion
            where pagos.[Fecha de pago] >= '{fecha3}' and pagos.[Fecha de pago] <= '{fecha4}')
			as facturas_particpante
			left join [Complemento_pagos] as complemento
	on facturas_particpante.uuid = complemento.UUID_relacionado
	)as todo left join [dbo].[Pagos_Admin] as pagos_admin
	on todo.fuf = pagos_admin.Folio
                      
                        """
                query = query.format(fecha3=fecha3,fecha4=fecha4)
                df2 = pd.read_sql(query, cnxn)
                monto_total = df2['Monto'].sum()
                monto_total="${:,.2f}".format(monto_total) 
                excel_facturas_cenace_g = {"nombre": "Facturas por pagar " + fecha1 + " al " + fecha2 ,"hojas": [{"titulo":"Hoja 1", "dataframe": df2}]}

                global excel_facturas_cenace 

                excel_facturas_cenace = excel_facturas_cenace_g

            


            return render_template("/dashboard/facturas.html",datos3=df2.to_html(classes=["records_list table table-striped table-bordered table-hover", "mydatatable"], justify='center' , table_id='mydatatable')
                                      ,datos=df.to_html(classes=[ "records_list table table-striped table-bordered table-hover", "mydatatable"], justify='center', table_id='mydatatable2'),
                                      monto_pagar=monto_pagar,
                                       monto_cobrar=monto_cobrar
                                       ,fecha_pago=fecha_pago,
                                       P0=P0,C0=C0,PC=PC,PD=PD,CC=CC,CD=CD,
                                       P02=P02,C02=C02,PC2=PC2,PD2=PD2,CC2=CC2,CD2=CD2,
                                       PagosBTMNN=PagosBTMNN,
                                       CobrosBTMNN=CobrosBTMNN,
                                       PLBTMNN=PLBTMNN,
                                       monto_pagar15=monto_pagar15,
                                   monto_cobrar15=monto_cobrar15
                                   ,fecha_pago2=fecha_pago2,
                                   monto_total=monto_total,
                                       show2="show",
                                       descarga_F_participante="display:none",
                                       descarga_F_cenace="display:block",
                                        montoto="display:block",
                                       balance=balance.to_html(classes=[ "records_list table table-striped table-bordered table-hover", "mydatatable"], justify='center', table_id='mydatatable3'))

        if request.form["boton_submit"] == "Facturas_Porpagar2":
            fecha3cobrar = request.form["fecha3cobrar"]
            fecha4cobrar = request.form["fecha4cobrar"]
            fecha3 = fecha3cobrar
            fecha4 = fecha4cobrar
            fecha= datetime.today()
           

       
            dias_a_sumar = (2, 1, 0, 6, 5, 4, 3)
            fecha_pago = (fecha + timedelta(days=dias_a_sumar[fecha.weekday()]))
            fecha_pago2 = (fecha_pago + timedelta(7))
            fecha_pago = fecha_pago.strftime('%Y-%m-%d')

            fecha_pago2 = fecha_pago2.strftime('%Y-%m-%d')
            cnxn = crear_conexion_SQL()

            ###############################################33


            with cnxn:
                query = """
                 select [fecha de operacion] + 7 as 'Fecha FUECD', fuf  as 'FUF'
			    , monto as 'Monto',pagos.folioo as 'Folio Interno de Facturación'
    ,[Fecha Pago] as 'Fecha de Pago',
                Imp_Pagado as 'Importe pagado'
    from(
    SELECT 
      [Estado de cuenta] as 'fecha de operacion' ,edc_pagos.fuf as 'fuf',
      factura.Folio as 'folioo',
        ABS([Monto + iva]) AS 'monto'
        FROM [dbo].[EDC_Pagos] as edc_pagos join Facturas as factura
	    on edc_pagos.FUF = factura.FUF
	    where [Fecha de pago] = '{fecha_pago2}' and(  edc_pagos.FUF like '%P00%' or edc_pagos.FUF like '%Pd%')) as pagos 
	    left join Complemento_pagos as complemento
	    on pagos.folioo = complemento.Folio_Relacionado

    union

    select [fecha de operacion] + 7 , fuf, monto,pagos.folioo,[Fecha Pago],Imp_Pagado as 'importe pagado'

    from(
    SELECT 
      [Estado de cuenta] as 'fecha de operacion' ,edc_pagos.fuf as 'fuf',
      factura.Folio as 'folioo',
        ABS([Monto + iva]) AS 'monto'
        FROM [dbo].[EDC_Pagos] as edc_pagos left join CFDI_Emitidos_CENACE as factura
	    on edc_pagos.FUF = factura.FUF
	    where [Fecha de pago] = '{fecha_pago2}' and( 
	    edc_pagos.FUF like '%cC%' )) as pagos 
	    left join Complemento_pagos as complemento
	    on pagos.folioo = complemento.Folio_Relacionado

                        """
                query = query.format(fecha_pago2=fecha_pago2)
                monto_C15 = pd.read_sql(query, cnxn)
                monto_cobrar15 = monto_C15['Monto'].sum()
                monto_cobrado15 = monto_C15['Importe pagado'].sum()
                monto_cobrar15 = monto_cobrar15 - monto_cobrado15

                excel_cobros15 = {"nombre": "Montos por cobrar al " + fecha_pago2 , "hojas": [{"titulo":"Hoja 1", "dataframe": monto_C15}]}
           
            with cnxn:
                query = """  SELECT [Estado de cuenta] + 7 as 'Fecha FUECD'
                ,fuf AS 'FUF',
        ABS([Monto + iva]) AS 'Monto'
	    ,pagos_admin.Fecha_vigencia as 'Fecha de Pago'
	    ,pagos_admin.Importe as 'Importe pagado'
        FROM [dbo].[EDC_Pagos] as edc_pagos 
        left join [dbo].[Pagos_Admin] as pagos_admin
	    on edc_pagos.FUF = pagos_admin.Folio
	    where [Fecha de pago] =  '{fecha_pago2}' and( 
	    FUF like '%PC%' or FUF like '%C00%' or FUF like '%Cd%')
	    order by [Estado de cuenta] asc, fuf asc 
                        """
                query = query.format(fecha_pago2=fecha_pago2)
                monto_p15 = pd.read_sql(query, cnxn)
                monto_pagar15 =monto_p15['Monto'].sum()
                monto_pagado15 = monto_p15['Importe pagado'].sum()
                monto_pagar15 = monto_pagar15 - monto_pagado15
          
           
                monto_pagar15 = "${:,.2f}".format(monto_pagar15)  
                monto_cobrar15 = "${:,.2f}".format(monto_cobrar15) 




#########################3333

            with cnxn:
                query = """

            
    select  pagos.[Estado de cuenta] + 7  as 'Fecha FUECD'
    ,pagos.subcuenta as 'Subcuenta'
    ,pagos.fuf as 'FUF'
    ,pagos.uuid as 'UUID (Folio Fiscal)'
    ,pagos.folio as 'Folio Interno'
    ,tabla.folio_relacionado as 'Folio Relacionado'
    ,pagos.[Fecha de pago] as 'Fecha Limite de Pago'
    ,abs(pagos.[Monto + iva]) as 'Monto + Iva'
				
                
		    from (SELECT 
		    facturas.fuf as 'fufu',
		    facturas.Folio as 'folio_relacionado'
		    ,[Fecha de operación] as 'fecha'
   
		    FROM [dbo].[EDC_Pagos] as edc_pagos join Facturas as facturas 
		    on facturas.FUF = edc_pagos.FUF
		    where facturas.fuf like '%P00' )
    as tabla 
			
    right join (     
			 
			 
		    SELECT [Estado de cuenta]
		    ,[Subcuenta]
		    ,pagos.FUF
		    ,format(Facturas.Folio,'#') as 'folio'
		    ,facturas.UUID
		    ,[Fecha de pago]
		    ,[Fecha de operación]
		    ,[Monto]
		    ,[Monto + iva]
		    FROM [dbo].[EDC_Pagos] as pagos left join [dbo].[Facturas] as facturas
		    on pagos.FUF = facturas.FUF
		    where ([Fecha de pago] = '{fecha_pago}'
		    or [Fecha de pago] = '{fecha_pago2}')
		    and pagos.[FUF] like '%P%' ) 
      as pagos
    on tabla.fufu = pagos.FUF
    order by pagos.[Estado de cuenta] desc

                        """
                query = query.format(fecha_pago=fecha_pago,fecha_pago2=fecha_pago2)
                df = pd.read_sql(query, cnxn)

            with cnxn:
                query = """
                 SELECT 'C0',
            abs(sum([Monto + iva]))
            FROM [dbo].[EDC_Pagos] where ([Fecha de pago]  = '{fecha_pago}') and FUF like '%C0%'

                        """
                query = query.format(fecha_pago=fecha_pago,fecha_pago2=fecha_pago2)
                dfC0 = pd.read_sql(query, cnxn)
                C0=dfC0.iloc[0,1]

            with cnxn:
                query = """
                        SELECT 'P0',
                        abs(sum([Monto + iva]))
                        FROM [dbo].[EDC_Pagos] where ([Fecha de pago]  = '{fecha_pago}' ) 
                       and FUF like '%P0%'
                        """
                query = query.format(fecha_pago=fecha_pago,fecha_pago2=fecha_pago2)
                dfP0 = pd.read_sql(query, cnxn)

                P0=dfP0.iloc[0,1]

            with cnxn:
                query = """
                    
        SELECT 'PC',
        abs(sum([Monto + iva]))
        FROM [dbo].[EDC_Pagos] where ([Fecha de pago]  = '{fecha_pago}' ) and FUF like '%PC%'
                        """
                query = query.format(fecha_pago=fecha_pago,fecha_pago2=fecha_pago2)
                dfPC = pd.read_sql(query, cnxn)
                PC=dfPC.iloc[0,1]

            with cnxn:
                query = """
                       SELECT 'PD',
       abs(sum([Monto + iva]))
        FROM [dbo].[EDC_Pagos] where ([Fecha de pago]  = '{fecha_pago}')  and FUF like '%PD%'
                        """
                query = query.format(fecha_pago=fecha_pago,fecha_pago2=fecha_pago2)
                dfPD = pd.read_sql(query, cnxn)
                PD=dfPD.iloc[0,1]

            with cnxn:
                query = """
       SELECT 'CC',
        abs(sum([Monto + iva]))
        FROM [dbo].[EDC_Pagos] where ([Fecha de pago]  = '{fecha_pago}' )  and FUF like '%CC%'
                        """
                query = query.format(fecha_pago=fecha_pago,fecha_pago2=fecha_pago2)
                dfCC = pd.read_sql(query, cnxn)
                CC=dfCC.iloc[0,1]

            with cnxn:
                query = """
       SELECT 'CD',
        abs(sum([Monto + iva]))
        FROM [dbo].[EDC_Pagos] where ([Fecha de pago]  = '{fecha_pago}' 
            or [Fecha de pago] =  '{fecha_pago2}')  and FUF like '%CD%'
                        """
                query = query.format(fecha_pago=fecha_pago,fecha_pago2=fecha_pago2)
                dfCD = pd.read_sql(query, cnxn)
                CD=dfCD.iloc[0,1]

            

                PC = "${:,.2f}".format(PC)
                PD = "${:,.2f}".format(PD)
                P0 = "${:,.2f}".format(P0)

#por facturar 2

            with cnxn:
                query = """
                 SELECT 'C0',
            abs(sum([Monto + iva]))
            FROM [dbo].[EDC_Pagos] where ( [Fecha de pago] =  '{fecha_pago2}') and FUF like '%C0%'

                        """
                query = query.format(fecha_pago=fecha_pago,fecha_pago2=fecha_pago2)
                dfC0 = pd.read_sql(query, cnxn)
                C02=dfC0.iloc[0,1]

            with cnxn:
                query = """
                        SELECT 'P0',
                        abs(sum([Monto + iva]))
                        FROM [dbo].[EDC_Pagos] where ([Fecha de pago] =  '{fecha_pago2}') 
                       and FUF like '%P0%'
                        """
                query = query.format(fecha_pago=fecha_pago,fecha_pago2=fecha_pago2)
                dfP0 = pd.read_sql(query, cnxn)

                P02=dfP0.iloc[0,1]

            with cnxn:
                query = """
                    
        SELECT 'PC',
        abs(sum([Monto + iva]))
        FROM [dbo].[EDC_Pagos] where ([Fecha de pago] =  '{fecha_pago2}') and FUF like '%PC%'
                        """
                query = query.format(fecha_pago=fecha_pago,fecha_pago2=fecha_pago2)
                dfPC = pd.read_sql(query, cnxn)
                PC2=dfPC.iloc[0,1]
                print(dfPC)
           

            with cnxn:
                query = """
                       SELECT 'PD',
       abs(sum([Monto + iva]))
        FROM [dbo].[EDC_Pagos] where ( [Fecha de pago] =  '{fecha_pago2}')  and FUF like '%PD%'
                        """
                query = query.format(fecha_pago=fecha_pago,fecha_pago2=fecha_pago2)
                dfPD = pd.read_sql(query, cnxn)
                PD2=dfPD.iloc[0,1]

            with cnxn:
                query = """
       SELECT 'CC',
        abs(sum([Monto + iva]))
        FROM [dbo].[EDC_Pagos] where ( [Fecha de pago] =  '{fecha_pago2}')  and FUF like '%CC%'
                        """
                query = query.format(fecha_pago=fecha_pago,fecha_pago2=fecha_pago2)
                dfCC = pd.read_sql(query, cnxn)
                CC2=dfCC.iloc[0,1]

            with cnxn:
                query = """
       SELECT 'CD',
        abs(sum([Monto + iva]))
        FROM [dbo].[EDC_Pagos] where ([Fecha de pago] =  '{fecha_pago2}')  and FUF like '%CD%'
                        """
                query = query.format(fecha_pago=fecha_pago,fecha_pago2=fecha_pago2)
                dfCD = pd.read_sql(query, cnxn)
                CD2=dfCD.iloc[0,1]
                
            
                print(PD2)
                if PC2 is not None:
                   
                    PC2 = "${:,.2f}".format(PC2)

                if PD2 is not None:
                   
                    PD2 = "${:,.2f}".format(PD2)
                if P02 is not None:
                   
                    P02 = "${:,.2f}".format(P02)

            with cnxn:
                query = """ 
                 select [fecha de operacion] + 7 as 'Fecha FUECD', fuf  as 'FUF'
			    , monto,pagos.folioo as 'Folio Interno de Facturación'
    ,[Fecha Pago] as 'Fecha de Pago',
                Imp_Pagado as 'Importe pagado'
    from(
    SELECT 
      [Estado de cuenta] as 'fecha de operacion' ,edc_pagos.fuf as 'fuf',
      factura.Folio as 'folioo',
        ABS([Monto + iva]) AS 'monto'
        FROM [dbo].[EDC_Pagos] as edc_pagos left join Facturas as factura
	    on edc_pagos.FUF = factura.FUF
	    where [Fecha de pago] = '{fecha_pago}' and(  edc_pagos.FUF like '%P00%' or edc_pagos.FUF like '%Pd%')) as pagos 
	    left join Complemento_pagos as complemento
	    on pagos.folioo = complemento.Folio_Relacionado

    union

    select [fecha de operacion], fuf, monto,pagos.folioo,[Fecha Pago],Imp_Pagado as 'importe pagado'

    from(
    SELECT 
      [Estado de cuenta] + 7 as 'fecha de operacion' ,edc_pagos.fuf as 'fuf',
      factura.Folio as 'folioo',
        ABS([Monto + iva]) AS 'monto'
        FROM [dbo].[EDC_Pagos] as edc_pagos left join CFDI_Emitidos_CENACE as factura
	    on edc_pagos.FUF = factura.FUF
	    where [Fecha de pago] = '{fecha_pago}' and( 
	    edc_pagos.FUF like '%cC%' )) as pagos 
	    left join Complemento_pagos as complemento
	    on pagos.folioo = complemento.Folio_Relacionado

                        """
                query = query.format(fecha_pago=fecha_pago)
                monto_C = pd.read_sql(query, cnxn)
                monto_cobrar = monto_C['monto'].sum()
                monto_cobrado = monto_C['Importe pagado'].sum()
                monto_cobrar = monto_cobrar - monto_cobrado

                excel_cobros = {"nombre": "Montos por cobrar al " + fecha_pago , "hojas": [{"titulo":"Hoja 1", "dataframe": monto_C}]}
           
            with cnxn:
                query = """  SELECT [Estado de cuenta] + 7 as 'Fecha FUECD'
                ,fuf AS 'FUF',
        ABS([Monto + iva]) AS 'Monto'
	    ,pagos_admin.Fecha_vigencia as 'Fecha de Pago'
	    ,pagos_admin.Importe as 'Importe Pagado'
        FROM [dbo].[EDC_Pagos] as edc_pagos 
        left join [dbo].[Pagos_Admin] as pagos_admin
	    on edc_pagos.FUF = pagos_admin.Folio
	    where [Fecha de pago] =  '{fecha_pago}' and( 
	    FUF like '%PC%' or FUF like '%C00%' or FUF like '%Cd%')
	    order by [Estado de cuenta] asc, fuf asc 
                        """
                query = query.format(fecha_pago=fecha_pago)
                monto_p = pd.read_sql(query, cnxn)
                monto_pagar =monto_p['Monto'].sum()
                monto_pagado = monto_p['Importe Pagado'].sum()
                monto_pagar = monto_pagar - monto_pagado
          
           
                monto_pagar = "${:,.2f}".format(monto_pagar)  
                monto_cobrar = "${:,.2f}".format(monto_cobrar)  

            with cnxn:
                query = """
 select total.[Dia de operación],
 total.[ BTMNN],
 total.CENACE,
 tbfin.total_pagos as TBFIN,
 total.[ BTMNN] - total.CENACE + (ISNULL(tbfin.total_pagos,0) ) as 'Total Diario'

 from
 (Select pagos.EDC_pagos as 'Dia de operación'
, pagos.total_pagos as ' BTMNN', 
cobros.[total cobros] as 'CENACE'
,  pagos.total_pagos-cobros.[total cobros] as 'P&L'
from
(SELECT [Fecha de operación] as 'EDC_pagos'
      ,	sum( CASE WHEN fuf  like '%P00%' THEN  [Monto + iva]  WHEN fuf like '%PC%' THEN (-1)*[Monto + iva] WHEN fuf like '%PD%' THEN (-1)*[Monto + iva] ELSE 0 END)  as 'total_pagos'
  FROM [dbo].[EDC_Pagos] where  Subcuenta = 'b01'
  group by [Fecha de operación]  ) as pagos join 
  (SELECT [Fecha de operación] as 'EDC_cobros'
      ,	sum( CASE WHEN fuf  like '%c00%' THEN  [Monto + iva]  WHEN fuf like '%cC%' THEN (-1)*[Monto + iva] WHEN fuf like '%cD%' THEN (-1)*[Monto + iva] ELSE 0 END)  as 'total cobros'
  FROM [dbo].[EDC_Pagos] where  Subcuenta = 'b01'
  group by [Fecha de operación]  )as cobros 
  on pagos.EDC_pagos=cobros.EDC_cobros ) as total left join
 (
  SELECT [Fecha de operación] as 'EDC_pagos'
      ,	sum( CASE WHEN fuf  like '%P00%' THEN  [Monto + iva]  WHEN fuf like '%PC%' THEN (-1)*[Monto + iva] WHEN fuf like '%PD%' THEN (-1)*[Monto + iva] ELSE 0 END)  as 'total_pagos'
  FROM [dbo].[EDC_Pagos] where  Subcuenta = 'b02'
  group by [Fecha de operación]  ) as tbfin
  on tbfin.EDC_pagos = total.[Dia de operación]
  order by total.[Dia de operación] asc
                        """
                query = query.format(fecha_pago=fecha_pago)
                balance = pd.read_sql(query, cnxn)
                excel_balance = {"nombre": "Balance "  ,"hojas": [{"titulo":"Hoja 1", "dataframe": balance}]}
            
                PagosBTMNN = balance[' BTMNN'].sum()
                CobrosBTMNN = balance['CENACE'].sum()
                PLBTMNN = balance['Total Diario'].sum()  
                PLBTMNN = "${:,.2f}".format(PLBTMNN)

                excel_pagos = {"nombre": "Montos por pagar al " + fecha_pago ,"hojas": [{"titulo":"Hoja 1", "dataframe": monto_p}]}

                #return descargar_excel(excel_cobros,)
                excel_facturas_x = {"nombre": "Facturas por emitir","hojas": [{"titulo":"Hoja 1", "dataframe": df}]}

                excel_pagos15 = {"nombre": "Montos por pagar al " + fecha_pago2 ,"hojas": [{"titulo":"Hoja 1", "dataframe": monto_p15}]}

            
 
                
                
                excel_ba = excel_balance
                excel_pagos_g = excel_pagos
                excel_cobros_g= excel_cobros
                excel_facturas = excel_facturas_x
                excel_pagos_g15 = excel_pagos15
                excel_cobros_g15= excel_cobros15

                

            cnxn = crear_conexion_SQL()
            with cnxn:
                query = """
           Select 
edc + 7 as 'Fecha de Operacion',
subcuenta as 'Subcuenta',
fuf as 'FUF',
todo.[Fecha de factura],
uuid as 'UUID Factura',
todo.folio as 'Folio Interno',
folio_relacionado as 'Folio Relacionado',
abs(monto) as 'Monto',
Fecha_vigencia as 'Fecha de Pago FOP',
Importe as 'Importe Pagado',
todo.pago_complemento as 'Fecha de Pago', 
todo.uuid2 as 'UUID Complemento de Pago',
[complemento folio] as 'Folio del Complemento de Pago',
todo.[importe pagado] as 'Monto en el Complemento de Pago'

from(  
	   select facturas_particpante.edc as 'edc',
	   facturas_particpante.[Fecha de pago] as 'fecha de pago',
	   facturas_particpante.[Fecha de factura],
  facturas_particpante.subcuenta as 'subcuenta',
  facturas_particpante.fuf as 'fuf',
  facturas_particpante.uuid as'uuid',
  facturas_particpante.folio as 'folio',
  facturas_particpante.folio_relacionado as 'folio_relacionado',
  facturas_particpante.[Monto + iva] as'monto',
  complemento.folio as 'complemento folio',
  complemento.Folio_Relacionado as 'folio relacionado',
  complemento.Imp_Pagado as 'importe pagado',
  complemento.UUID as 'uuid2',
  complemento.[Fecha Pago] as 'pago_complemento'
  
  
  from 
	   (select pagos.edc
                ,pagos.subcuenta
                ,pagos.fuf
                ,pagos.uuid
				,pagos.folio
				,tabla.folio_relacionado
				,pagos.[Monto + iva],
				pagos.[Fecha de pago]
				,pagos.[Fecha de factura]
                
    from (SELECT 
	subcuenta as 'subcuenta',
                facturas.fuf as 'fufu',
				facturas.Folio as 'folio_relacionado'
				,[Fecha de operación] as 'fecha'
   
            FROM [dbo].[EDC_Pagos] as edc_pagos join [CFDI_Emitidos_CENACE] as facturas 
            on facturas.FUF = edc_pagos.FUF
			where facturas.fuf like '%c00' )as tabla 
			
			right join (SELECT 
                [Monto + iva]
	            ,facturas.Folio as 'folio'
			,[Estado de cuenta] as 'edc'
                ,[Subcuenta] as 'subcuenta'
                ,[edc_pagos].fuf as 'fuf'
                ,[UUID] as 'uuid'
                
                ,[Fecha de operación] as 'operacion'
   
	            ,facturas.Fecha as 'Fecha de factura',
				edc_pagos.[Fecha de pago] as 'Fecha de pago'
	            
            FROM [dbo].[EDC_Pagos] as edc_pagos join [CFDI_Emitidos_CENACE] as facturas 
            on facturas.FUF = edc_pagos.FUF
			where facturas.fuf like '%cd%' or facturas.fuf like '%c00'
			) as pagos
			on tabla.fecha = pagos.operacion and tabla.subcuenta = pagos.subcuenta
            where pagos.[Fecha de factura] >= '{fecha3}' and pagos.[Fecha de factura]  - 1  <= '{fecha4}' )
			as facturas_particpante
			left join [Complemento_pagos] as complemento
	on facturas_particpante.uuid = complemento.UUID_relacionado
	)as todo left join [dbo].[Pagos_Admin] as pagos_admin
	on todo.fuf = pagos_admin.Folio

	union 

	   Select 
edc + 7 as 'Fecha de Operacion',
subcuenta as 'Subcuenta',
fuf as 'FUF',
todo.[Fecha de factura],
uuid as 'UUID Factura',
todo.folio as 'Folio Interno',
folio_relacionado as 'Folio Relacionado',
abs(monto) as 'Monto',
Fecha_vigencia as 'Fecha de Pago FOP',
Importe as 'Importe Pagado',
todo.pago_complemento as 'Fecha de Pago',
todo.uuid2 as 'UUID Complemento de Pago',
[complemento folio] as 'Folio del Complemento de Pago',
todo.[importe pagado] as 'Monto en el Complemento de Pago'




from(  
	   select facturas_particpante.edc as 'edc',
	   facturas_particpante.[Fecha de pago] as 'fecha de pago',
  facturas_particpante.subcuenta as 'subcuenta',
  facturas_particpante.fuf as 'fuf',
  facturas_particpante.uuid as'uuid',
  facturas_particpante.folio as 'folio',
  facturas_particpante.folio_relacionado as 'folio_relacionado',
  facturas_particpante.[Monto + iva] as'monto',
  complemento.folio as 'complemento folio',
  complemento.Folio_Relacionado as 'folio relacionado',
  complemento.Imp_Pagado as 'importe pagado',
  complemento.UUID as 'uuid2',
   complemento.[Fecha Pago] as 'pago_complemento',
    facturas_particpante.[Fecha de factura]
  
  
  from 
	   (select pagos.edc
                ,pagos.subcuenta
                ,pagos.fuf
                ,pagos.uuid
				,pagos.folio
				,tabla.folio_relacionado
				,pagos.[Monto + iva],
				pagos.[Fecha de pago]
				,pagos.[Fecha de factura]
                
    from (SELECT 
	edc_pagos.Subcuenta as 'subcuenta',
                facturas.fuf as 'fufu',
				facturas.Folio as 'folio_relacionado'
				,[Fecha de operación] as 'fecha'
   
            FROM [dbo].[EDC_Pagos] as edc_pagos join [Facturas] as facturas 
            on facturas.FUF = edc_pagos.FUF 
			where facturas.fuf like '%P00' )as tabla 
			
			right join (SELECT 
                [Monto + iva]
	            ,facturas.Folio as 'folio'
			,[Estado de cuenta] as 'edc'
                ,[Subcuenta] as 'subcuenta'
                ,[edc_pagos].fuf as 'fuf'
                ,[UUID] as 'uuid'
                
                ,[Fecha de operación] as 'operacion'
   
	            ,facturas.Fecha as 'Fecha de factura',
				edc_pagos.[Fecha de pago] as 'Fecha de pago'
	            
            FROM [dbo].[EDC_Pagos] as edc_pagos join Facturas as facturas 
            on facturas.FUF = edc_pagos.FUF
			where facturas.fuf like '%Pc%' ) as pagos
			on tabla.fecha = pagos.operacion and tabla.subcuenta = pagos.subcuenta
            where pagos.[Fecha de factura] >= '{fecha3}' and pagos.[Fecha de factura]  - 1  <= '{fecha4}')
			as facturas_particpante
			left join [Complemento_pagos] as complemento
	on facturas_particpante.uuid = complemento.UUID_relacionado
	)as todo left join [dbo].[Pagos_Admin] as pagos_admin
	on todo.fuf = pagos_admin.Folio
	order by todo.[Fecha de factura] asc
                      
                        """
                query = query.format(fecha3=fecha3,fecha4=fecha4)
                df2 = pd.read_sql(query, cnxn)
                monto_total = df2['Monto'].sum()
                monto_total="${:,.2f}".format(monto_total) 
                excel_facturas_cenace_g = {"nombre": "Facturas por pagar " + fecha1 + " al " + fecha2 ,"hojas": [{"titulo":"Hoja 1", "dataframe": df2}]}

                

                excel_facturas_cenace = excel_facturas_cenace_g

            


            return render_template("/dashboard/facturas.html",datos3=df2.to_html(classes=["records_list table table-striped table-bordered table-hover", "mydatatable"], justify='center' , table_id='mydatatable')
                                      ,datos=df.to_html(classes=[ "records_list table table-striped table-bordered table-hover", "mydatatable"], justify='center', table_id='mydatatable2'),
                                      monto_pagar=monto_pagar,
                                       monto_cobrar=monto_cobrar
                                       ,fecha_pago=fecha_pago,
                                       P0=P0,C0=C0,PC=PC,PD=PD,CC=CC,CD=CD,
                                       P02=P02,C02=C02,PC2=PC2,PD2=PD2,CC2=CC2,CD2=CD2,
                                       PagosBTMNN=PagosBTMNN,
                                       CobrosBTMNN=CobrosBTMNN,
                                       PLBTMNN=PLBTMNN,
                                       monto_pagar15=monto_pagar15,
                                   monto_cobrar15=monto_cobrar15
                                   ,fecha_pago2=fecha_pago2,
                                   monto_total=monto_total,
                                    montoto2="display:none",
                                       show2="show",
                                       descarga_F_participante="display:none",
                                       descarga_F_cenace="display:block",
                                       balance=balance.to_html(classes=[ "records_list table table-striped table-bordered table-hover", "mydatatable"], justify='center', table_id='mydatatable3'))

        elif request.form["boton_submit"] == "Facturas_Porcobrar":

            fecha= datetime.today()
       
            dias_a_sumar = (2, 1, 0, 6, 5, 4, 3)
            fecha_pago = (fecha + timedelta(days=dias_a_sumar[fecha.weekday()]))
            fecha_pago2 = (fecha_pago + timedelta(7))
            fecha_pago = fecha_pago.strftime('%Y-%m-%d')
        
            fecha_pago2 = fecha_pago2.strftime('%Y-%m-%d')
           
            cnxn = crear_conexion_SQL()

            ###############################################33


            with cnxn:
                query = """
                 select [fecha de operacion] + 7 as 'Fecha FUECD', fuf  as 'FUF'
			    , monto as 'Monto',pagos.folioo as 'Folio Interno de Facturación'
    ,[Fecha Pago] as 'Fecha de Pago',
                Imp_Pagado as 'Importe pagado'
    from(
    SELECT 
      [Estado de cuenta] as 'fecha de operacion' ,edc_pagos.fuf as 'fuf',
      factura.Folio as 'folioo',
        ABS([Monto + iva]) AS 'monto'
        FROM [dbo].[EDC_Pagos] as edc_pagos join Facturas as factura
	    on edc_pagos.FUF = factura.FUF
	    where [Fecha de pago] = '{fecha_pago2}' and( 
	    edc_pagos.FUF like '%cC%' or edc_pagos.FUF like '%P00%' or edc_pagos.FUF like '%Pd%')) as pagos 
	    left join Complemento_pagos as complemento
	    on pagos.folioo = complemento.Folio_Relacionado

    union

    select [fecha de operacion] + 7 , fuf, monto,pagos.folioo,[Fecha Pago],Imp_Pagado as 'importe pagado'

    from(
    SELECT 
      [Estado de cuenta] as 'fecha de operacion' ,edc_pagos.fuf as 'fuf',
      factura.Folio as 'folioo',
        ABS([Monto + iva]) AS 'monto'
        FROM [dbo].[EDC_Pagos] as edc_pagos left join CFDI_Emitidos_CENACE as factura
	    on edc_pagos.FUF = factura.FUF
	    where [Fecha de pago] = '{fecha_pago2}' and( 
	    edc_pagos.FUF like '%cC%' )) as pagos 
	    left join Complemento_pagos as complemento
	    on pagos.folioo = complemento.Folio_Relacionado

                        """
                query = query.format(fecha_pago2=fecha_pago2)
                monto_C15 = pd.read_sql(query, cnxn)
                monto_cobrar15 = monto_C15['Monto'].sum()
                monto_cobrado15 = monto_C15['Importe pagado'].sum()
                monto_cobrar15 = monto_cobrar15 - monto_cobrado15

                excel_cobros15 = {"nombre": "Montos por cobrar al " + fecha_pago2 , "hojas": [{"titulo":"Hoja 1", "dataframe": monto_C15}]}
           
            with cnxn:
                query = """  SELECT [Estado de cuenta] + 7 as 'Fecha FUECD'
                ,fuf AS 'FUF',
        ABS([Monto + iva]) AS 'Monto'
	    ,pagos_admin.Fecha_vigencia as 'Fecha de Pago'
	    ,pagos_admin.Importe as 'Importe pagado'
        FROM [dbo].[EDC_Pagos] as edc_pagos 
        left join [dbo].[Pagos_Admin] as pagos_admin
	    on edc_pagos.FUF = pagos_admin.Folio
	    where [Fecha de pago] =  '{fecha_pago2}' and( 
	    FUF like '%PC%' or FUF like '%C00%' or FUF like '%Cd%')
	    order by [Estado de cuenta] asc, fuf asc 
                        """
                query = query.format(fecha_pago2=fecha_pago2)
                monto_p15 = pd.read_sql(query, cnxn)
                monto_pagar15 =monto_p15['Monto'].sum()
                monto_pagado15 = monto_p15['Importe pagado'].sum()
                monto_pagar15 = monto_pagar15 - monto_pagado15
          
           
                monto_pagar15 = "${:,.2f}".format(monto_pagar15)  
                monto_cobrar15 = "${:,.2f}".format(monto_cobrar15) 




#########################3333
            with cnxn:
                query = """

            
    select  pagos.[Estado de cuenta] + 7  as 'Fecha FUECD'
    ,pagos.subcuenta as 'Subcuenta'
    ,pagos.fuf as 'FUF'
    ,pagos.uuid as 'UUID (Folio Fiscal)'
    ,pagos.folio as 'Folio Interno'
    ,tabla.folio_relacionado as 'Folio Relacionado'
    ,pagos.[Fecha de pago] as 'Fecha Limite de Pago'
    ,abs(pagos.[Monto + iva]) as 'Monto + Iva'
				
                
		    from (SELECT 
		    facturas.fuf as 'fufu',
		    facturas.Folio as 'folio_relacionado'
		    ,[Fecha de operación] as 'fecha'
   
		    FROM [dbo].[EDC_Pagos] as edc_pagos join Facturas as facturas 
		    on facturas.FUF = edc_pagos.FUF
		    where facturas.fuf like '%P00' )
    as tabla 
			
    right join (     
			 
			 
		    SELECT [Estado de cuenta]
		    ,[Subcuenta]
		    ,pagos.FUF
		    ,format(Facturas.Folio,'#') as 'folio'
		    ,facturas.UUID
		    ,[Fecha de pago]
		    ,[Fecha de operación]
		    ,[Monto]
		    ,[Monto + iva]
		    FROM [dbo].[EDC_Pagos] as pagos left join [dbo].[Facturas] as facturas
		    on pagos.FUF = facturas.FUF
		    where ([Fecha de pago] = '{fecha_pago}'
		    or [Fecha de pago] = '{fecha_pago2}')
		    and pagos.[FUF] like '%P%' ) 
      as pagos
    on tabla.fufu = pagos.FUF
    order by pagos.[Estado de cuenta] desc

                        """
                query = query.format(fecha_pago=fecha_pago,fecha_pago2=fecha_pago2)
                df = pd.read_sql(query, cnxn)


            with cnxn:
                query = """
                 SELECT 'C0',
            abs(sum([Monto + iva]))
            FROM [dbo].[EDC_Pagos] where ([Fecha de pago]  = '{fecha_pago}') and FUF like '%C0%'

                        """
                query = query.format(fecha_pago=fecha_pago,fecha_pago2=fecha_pago2)
                dfC0 = pd.read_sql(query, cnxn)
                C0=dfC0.iloc[0,1]

            with cnxn:
                query = """
                        SELECT 'P0',
                        abs(sum([Monto + iva]))
                        FROM [dbo].[EDC_Pagos] where ([Fecha de pago]  = '{fecha_pago}' ) 
                       and FUF like '%P0%'
                        """
                query = query.format(fecha_pago=fecha_pago,fecha_pago2=fecha_pago2)
                dfP0 = pd.read_sql(query, cnxn)

                P0=dfP0.iloc[0,1]

            with cnxn:
                query = """
                    
        SELECT 'PC',
        abs(sum([Monto + iva]))
        FROM [dbo].[EDC_Pagos] where ([Fecha de pago]  = '{fecha_pago}' ) and FUF like '%PC%'
                        """
                query = query.format(fecha_pago=fecha_pago,fecha_pago2=fecha_pago2)
                dfPC = pd.read_sql(query, cnxn)
                PC=dfPC.iloc[0,1]

            with cnxn:
                query = """
                       SELECT 'PD',
       abs(sum([Monto + iva]))
        FROM [dbo].[EDC_Pagos] where ([Fecha de pago]  = '{fecha_pago}')  and FUF like '%PD%'
                        """
                query = query.format(fecha_pago=fecha_pago,fecha_pago2=fecha_pago2)
                dfPD = pd.read_sql(query, cnxn)
                PD=dfPD.iloc[0,1]

            with cnxn:
                query = """
       SELECT 'CC',
        abs(sum([Monto + iva]))
        FROM [dbo].[EDC_Pagos] where ([Fecha de pago]  = '{fecha_pago}' )  and FUF like '%CC%'
                        """
                query = query.format(fecha_pago=fecha_pago,fecha_pago2=fecha_pago2)
                dfCC = pd.read_sql(query, cnxn)
                CC=dfCC.iloc[0,1]

            with cnxn:
                query = """
       SELECT 'CD',
        abs(sum([Monto + iva]))
        FROM [dbo].[EDC_Pagos] where ([Fecha de pago]  = '{fecha_pago}' 
            or [Fecha de pago] =  '{fecha_pago2}')  and FUF like '%CD%'
                        """
                query = query.format(fecha_pago=fecha_pago,fecha_pago2=fecha_pago2)
                dfCD = pd.read_sql(query, cnxn)
                CD=dfCD.iloc[0,1]

            

                PC = "${:,.2f}".format(PC)
                PD = "${:,.2f}".format(PD)
                P0 = "${:,.2f}".format(P0)

    #por facturar 2

            with cnxn:
                query = """
                    SELECT 'C0',
            abs(sum([Monto + iva]))
            FROM [dbo].[EDC_Pagos] where ( [Fecha de pago] =  '{fecha_pago2}') and FUF like '%C0%'

                        """
                query = query.format(fecha_pago=fecha_pago,fecha_pago2=fecha_pago2)
                dfC0 = pd.read_sql(query, cnxn)
                C02=dfC0.iloc[0,1]

            with cnxn:
                query = """
                        SELECT 'P0',
                        abs(sum([Monto + iva]))
                        FROM [dbo].[EDC_Pagos] where ([Fecha de pago] =  '{fecha_pago2}') 
                        and FUF like '%P0%'
                        """
                query = query.format(fecha_pago=fecha_pago,fecha_pago2=fecha_pago2)
                dfP0 = pd.read_sql(query, cnxn)

                P02=dfP0.iloc[0,1]

            with cnxn:
                query = """
                    
        SELECT 'PC',
        abs(sum([Monto + iva]))
        FROM [dbo].[EDC_Pagos] where ([Fecha de pago] =  '{fecha_pago2}') and FUF like '%PC%'
                        """
                query = query.format(fecha_pago=fecha_pago,fecha_pago2=fecha_pago2)
                dfPC = pd.read_sql(query, cnxn)
                PC2=dfPC.iloc[0,1]
                print(dfPC)
           

            with cnxn:
                query = """
                        SELECT 'PD',
        abs(sum([Monto + iva]))
        FROM [dbo].[EDC_Pagos] where ( [Fecha de pago] =  '{fecha_pago2}')  and FUF like '%PD%'
                        """
                query = query.format(fecha_pago=fecha_pago,fecha_pago2=fecha_pago2)
                dfPD = pd.read_sql(query, cnxn)
                PD2=dfPD.iloc[0,1]

            with cnxn:
                query = """
        SELECT 'CC',
        abs(sum([Monto + iva]))
        FROM [dbo].[EDC_Pagos] where ( [Fecha de pago] =  '{fecha_pago2}')  and FUF like '%CC%'
                        """
                query = query.format(fecha_pago=fecha_pago,fecha_pago2=fecha_pago2)
                dfCC = pd.read_sql(query, cnxn)
                CC2=dfCC.iloc[0,1]

            with cnxn:
                query = """
        SELECT 'CD',
        abs(sum([Monto + iva]))
        FROM [dbo].[EDC_Pagos] where ([Fecha de pago] =  '{fecha_pago2}')  and FUF like '%CD%'
                        """
                query = query.format(fecha_pago=fecha_pago,fecha_pago2=fecha_pago2)
                dfCD = pd.read_sql(query, cnxn)
                CD2=dfCD.iloc[0,1]
                
            
                print(PD2)
                if PC2 is not None:
                   
                    PC2 = "${:,.2f}".format(PC2)

                if PD2 is not None:
                   
                    PD2 = "${:,.2f}".format(PD2)
                if P02 is not None:
                   
                    P02 = "${:,.2f}".format(P02)

            with cnxn:
                query = """ 
                 select [fecha de operacion] + 7 as 'Fecha FUECD', fuf  as 'FUF'
			    , monto,pagos.folioo as 'Folio Interno de Facturación'
    ,[Fecha Pago] as 'Fecha de Pago',
                Imp_Pagado as 'Importe pagado'
    from(
    SELECT 
      [Estado de cuenta] as 'fecha de operacion' ,edc_pagos.fuf as 'fuf',
      factura.Folio as 'folioo',
        ABS([Monto + iva]) AS 'monto'
        FROM [dbo].[EDC_Pagos] as edc_pagos left join Facturas as factura
	    on edc_pagos.FUF = factura.FUF
	    where [Fecha de pago] = '{fecha_pago}' and(  edc_pagos.FUF like '%P00%' or edc_pagos.FUF like '%Pd%')) as pagos 
	    left join Complemento_pagos as complemento
	    on pagos.folioo = complemento.Folio_Relacionado

    union

    select [fecha de operacion], fuf, monto,pagos.folioo,[Fecha Pago],Imp_Pagado as 'importe pagado'

    from(
    SELECT 
      [Estado de cuenta] + 7 as 'fecha de operacion' ,edc_pagos.fuf as 'fuf',
      factura.Folio as 'folioo',
        ABS([Monto + iva]) AS 'monto'
        FROM [dbo].[EDC_Pagos] as edc_pagos left join CFDI_Emitidos_CENACE as factura
	    on edc_pagos.FUF = factura.FUF
	    where [Fecha de pago] = '{fecha_pago}' and( 
	    edc_pagos.FUF like '%cC%' )) as pagos 
	    left join Complemento_pagos as complemento
	    on pagos.folioo = complemento.Folio_Relacionado

                        """
                query = query.format(fecha_pago=fecha_pago)
                monto_C = pd.read_sql(query, cnxn)
                monto_cobrar = monto_C['monto'].sum()
                monto_cobrado = monto_C['Importe pagado'].sum()
                monto_cobrar = monto_cobrar - monto_cobrado

                excel_cobros = {"nombre": "Montos por cobrar al " + fecha_pago , "hojas": [{"titulo":"Hoja 1", "dataframe": monto_C}]}
           
            with cnxn:
                query = """  SELECT [Estado de cuenta] + 7 as 'Fecha FUECD'
                ,fuf AS 'FUF',
        ABS([Monto + iva]) AS 'Monto'
	    ,pagos_admin.Fecha_vigencia as 'Fecha de Pago'
	    ,pagos_admin.Importe as 'Importe Pagado'
        FROM [dbo].[EDC_Pagos] as edc_pagos 
        left join [dbo].[Pagos_Admin] as pagos_admin
	    on edc_pagos.FUF = pagos_admin.Folio
	    where [Fecha de pago] =  '{fecha_pago}' and( 
	    FUF like '%PC%' or FUF like '%C00%' or FUF like '%Cd%')
	    order by [Estado de cuenta] asc, fuf asc 
                        """
                query = query.format(fecha_pago=fecha_pago)
                monto_p = pd.read_sql(query, cnxn)
                monto_pagar =monto_p['Monto'].sum()
                monto_pagado = monto_p['Importe Pagado'].sum()
                monto_pagar = monto_pagar - monto_pagado
          
           
                monto_pagar = "${:,.2f}".format(monto_pagar)  
                monto_cobrar = "${:,.2f}".format(monto_cobrar)  

            with cnxn:
                query = """
 select total.[Dia de operación],
 total.[ BTMNN],
 total.CENACE,
 tbfin.total_pagos as TBFIN,
 total.[ BTMNN] - total.CENACE + (ISNULL(tbfin.total_pagos,0) ) as 'Total Diario'

 from
 (Select pagos.EDC_pagos as 'Dia de operación'
, pagos.total_pagos as ' BTMNN', 
cobros.[total cobros] as 'CENACE'
,  pagos.total_pagos-cobros.[total cobros] as 'P&L'
from
(SELECT [Fecha de operación] as 'EDC_pagos'
      ,	sum( CASE WHEN fuf  like '%P00%' THEN  [Monto + iva]  WHEN fuf like '%PC%' THEN (-1)*[Monto + iva] WHEN fuf like '%PD%' THEN (-1)*[Monto + iva] ELSE 0 END)  as 'total_pagos'
  FROM [dbo].[EDC_Pagos] where  Subcuenta = 'b01'
  group by [Fecha de operación]  ) as pagos join 
  (SELECT [Fecha de operación] as 'EDC_cobros'
      ,	sum( CASE WHEN fuf  like '%c00%' THEN  [Monto + iva]  WHEN fuf like '%cC%' THEN (-1)*[Monto + iva] WHEN fuf like '%cD%' THEN (-1)*[Monto + iva] ELSE 0 END)  as 'total cobros'
  FROM [dbo].[EDC_Pagos] where  Subcuenta = 'b01'
  group by [Fecha de operación]  )as cobros 
  on pagos.EDC_pagos=cobros.EDC_cobros ) as total left join
 (
  SELECT [Fecha de operación] as 'EDC_pagos'
      ,	sum( CASE WHEN fuf  like '%P00%' THEN  [Monto + iva]  WHEN fuf like '%PC%' THEN (-1)*[Monto + iva] WHEN fuf like '%PD%' THEN (-1)*[Monto + iva] ELSE 0 END)  as 'total_pagos'
  FROM [dbo].[EDC_Pagos] where  Subcuenta = 'b02'
  group by [Fecha de operación]  ) as tbfin
  on tbfin.EDC_pagos = total.[Dia de operación]
  order by total.[Dia de operación] asc
                        """
                query = query.format(fecha_pago=fecha_pago)
                balance = pd.read_sql(query, cnxn)
                excel_balance = {"nombre": "Balance "  ,"hojas": [{"titulo":"Hoja 1", "dataframe": balance}]}
            
                PagosBTMNN = balance[' BTMNN'].sum()
                CobrosBTMNN = balance['CENACE'].sum()
                PLBTMNN = balance['Total Diario'].sum()  
                PLBTMNN = "${:,.2f}".format(PLBTMNN)

                excel_pagos = {"nombre": "Montos por pagar al " + fecha_pago ,"hojas": [{"titulo":"Hoja 1", "dataframe": monto_p}]}

                #return descargar_excel(excel_cobros,)
                excel_facturas_x = {"nombre": "Facturas por emitir","hojas": [{"titulo":"Hoja 1", "dataframe": df}]}

                excel_pagos15 = {"nombre": "Montos por pagar al " + fecha_pago2 ,"hojas": [{"titulo":"Hoja 1", "dataframe": monto_p15}]}

            
 
                
                
                excel_ba = excel_balance
                excel_pagos_g = excel_pagos
                excel_cobros_g= excel_cobros
                excel_facturas = excel_facturas_x
                excel_pagos_g15 = excel_pagos15
                excel_cobros_g15= excel_cobros15

            cnxn = crear_conexion_SQL()
            with cnxn:
                query = """
Select 
edc + 7 as 'Fecha FUECD',
[fecha de pago] as 'Fecha de Pago',
subcuenta as 'Subcuenta',
fuf as 'FUF',
uuid as 'UUID Factura',
todo.folio as 'Folio Interno',
folio_relacionado as 'Folio Relacionado',
abs(monto) as 'Monto',
todo.pago_complemento as 'Fecha de Pago', 
todo.uuid2 as 'UUID Complemento de Pago',
[complemento folio] as 'Folio del Complemento de Pago',
todo.[importe pagado] as 'Monto en el Complemento de Pago'

from(  
	   select facturas_particpante.edc as 'edc',
	   facturas_particpante.[Fecha de pago] as 'fecha de pago',
  facturas_particpante.subcuenta as 'subcuenta',
  facturas_particpante.fuf as 'fuf',
  facturas_particpante.uuid as'uuid',
  facturas_particpante.folio as 'folio',
  facturas_particpante.folio_relacionado as 'folio_relacionado',
  facturas_particpante.[Monto + iva] as'monto',
  complemento.folio as 'complemento folio',
  complemento.Folio_Relacionado as 'folio relacionado',
  complemento.Imp_Pagado as 'importe pagado',
  complemento.UUID as 'uuid2',
  complemento.[Fecha Pago] as 'pago_complemento'
  
  
  from 
	   (select pagos.edc
                ,pagos.subcuenta
                ,pagos.fuf
                ,pagos.uuid
				,pagos.folio
				,tabla.folio_relacionado
				,pagos.[Monto + iva],
				pagos.[Fecha de pago]
                
    from (SELECT 
	subcuenta as 'subcuenta',
                facturas.fuf as 'fufu',
				facturas.Folio as 'folio_relacionado'
				,[Fecha de operación] as 'fecha'
				
   
            FROM [dbo].[EDC_Pagos] as edc_pagos left join [CFDI_Emitidos_CENACE] as facturas 
            on facturas.FUF = edc_pagos.FUF
			where edc_pagos.fuf like '%c00' )as tabla 
			
			right join (SELECT 
                [Monto + iva]
	            ,facturas.Folio as 'folio'
			,[Estado de cuenta] as 'edc'
                ,[Subcuenta] as 'subcuenta'
                ,[edc_pagos].fuf as 'fuf'
                ,[UUID] as 'uuid'
                
                ,[Fecha de operación] as 'operacion'
   
	            ,facturas.Fecha as 'Fecha de factura',
				edc_pagos.[Fecha de pago] as 'Fecha de pago'

	            
            FROM [dbo].[EDC_Pagos] as edc_pagos left join [CFDI_Emitidos_CENACE] as facturas 
            on facturas.FUF = edc_pagos.FUF
			where edc_pagos.fuf like '%cc%'
			) as pagos
			on tabla.fecha = pagos.operacion
           where pagos.[Fecha de pago] >= '{fecha1}' and pagos.[Fecha de pago] <= '{fecha2}')
			as facturas_particpante
			left join [Complemento_pagos] as complemento
	on facturas_particpante.folio = complemento.[Folio_Relacionado]
	)as todo left join [dbo].[Pagos_Admin] as pagos_admin
	on todo.fuf = pagos_admin.Folio

	union 

	   Select 
edc + 7 as 'Fecha FUECD',
[fecha de pago] as 'Fecha de Pago',
subcuenta as 'Subcuenta',
fuf as 'FUF',
uuid as 'UUID Factura',
todo.folio as 'Folio Interno',
folio_relacionado as 'Folio Relacionado',
abs(monto) as 'Monto',
todo.pago_complemento as 'Fecha de Pago',
todo.uuid2 as 'UUID Complemento de Pago',
[complemento folio] as 'Folio del Complemento de Pago',
todo.[importe pagado] as 'Monto en el Complemento de Pago'




from(  
	   select facturas_particpante.edc as 'edc',
	   facturas_particpante.[Fecha de pago] as 'fecha de pago',
  facturas_particpante.subcuenta as 'subcuenta',
  facturas_particpante.fuf as 'fuf',
  facturas_particpante.uuid as'uuid',
  facturas_particpante.folio as 'folio',
  facturas_particpante.folio_relacionado as 'folio_relacionado',
  facturas_particpante.[Monto + iva] as'monto',
  complemento.folio as 'complemento folio',
  complemento.Folio_Relacionado as 'folio relacionado',
  complemento.Imp_Pagado as 'importe pagado',
  complemento.UUID as 'uuid2',
   complemento.[Fecha Pago] as 'pago_complemento'
  
  
  from 
	   (select pagos.edc
                ,pagos.subcuenta
                ,pagos.fuf
                ,pagos.uuid
				,pagos.folio
				,tabla.folio_relacionado
				,pagos.[Monto + iva],
				pagos.[Fecha de pago]
                
    from (SELECT 
                facturas.fuf as 'fufu',
				facturas.Folio as 'folio_relacionado'
				,[Fecha de operación] as 'fecha'
				,facturas.Participante
   
            FROM [dbo].[EDC_Pagos] as edc_pagos left join [Facturas] as facturas 
            on facturas.FUF = edc_pagos.FUF
			where edc_pagos.fuf like '%P00' )as tabla 
			
			right join (SELECT 
                [Monto + iva]
	            ,facturas.Folio as 'folio'
			,[Estado de cuenta] as 'edc'
                ,[Subcuenta] as 'subcuenta'
                ,[edc_pagos].fuf as 'fuf'
                ,[UUID] as 'uuid'
                
                ,[Fecha de operación] as 'operacion'
   
	            ,facturas.Fecha as 'Fecha de factura',
				edc_pagos.[Fecha de pago] as 'Fecha de pago'
				,facturas.Participante
	            
            FROM [dbo].[EDC_Pagos] as edc_pagos left join Facturas as facturas 
            on facturas.FUF = edc_pagos.FUF
			where edc_pagos.fuf like '%PD%' or edc_pagos.fuf like '%P00' ) as pagos
			on tabla.fecha = pagos.operacion and tabla.Participante = pagos.Participante
            where pagos.[Fecha de pago] >= '{fecha1}' and pagos.[Fecha de pago] <= '{fecha2}'   )
			as facturas_particpante
			left join [Complemento_pagos] as complemento
	on facturas_particpante.folio = complemento.[Folio_Relacionado]
	)as todo left join [dbo].[Pagos_Admin] as pagos_admin
	on todo.fuf = pagos_admin.Folio
                      
               
            """
                query = query.format(fecha1=fecha1,fecha2=fecha2)
                df2 = pd.read_sql(query, cnxn)
                monto_total = df2['Monto'].sum()
                monto_total="${:,.2f}".format(monto_total) 
                excel_facturas_participante_g = {"nombre": "Facturas por cobrar " + fecha1 + " al " + fecha2 ,"hojas": [{"titulo":"Hoja 1", "dataframe": df2}]}

                global excel_facturas_participante 

                excel_facturas_participante = excel_facturas_participante_g

            


            return render_template("/dashboard/facturas.html",datos=df.to_html(classes=[ "records_list table table-striped table-bordered table-hover", "mydatatable"], justify='center', table_id='mydatatable'),
                             monto_pagar=monto_pagar,
                                   monto_cobrar=monto_cobrar
                                   ,fecha_pago=fecha_pago,
                                   P0=P0,C0=C0,PC=PC,PD=PD,CC=CC,CD=CD,
                                   P02=P02,C02=C02,PC2=PC2,PD2=PD2,CC2=CC2,CD2=CD2,
                                   PagosBTMNN=PagosBTMNN,
                                   CobrosBTMNN=CobrosBTMNN,
                                   PLBTMNN=PLBTMNN,
                                   show1='show',
                                   monto_pagar15=monto_pagar15,
                                   monto_cobrar15=monto_cobrar15
                                   ,fecha_pago2=fecha_pago2,
                                   monto_total=monto_total,
                                   descarga_F_participante="display:block",
                                   descarga_F_cenace="display:none",
                                   montoto="display:block",
                                   balance=balance.to_html(classes=[ "records_list table table-striped table-bordered table-hover", "mydatatable"], justify='center', table_id='mydatatable2'),
                                   datos2=df2.to_html(classes=[ "records_list table table-striped table-bordered table-hover", "mydatatable"], justify='center', table_id='mydatatable3'))

        elif request.form["boton_submit"] == "Facturas_Porcobrar2":
            
            fecha1cobrar = request.form["fecha1cobrar"]
            fecha2cobrar = request.form["fecha2cobrar"]
            fecha1 = fecha1cobrar
            fecha2 = fecha2cobrar
            fecha= datetime.today()
       
            dias_a_sumar = (2, 1, 0, 6, 5, 4, 3)
            fecha_pago = (fecha + timedelta(days=dias_a_sumar[fecha.weekday()]))
            fecha_pago2 = (fecha_pago + timedelta(7))
            fecha_pago = fecha_pago.strftime('%Y-%m-%d')
        
            fecha_pago2 = fecha_pago2.strftime('%Y-%m-%d')
           
            cnxn = crear_conexion_SQL()

            ###############################################33


            with cnxn:
                query = """
                 select [fecha de operacion] + 7 as 'Fecha FUECD', fuf  as 'FUF'
			    , monto as 'Monto',pagos.folioo as 'Folio Interno de Facturación'
    ,[Fecha Pago] as 'Fecha de Pago',
                Imp_Pagado as 'Importe pagado'
    from(
    SELECT 
      [Estado de cuenta] as 'fecha de operacion' ,edc_pagos.fuf as 'fuf',
      factura.Folio as 'folioo',
        ABS([Monto + iva]) AS 'monto'
        FROM [dbo].[EDC_Pagos] as edc_pagos join Facturas as factura
	    on edc_pagos.FUF = factura.FUF
	    where [Fecha de pago] = '{fecha_pago2}' and(  edc_pagos.FUF like '%P00%' or edc_pagos.FUF like '%Pd%')) as pagos 
	    left join Complemento_pagos as complemento
	    on pagos.folioo = complemento.Folio_Relacionado

    union

    select [fecha de operacion] + 7 , fuf, monto,pagos.folioo,[Fecha Pago],Imp_Pagado as 'importe pagado'

    from(
    SELECT 
      [Estado de cuenta] as 'fecha de operacion' ,edc_pagos.fuf as 'fuf',
      factura.Folio as 'folioo',
        ABS([Monto + iva]) AS 'monto'
        FROM [dbo].[EDC_Pagos] as edc_pagos left join CFDI_Emitidos_CENACE as factura
	    on edc_pagos.FUF = factura.FUF
	    where [Fecha de pago] = '{fecha_pago2}' and( 
	    edc_pagos.FUF like '%cC%' )) as pagos 
	    left join Complemento_pagos as complemento
	    on pagos.folioo = complemento.Folio_Relacionado

                        """
                query = query.format(fecha_pago2=fecha_pago2)
                monto_C15 = pd.read_sql(query, cnxn)
                monto_cobrar15 = monto_C15['Monto'].sum()
                monto_cobrado15 = monto_C15['Importe pagado'].sum()
                monto_cobrar15 = monto_cobrar15 - monto_cobrado15

                excel_cobros15 = {"nombre": "Montos por cobrar al " + fecha_pago2 , "hojas": [{"titulo":"Hoja 1", "dataframe": monto_C15}]}
           
            with cnxn:
                query = """  SELECT [Estado de cuenta] + 7 as 'Fecha FUECD'
                ,fuf AS 'FUF',
        ABS([Monto + iva]) AS 'Monto'
	    ,pagos_admin.Fecha_vigencia as 'Fecha de Pago'
	    ,pagos_admin.Importe as 'Importe pagado'
        FROM [dbo].[EDC_Pagos] as edc_pagos 
        left join [dbo].[Pagos_Admin] as pagos_admin
	    on edc_pagos.FUF = pagos_admin.Folio
	    where [Fecha de pago] =  '{fecha_pago2}' and( 
	    FUF like '%PC%' or FUF like '%C00%' or FUF like '%Cd%')
	    order by [Estado de cuenta] asc, fuf asc 
                        """
                query = query.format(fecha_pago2=fecha_pago2)
                monto_p15 = pd.read_sql(query, cnxn)
                monto_pagar15 =monto_p15['Monto'].sum()
                monto_pagado15 = monto_p15['Importe pagado'].sum()
                monto_pagar15 = monto_pagar15 - monto_pagado15
          
           
                monto_pagar15 = "${:,.2f}".format(monto_pagar15)  
                monto_cobrar15 = "${:,.2f}".format(monto_cobrar15) 




#########################3333
            with cnxn:
                query = """

            
    select  pagos.[Estado de cuenta] + 7  as 'Fecha FUECD'
    ,pagos.subcuenta as 'Subcuenta'
    ,pagos.fuf as 'FUF'
    ,pagos.uuid as 'UUID (Folio Fiscal)'
    ,pagos.folio as 'Folio Interno'
    ,tabla.folio_relacionado as 'Folio Relacionado'
    ,pagos.[Fecha de pago] as 'Fecha Limite de Pago'
    ,abs(pagos.[Monto + iva]) as 'Monto + Iva'
				
                
		    from (SELECT 
		    facturas.fuf as 'fufu',
		    facturas.Folio as 'folio_relacionado'
		    ,[Fecha de operación] as 'fecha'
   
		    FROM [dbo].[EDC_Pagos] as edc_pagos join Facturas as facturas 
		    on facturas.FUF = edc_pagos.FUF
		    where facturas.fuf like '%P00' )
    as tabla 
			
    right join (     
			 
			 
		    SELECT [Estado de cuenta]
		    ,[Subcuenta]
		    ,pagos.FUF
		    ,format(Facturas.Folio,'#') as 'folio'
		    ,facturas.UUID
		    ,[Fecha de pago]
		    ,[Fecha de operación]
		    ,[Monto]
		    ,[Monto + iva]
		    FROM [dbo].[EDC_Pagos] as pagos left join [dbo].[Facturas] as facturas
		    on pagos.FUF = facturas.FUF
		    where ([Fecha de pago] = '{fecha_pago}'
		    or [Fecha de pago] = '{fecha_pago2}')
		    and pagos.[FUF] like '%P%' ) 
      as pagos
    on tabla.fufu = pagos.FUF
    order by pagos.[Estado de cuenta] desc

                        """
                query = query.format(fecha_pago=fecha_pago,fecha_pago2=fecha_pago2)
                df = pd.read_sql(query, cnxn)


            with cnxn:
                query = """
                 SELECT 'C0',
            abs(sum([Monto + iva]))
            FROM [dbo].[EDC_Pagos] where ([Fecha de pago]  = '{fecha_pago}') and FUF like '%C0%'

                        """
                query = query.format(fecha_pago=fecha_pago,fecha_pago2=fecha_pago2)
                dfC0 = pd.read_sql(query, cnxn)
                C0=dfC0.iloc[0,1]

            with cnxn:
                query = """
                        SELECT 'P0',
                        abs(sum([Monto + iva]))
                        FROM [dbo].[EDC_Pagos] where ([Fecha de pago]  = '{fecha_pago}' ) 
                       and FUF like '%P0%'
                        """
                query = query.format(fecha_pago=fecha_pago,fecha_pago2=fecha_pago2)
                dfP0 = pd.read_sql(query, cnxn)

                P0=dfP0.iloc[0,1]

            with cnxn:
                query = """
                    
        SELECT 'PC',
        abs(sum([Monto + iva]))
        FROM [dbo].[EDC_Pagos] where ([Fecha de pago]  = '{fecha_pago}' ) and FUF like '%PC%'
                        """
                query = query.format(fecha_pago=fecha_pago,fecha_pago2=fecha_pago2)
                dfPC = pd.read_sql(query, cnxn)
                PC=dfPC.iloc[0,1]

            with cnxn:
                query = """
                       SELECT 'PD',
       abs(sum([Monto + iva]))
        FROM [dbo].[EDC_Pagos] where ([Fecha de pago]  = '{fecha_pago}')  and FUF like '%PD%'
                        """
                query = query.format(fecha_pago=fecha_pago,fecha_pago2=fecha_pago2)
                dfPD = pd.read_sql(query, cnxn)
                PD=dfPD.iloc[0,1]

            with cnxn:
                query = """
       SELECT 'CC',
        abs(sum([Monto + iva]))
        FROM [dbo].[EDC_Pagos] where ([Fecha de pago]  = '{fecha_pago}' )  and FUF like '%CC%'
                        """
                query = query.format(fecha_pago=fecha_pago,fecha_pago2=fecha_pago2)
                dfCC = pd.read_sql(query, cnxn)
                CC=dfCC.iloc[0,1]

            with cnxn:
                query = """
       SELECT 'CD',
        abs(sum([Monto + iva]))
        FROM [dbo].[EDC_Pagos] where ([Fecha de pago]  = '{fecha_pago}' 
            or [Fecha de pago] =  '{fecha_pago2}')  and FUF like '%CD%'
                        """
                query = query.format(fecha_pago=fecha_pago,fecha_pago2=fecha_pago2)
                dfCD = pd.read_sql(query, cnxn)
                CD=dfCD.iloc[0,1]

            

                PC = "${:,.2f}".format(PC)
                PD = "${:,.2f}".format(PD)
                P0 = "${:,.2f}".format(P0)

    #por facturar 2

            with cnxn:
                query = """
                    SELECT 'C0',
            abs(sum([Monto + iva]))
            FROM [dbo].[EDC_Pagos] where ( [Fecha de pago] =  '{fecha_pago2}') and FUF like '%C0%'

                        """
                query = query.format(fecha_pago=fecha_pago,fecha_pago2=fecha_pago2)
                dfC0 = pd.read_sql(query, cnxn)
                C02=dfC0.iloc[0,1]

            with cnxn:
                query = """
                        SELECT 'P0',
                        abs(sum([Monto + iva]))
                        FROM [dbo].[EDC_Pagos] where ([Fecha de pago] =  '{fecha_pago2}') 
                        and FUF like '%P0%'
                        """
                query = query.format(fecha_pago=fecha_pago,fecha_pago2=fecha_pago2)
                dfP0 = pd.read_sql(query, cnxn)

                P02=dfP0.iloc[0,1]

            with cnxn:
                query = """
                    
        SELECT 'PC',
        abs(sum([Monto + iva]))
        FROM [dbo].[EDC_Pagos] where ([Fecha de pago] =  '{fecha_pago2}') and FUF like '%PC%'
                        """
                query = query.format(fecha_pago=fecha_pago,fecha_pago2=fecha_pago2)
                dfPC = pd.read_sql(query, cnxn)
                PC2=dfPC.iloc[0,1]
                print(dfPC)
           

            with cnxn:
                query = """
                        SELECT 'PD',
        abs(sum([Monto + iva]))
        FROM [dbo].[EDC_Pagos] where ( [Fecha de pago] =  '{fecha_pago2}')  and FUF like '%PD%'
                        """
                query = query.format(fecha_pago=fecha_pago,fecha_pago2=fecha_pago2)
                dfPD = pd.read_sql(query, cnxn)
                PD2=dfPD.iloc[0,1]

            with cnxn:
                query = """
        SELECT 'CC',
        abs(sum([Monto + iva]))
        FROM [dbo].[EDC_Pagos] where ( [Fecha de pago] =  '{fecha_pago2}')  and FUF like '%CC%'
                        """
                query = query.format(fecha_pago=fecha_pago,fecha_pago2=fecha_pago2)
                dfCC = pd.read_sql(query, cnxn)
                CC2=dfCC.iloc[0,1]

            with cnxn:
                query = """
        SELECT 'CD',
        abs(sum([Monto + iva]))
        FROM [dbo].[EDC_Pagos] where ([Fecha de pago] =  '{fecha_pago2}')  and FUF like '%CD%'
                        """
                query = query.format(fecha_pago=fecha_pago,fecha_pago2=fecha_pago2)
                dfCD = pd.read_sql(query, cnxn)
                CD2=dfCD.iloc[0,1]
                
            
                print(PD2)
                if PC2 is not None:
                   
                    PC2 = "${:,.2f}".format(PC2)

                if PD2 is not None:
                   
                    PD2 = "${:,.2f}".format(PD2)
                if P02 is not None:
                   
                    P02 = "${:,.2f}".format(P02)

            with cnxn:
                query = """ 
                 select [fecha de operacion] + 7 as 'Fecha FUECD', fuf  as 'FUF'
			    , monto,pagos.folioo as 'Folio Interno de Facturación'
    ,[Fecha Pago] as 'Fecha de Pago',
                Imp_Pagado as 'Importe pagado'
    from(
    SELECT 
      [Estado de cuenta] as 'fecha de operacion' ,edc_pagos.fuf as 'fuf',
      factura.Folio as 'folioo',
        ABS([Monto + iva]) AS 'monto'
        FROM [dbo].[EDC_Pagos] as edc_pagos left join Facturas as factura
	    on edc_pagos.FUF = factura.FUF
	    where [Fecha de pago] = '{fecha_pago}' and( edc_pagos.FUF like '%P00%' or edc_pagos.FUF like '%Pd%')) as pagos 
	    left join Complemento_pagos as complemento
	    on pagos.folioo = complemento.Folio_Relacionado

    union

    select [fecha de operacion], fuf, monto,pagos.folioo,[Fecha Pago],Imp_Pagado as 'importe pagado'

    from(
    SELECT 
      [Estado de cuenta] + 7 as 'fecha de operacion' ,edc_pagos.fuf as 'fuf',
      factura.Folio as 'folioo',
        ABS([Monto + iva]) AS 'monto'
        FROM [dbo].[EDC_Pagos] as edc_pagos left join CFDI_Emitidos_CENACE as factura
	    on edc_pagos.FUF = factura.FUF
	    where [Fecha de pago] = '{fecha_pago}' and( 
	    edc_pagos.FUF like '%cC%' )) as pagos 
	    left join Complemento_pagos as complemento
	    on pagos.folioo = complemento.Folio_Relacionado

                        """
                query = query.format(fecha_pago=fecha_pago)
                monto_C = pd.read_sql(query, cnxn)
                monto_cobrar = monto_C['monto'].sum()
                monto_cobrado = monto_C['Importe pagado'].sum()
                monto_cobrar = monto_cobrar - monto_cobrado

                excel_cobros = {"nombre": "Montos por cobrar al " + fecha_pago , "hojas": [{"titulo":"Hoja 1", "dataframe": monto_C}]}
           
            with cnxn:
                query = """  SELECT [Estado de cuenta] + 7 as 'Fecha FUECD'
                ,fuf AS 'FUF',
        ABS([Monto + iva]) AS 'Monto'
	    ,pagos_admin.Fecha_vigencia as 'Fecha de Pago'
	    ,pagos_admin.Importe as 'Importe Pagado'
        FROM [dbo].[EDC_Pagos] as edc_pagos 
        left join [dbo].[Pagos_Admin] as pagos_admin
	    on edc_pagos.FUF = pagos_admin.Folio
	    where [Fecha de pago] =  '{fecha_pago}' and( 
	    FUF like '%PC%' or FUF like '%C00%' or FUF like '%Cd%')
	    order by [Estado de cuenta] asc, fuf asc 
                        """
                query = query.format(fecha_pago=fecha_pago)
                monto_p = pd.read_sql(query, cnxn)
                monto_pagar =monto_p['Monto'].sum()
                monto_pagado = monto_p['Importe Pagado'].sum()
                monto_pagar = monto_pagar - monto_pagado
          
           
                monto_pagar = "${:,.2f}".format(monto_pagar)  
                monto_cobrar = "${:,.2f}".format(monto_cobrar)  

            with cnxn:
                query = """
 select total.[Dia de operación],
 total.[ BTMNN],
 total.CENACE,
 tbfin.total_pagos as TBFIN,
 total.[ BTMNN] - total.CENACE + (ISNULL(tbfin.total_pagos,0) ) as 'Total Diario'

 from
 (Select pagos.EDC_pagos as 'Dia de operación'
, pagos.total_pagos as ' BTMNN', 
cobros.[total cobros] as 'CENACE'
,  pagos.total_pagos-cobros.[total cobros] as 'P&L'
from
(SELECT [Fecha de operación] as 'EDC_pagos'
      ,	sum( CASE WHEN fuf  like '%P00%' THEN  [Monto + iva]  WHEN fuf like '%PC%' THEN (-1)*[Monto + iva] WHEN fuf like '%PD%' THEN (-1)*[Monto + iva] ELSE 0 END)  as 'total_pagos'
  FROM [dbo].[EDC_Pagos] where  Subcuenta = 'b01'
  group by [Fecha de operación]  ) as pagos join 
  (SELECT [Fecha de operación] as 'EDC_cobros'
      ,	sum( CASE WHEN fuf  like '%c00%' THEN  [Monto + iva]  WHEN fuf like '%cC%' THEN (-1)*[Monto + iva] WHEN fuf like '%cD%' THEN (-1)*[Monto + iva] ELSE 0 END)  as 'total cobros'
  FROM [dbo].[EDC_Pagos] where  Subcuenta = 'b01'
  group by [Fecha de operación]  )as cobros 
  on pagos.EDC_pagos=cobros.EDC_cobros ) as total left join
 (
  SELECT [Fecha de operación] as 'EDC_pagos'
      ,	sum( CASE WHEN fuf  like '%P00%' THEN  [Monto + iva]  WHEN fuf like '%PC%' THEN (-1)*[Monto + iva] WHEN fuf like '%PD%' THEN (-1)*[Monto + iva] ELSE 0 END)  as 'total_pagos'
  FROM [dbo].[EDC_Pagos] where  Subcuenta = 'b02'
  group by [Fecha de operación]  ) as tbfin
  on tbfin.EDC_pagos = total.[Dia de operación]
  order by total.[Dia de operación] asc
                        """
                query = query.format(fecha_pago=fecha_pago)
                balance = pd.read_sql(query, cnxn)
                excel_balance = {"nombre": "Balance "  ,"hojas": [{"titulo":"Hoja 1", "dataframe": balance}]}
            
                PagosBTMNN = balance[' BTMNN'].sum()
                CobrosBTMNN = balance['CENACE'].sum()
                PLBTMNN = balance['Total Diario'].sum()  
                PLBTMNN = "${:,.2f}".format(PLBTMNN)

                excel_pagos = {"nombre": "Montos por pagar al " + fecha_pago ,"hojas": [{"titulo":"Hoja 1", "dataframe": monto_p}]}

                #return descargar_excel(excel_cobros,)
                excel_facturas_x = {"nombre": "Facturas por emitir","hojas": [{"titulo":"Hoja 1", "dataframe": df}]}

                excel_pagos15 = {"nombre": "Montos por pagar al " + fecha_pago2 ,"hojas": [{"titulo":"Hoja 1", "dataframe": monto_p15}]}

            
 
                
                
                excel_ba = excel_balance
                excel_pagos_g = excel_pagos
                excel_cobros_g= excel_cobros
                excel_facturas = excel_facturas_x
                excel_pagos_g15 = excel_pagos15
                excel_cobros_g15= excel_cobros15

            cnxn = crear_conexion_SQL()
            with cnxn:
                query = """
Select 
edc + 7 as 'Fecha FUECD',
subcuenta as 'Subcuenta',
fuf as 'FUF',
todo.[Fecha de factura],
uuid as 'UUID Factura',
todo.folio as 'Folio Interno',
folio_relacionado as 'Folio Relacionado',
abs(monto) as 'Monto',
todo.pago_complemento as 'Fecha de Pago', 
todo.uuid2 as 'UUID Complemento de Pago',
todo.[fecha complemento] as 'Fecha complemento de pago',
[complemento folio] as 'Folio del Complemento de Pago',
todo.[importe pagado] as 'Monto en el Complemento de Pago'

from(  
	   select facturas_particpante.edc as 'edc',
	   facturas_particpante.[Fecha de factura],
	   facturas_particpante.[Fecha de pago] as 'fecha de pago',
  facturas_particpante.subcuenta as 'subcuenta',
  facturas_particpante.fuf as 'fuf',
  facturas_particpante.uuid as'uuid',
  facturas_particpante.folio as 'folio',
  facturas_particpante.folio_relacionado as 'folio_relacionado',
  facturas_particpante.[Monto + iva] as'monto',
  complemento.folio as 'complemento folio',
  complemento.Folio_Relacionado as 'folio relacionado',
  complemento.Imp_Pagado as 'importe pagado',
  complemento.UUID as 'uuid2',
  complemento.[Fecha Pago] as 'pago_complemento'
  ,complemento.Fecha as 'fecha complemento'
  
  
  from 
	   (select pagos.edc
                ,pagos.subcuenta
                ,pagos.fuf
                ,pagos.uuid
				,pagos.folio
				,tabla.folio_relacionado
				,pagos.[Monto + iva],
				pagos.[Fecha de pago]
				,pagos.[Fecha de factura]
                
    from (SELECT 
	subcuenta as 'subcuenta',
                facturas.fuf as 'fufu',
				facturas.Folio as 'folio_relacionado'
				,[Fecha de operación] as 'fecha'
				
				
   
            FROM [dbo].[EDC_Pagos] as edc_pagos left join [CFDI_Emitidos_CENACE] as facturas 
            on facturas.FUF = edc_pagos.FUF
			where edc_pagos.fuf like '%c00' )as tabla 
			
			right join (SELECT 
                [Monto + iva]
	            ,facturas.Folio as 'folio'
			,[Estado de cuenta] as 'edc'
                ,[Subcuenta] as 'subcuenta'
                ,[edc_pagos].fuf as 'fuf'
                ,[UUID] as 'uuid'
                
                ,[Fecha de operación] as 'operacion'
   
	            ,facturas.Fecha as 'Fecha de factura',
				edc_pagos.[Fecha de pago] as 'Fecha de pago'

	            
            FROM [dbo].[EDC_Pagos] as edc_pagos left join [CFDI_Emitidos_CENACE] as facturas 
            on facturas.FUF = edc_pagos.FUF
			where edc_pagos.fuf like '%cc%'
			) as pagos
			on tabla.fecha = pagos.operacion
           where pagos.[Fecha de factura] >= '{fecha1}' and pagos.[Fecha de factura] - 1 <= '{fecha2}')
			as facturas_particpante
			left join [Complemento_pagos] as complemento
	on facturas_particpante.folio = complemento.[Folio_Relacionado]
	)as todo left join [dbo].[Pagos_Admin] as pagos_admin
	on todo.fuf = pagos_admin.Folio

	union 

	   Select 
edc + 7 as 'Fecha FUECD',
subcuenta as 'Subcuenta',
fuf as 'FUF',
todo.[Fecha de factura],
uuid as 'UUID Factura',
todo.folio as 'Folio Interno',
folio_relacionado as 'Folio Relacionado',
abs(monto) as 'Monto',
todo.pago_complemento as 'Fecha de Pago',
todo.uuid2 as 'UUID Complemento de Pago',
todo.[fecha complemento] as 'Fecha complemento de pago',
[complemento folio] as 'Folio del Complemento de Pago',
todo.[importe pagado] as 'Monto en el Complemento de Pago'




from(  
	   select facturas_particpante.edc as 'edc',
	   facturas_particpante.[Fecha de factura],
	   facturas_particpante.[Fecha de pago] as 'fecha de pago',
  facturas_particpante.subcuenta as 'subcuenta',
  facturas_particpante.fuf as 'fuf',
  facturas_particpante.uuid as'uuid',
  facturas_particpante.folio as 'folio',
  facturas_particpante.folio_relacionado as 'folio_relacionado',
  facturas_particpante.[Monto + iva] as'monto',
  complemento.folio as 'complemento folio',
  complemento.Folio_Relacionado as 'folio relacionado',
  complemento.Imp_Pagado as 'importe pagado',
  complemento.UUID as 'uuid2',
   complemento.[Fecha Pago] as 'pago_complemento'
   ,complemento.Fecha as 'fecha complemento'
  
  
  from 
	   (select pagos.edc,
	   pagos.[Fecha de factura],
                pagos.subcuenta
                ,pagos.fuf
                ,pagos.uuid
				,pagos.folio
				,tabla.folio_relacionado
				,pagos.[Monto + iva],
				pagos.[Fecha de pago]
                
    from (SELECT 
                facturas.fuf as 'fufu',
				facturas.Folio as 'folio_relacionado'
				,[Fecha de operación] as 'fecha'
				,facturas.Participante
				
   
            FROM [dbo].[EDC_Pagos] as edc_pagos left join [Facturas] as facturas 
            on facturas.FUF = edc_pagos.FUF
			where edc_pagos.fuf like '%P00' )as tabla 
			
			right join (SELECT 
                [Monto + iva]
	            ,facturas.Folio as 'folio'
			,[Estado de cuenta] as 'edc'
                ,[Subcuenta] as 'subcuenta'
                ,[edc_pagos].fuf as 'fuf'
                ,[UUID] as 'uuid'
                
                ,[Fecha de operación] as 'operacion'
   
	            ,facturas.Fecha as 'Fecha de factura',
				edc_pagos.[Fecha de pago] as 'Fecha de pago'
				,facturas.Participante
	            
            FROM [dbo].[EDC_Pagos] as edc_pagos left join Facturas as facturas 
            on facturas.FUF = edc_pagos.FUF
			where edc_pagos.fuf like '%PD%' or edc_pagos.fuf like '%P00' ) as pagos
			on tabla.fecha = pagos.operacion and tabla.Participante = pagos.Participante
            where pagos.[Fecha de factura] >= '{fecha1}' and pagos.[Fecha de factura] - 1 <= '{fecha2}'  )
			as facturas_particpante
			left join [Complemento_pagos] as complemento
	on facturas_particpante.folio = complemento.[Folio_Relacionado]
	)as todo left join [dbo].[Pagos_Admin] as pagos_admin
	on todo.fuf = pagos_admin.Folio
    order by todo.[Fecha de factura] asc
                      
               
            """
                query = query.format(fecha1=fecha1,fecha2=fecha2)
                df2 = pd.read_sql(query, cnxn)
                monto_total = df2['Monto'].sum()
                monto_total="${:,.2f}".format(monto_total) 
                excel_facturas_participante_g = {"nombre": "Facturas por cobrar " + fecha1 + " al " + fecha2 ,"hojas": [{"titulo":"Hoja 1", "dataframe": df2}]}

                

                excel_facturas_participante = excel_facturas_participante_g

            


            return render_template("/dashboard/facturas.html",datos=df.to_html(classes=[ "records_list table table-striped table-bordered table-hover", "mydatatable"], justify='center', table_id='mydatatable'),
                             monto_pagar=monto_pagar,
                                   monto_cobrar=monto_cobrar
                                   ,fecha_pago=fecha_pago,
                                   P0=P0,C0=C0,PC=PC,PD=PD,CC=CC,CD=CD,
                                   P02=P02,C02=C02,PC2=PC2,PD2=PD2,CC2=CC2,CD2=CD2,
                                   PagosBTMNN=PagosBTMNN,
                                   CobrosBTMNN=CobrosBTMNN,
                                   PLBTMNN=PLBTMNN,
                                   show1='show',
                                   monto_pagar15=monto_pagar15,
                                   monto_cobrar15=monto_cobrar15
                                   ,fecha_pago2=fecha_pago2,
                                   monto_total=monto_total,
                                   descarga_F_participante="display:block",
                                   descarga_F_cenace="display:none",
                                   montoto="display:none",
                                   balance=balance.to_html(classes=[ "records_list table table-striped table-bordered table-hover", "mydatatable"], justify='center', table_id='mydatatable2'),
                                   datos2=df2.to_html(classes=[ "records_list table table-striped table-bordered table-hover", "mydatatable"], justify='center', table_id='mydatatable3'))


        elif request.form["boton_submit"] == "Descarga_facturas":
            return descargar_excel(excel_facturas)

        elif request.form["boton_submit"] == "Descarga_pagos":
            return descargar_excel(excel_pagos_g)
        elif request.form["boton_submit"] == "Descarga_cobros":
            return descargar_excel(excel_cobros_g)
        elif request.form["boton_submit"] == "Descarga_pagos15":
            return descargar_excel(excel_pagos_g15)
        elif request.form["boton_submit"] == "Descarga_cobros15":
            return descargar_excel(excel_cobros_g15)
        elif request.form["boton_submit"] == "Descarga_facturas_participante":
            return descargar_excel(excel_facturas_participante)
        elif request.form["boton_submit"] == "Descarga_facturas_cenace":
            return descargar_excel(excel_facturas_cenace)

        elif request.form["boton_submit"] == "Descarga_Balance":
            return descargar_excel(excel_ba)

#######################################


def rea_test():
    return render_template("/dashboard/rea_test.html")

#from internal_libraries import rea as rea


def calculo_rea():
    
    if request.method == "GET":

        rea_color, df, cenace_factor_volatilidad, cenace_rea, cenace_date, cenace_pc, cenace_ppe,cenace_oopm, cenace_opc,cenace_cpmcp, cenace_cpmtdo, cenace_tmp, cenace_pc_difference, cenace_ppe_difference, cenace_rea_difference, inc_pc, inc_ppe, inc_rea, MGP=rea.obtener_rea()
                
        return render_template("/dashboard/calculo_rea.html", rea_color=rea_color, original_cenace_rea=df["REA"][0],
                                original_cenace_ppe=df["Pasivo potencial"][0], original_cenace_pc=df["Pasivo conocido"][0],
                                cenace_rea=cenace_rea, cenace_ppe=cenace_ppe, cenace_pc=cenace_pc, cenace_date=cenace_date,
                                cenace_factor_volatilidad=cenace_factor_volatilidad, cenace_oopm=cenace_oopm,
                                cenace_opc=cenace_opc, cenace_cpmcp=cenace_cpmcp, cenace_cpmtdo=cenace_cpmtdo,
                                cenace_tmp=cenace_tmp, cenace_pc_difference=cenace_pc_difference,
                                cenace_ppe_difference=cenace_ppe_difference, cenace_rea_difference=cenace_rea_difference,
                                inc_pc=inc_pc, inc_ppe=inc_ppe, inc_rea=inc_rea, REA_vs_MGP=MGP)
        

    elif request.method == "POST":
        #date_input = date.fromisoformat(request.form["fecha"])
        day, month, year = int(request.form["fecha"][8:10]), int(request.form["fecha"][5:7]), int(request.form["fecha"][0:4])
        rea_color,df, cenace_rea, cenace_ppe, cenace_pc, cenace_date, cenace_factor_volatilidad, cenace_oopm, cenace_opc, cenace_cpmcp,  cenace_cpmtdo, cenace_tmp, cenace_pc_difference, cenace_ppe_difference, cenace_rea_difference, inc_pc, inc_ppe, inc_rea, REA, PPE, PC, btmnn_rea, btmnn_ppe, btmnn_pc, btmnn_date, btmnn_factor_volatilidad, btmnn_oopm, btmnn_opc, btmnn_cpmcp, btmnn_cpmtdo, btmnn_tmp, rea_error, ppe_error, pc_error, MGP=rea.buscar_rea(year, month, day)
        return render_template("/dashboard/calculo_rea.html", rea_color=rea_color, original_cenace_rea=df["REA"][0],
                                original_cenace_ppe=df["Pasivo potencial"][0], original_cenace_pc=df["Pasivo conocido"][0],
                                cenace_rea=cenace_rea, cenace_ppe=cenace_ppe, cenace_pc=cenace_pc, cenace_date=cenace_date,
                                cenace_factor_volatilidad=cenace_factor_volatilidad, cenace_oopm=cenace_oopm,
                                cenace_opc=cenace_opc, cenace_cpmcp=cenace_cpmcp, cenace_cpmtdo=cenace_cpmtdo,
                                cenace_tmp=cenace_tmp, cenace_pc_difference=cenace_pc_difference,
                                cenace_ppe_difference=cenace_ppe_difference, cenace_rea_difference=cenace_rea_difference,
                                inc_pc=inc_pc, inc_ppe=inc_ppe, inc_rea=inc_rea, original_btmnn_rea=REA, original_btmnn_ppe=PPE,
                                original_btmnn_pc=PC, btmnn_rea=btmnn_rea, btmnn_ppe=btmnn_ppe, btmnn_pc=btmnn_pc,
                                btmnn_date=btmnn_date,btmnn_factor_volatilidad=btmnn_factor_volatilidad, btmnn_oopm=btmnn_oopm,
                                btmnn_opc=btmnn_opc, btmnn_cpmcp=btmnn_cpmcp, btmnn_cpmtdo=btmnn_cpmtdo, btmnn_tmp=btmnn_tmp,
                                rea_error=rea_error, ppe_error=ppe_error, pc_error=pc_error, REA_vs_MGP=MGP)

##############################################################


def conciliacion1(fecha=None):

    if fecha==None:
        dicc_dataFrames, dataFrame_suma, lista_excel, fecha_1, fecha_2 = cn.conciliacion_actual()
    else:
        dicc_dataFrames, dataFrame_suma, lista_excel, fecha_1, fecha_2 = cn.conciliacion_actual(fecha)

    if request.method == "GET":
        
        #mandar al html
        return render_template("dashboard/conciliacion.html", primeratabla = dataFrame_suma, diccionario_dataFrame=dicc_dataFrames, fecha_1=fecha_1, fecha_2=fecha_2)


    elif request.method == "POST":
        if request.form["boton_submit"] == "Descarga":
            id_boton_presionado = request.form.get('idOculto')
            id_boton_presionado=int(id_boton_presionado)
            return send_file((lista_excel[id_boton_presionado]), attachment_filename='fecha.xlsx', as_attachment=True)
            
        elif request.form["boton_submit"]=="Consulta":
            fecha1=request.form.get("fecha")
            cache.clear()
            redireccion="/conciliacion"+fecha1
            return redirect(url_for('conciliacion1', fecha=fecha1))
         

@login_required  
def Consultas_mercados():
    if request.method == "GET":
        return render_template('/dashboard/Consultas_Mercados.html')

    elif request.method == "POST":

        fecha = request.form["fecha"]
        fecha_2 = request.form["fecha_2"]
        clave_de_carga = request.form["clave_de_carga"]
        tipo_de_datos = request.form["tipo_de_datos"]


            # TODOS
        if tipo_de_datos == "REA":
            ataFrame_Ced_final,dataFrame_CE_final ,dataFrame_B08030_DF , dataFrame_B09030_DF , excel = cn.main_REA_consulta_operacion(fecha,fecha_2,clave_de_carga)
            pd.set_option("display.max_rows", None, "display.max_columns", None)
            print(ataFrame_Ced_final,dataFrame_CE_final ,dataFrame_B08030_DF , dataFrame_B09030_DF)

            
        elif tipo_de_datos == "CONCILIACION":

            dataFrame_mda, dataFrame_mtr , excel = cn.main_conciliacion_consulta_operacion(fecha,fecha_2,clave_de_carga)

            pd.set_option("display.max_rows", None, "display.max_columns", None)
            print(dataFrame_mda, dataFrame_mtr)


                    
        if request.form["boton_submit"] == "Consulta":
            return render_template('/dashboard/Consultas_Mercados.html')

        elif request.form["boton_submit"] == "Descarga":
            return descargar_excel(excel)
    
    return render_template('/dashboard/Consultas_Mercados.html')


#####################################################


###################WS Para ofertas de compra
@login_required
def dashboard_WS_ODC():
    if request.method == "GET":
        hora_1=""
        return render_template('/dashboard/WS_ODC.html', Metodo_promedio="display:block" ,
                               Metodo_dia="display:none",hora_1=hora_1, display="display:none",display2="display:none",
                               displayODC="display:none", fechas_porcentaje_style="display:none")
    elif request.method == "POST":
        
        #Metodo para calcular ODC por promedio 
        if request.form["boton_submit"] == "Calcular":
            
            
            fechas=request.form.get('arreglo_fechas')
            fechas=fechas.split(',')
            cliente = request.form["cliente"]
            
            VariablesGlobales.caso_odc = 0

            valores_horas,Total,dataFrame_tabla_promedio, etiquetas, dataset= odc.dia_promedio(fechas, cliente)

            return render_template('/dashboard/WS_ODC.html',Metodo_promedio="display:none" ,Metodo_dia="display:none",
                                   display="display:none",display2="display:block",displayODC="display:block",cuadro_opcion_style="display:none",etiquetas=etiquetas,dataset=dataset,
                                  metodo='PROM',TOTAL=Total, hora_1=valores_horas[0] , hora_2=valores_horas[1] , hora_3=valores_horas[2]
                                   , hora_4=valores_horas[3], hora_5=valores_horas[4], hora_6=valores_horas[5], hora_7=valores_horas[6], hora_8=valores_horas[7]
                                   , hora_9=valores_horas[8], hora_10=valores_horas[9], hora_11=valores_horas[10], hora_12=valores_horas[11], hora_13=valores_horas[12]
                                   , hora_14=valores_horas[13], hora_15=valores_horas[14], hora_16=valores_horas[15], hora_17=valores_horas[16], hora_18=valores_horas[17]
                                   , hora_19=valores_horas[18], hora_20=valores_horas[19], hora_21=valores_horas[20], hora_22=valores_horas[21], hora_23=valores_horas[22]
                                   , hora_24=valores_horas[23], fechas_porcentaje_style = "display:block", label_fechas_valor=listToString(fechas), porcentaje=100, porcentaje_anterior=100, label_cliente=cliente, ODC=dataFrame_tabla_promedio.to_html(classes=['table table-bordered dataTable" id = "dataTable2']))

        elif request.form["boton_submit"] == "Calcular2":
    
            if request.form["consultad"] == "Otro":
                #solicitar los dias a buscar
                dias_buscados=request.form['label_dias']
                dias_buscados=dias_buscados.split(',')

                fecha_x = request.form['fecha_x']
                Dias = request.form['Dia_Pronostico']
                
                VariablesGlobales.caso_odc = 1
                horas, etiquetas, dataFrame_final, fechas, Total, dataset=odc.otro(dias_buscados, fecha_x, Dias)

                return render_template('/dashboard/WS_ODC.html',Metodo_promedio="display:none" ,Metodo_dia="display:none" ,display="display:none",display2="display:block",displayODC="display:block", cuadro_opcion_style="display:none"
                                       ,TOTAL=Total,fecha_x= fecha_x,hora_1=horas[0] , hora_2=horas[1] , hora_3=horas[2], etiquetas=etiquetas,dataset=dataset
                            , hora_4=horas[3], hora_5=horas[4], hora_6=horas[5], hora_7=horas[6], hora_8=horas[7]
                            , hora_9=horas[8], hora_10=horas[9], hora_11=horas[10], hora_12=horas[11], hora_13=horas[12]
                            , hora_14=horas[13], hora_15=horas[14], hora_16=horas[15], hora_17=horas[16], hora_18=horas[17]
                            , hora_19=horas[18], hora_20=horas[19], hora_21=horas[20], hora_22=horas[21], hora_23=horas[22]
                            , hora_24=horas[23], fechas_porcentaje_style = "display:block",metodo='DMP',label_fechas_valor=listToString(fechas),
                            ODC=dataFrame_final.to_html(classes=["table-bordered", "table-striped", "table-hover", "table"]))
            else:
                Dias = request.form['Dia_Pronostico']
                #print("Dia mas parecido")
                
                df, Total, resultados = odc.dia_mas_parecido(Dias)

                hora_1 = df.iloc[0,4]
                hora_2 = df.iloc[1,4]
                hora_3 = df.iloc[2,4]
                hora_4 = df.iloc[3,4]
                hora_5 = df.iloc[4,4]
                hora_6 = df.iloc[5,4]
                hora_7 = df.iloc[6,4]
                hora_8 = df.iloc[7,4]
                hora_9 = df.iloc[8,4]
                hora_10 = df.iloc[9,4]
                hora_11 = df.iloc[10,4]
                hora_12 = df.iloc[11,4]
                hora_13 = df.iloc[12,4]
                hora_14 = df.iloc[13,4]
                hora_15 = df.iloc[14,4]
                hora_16 = df.iloc[15,4]
                hora_17 = df.iloc[16,4]
                hora_18 = df.iloc[17,4]
                hora_19 = df.iloc[18,4]
                hora_20 = df.iloc[19,4]
                hora_21 = df.iloc[20,4]
                hora_22 = df.iloc[21,4]
                hora_23 = df.iloc[22,4]
                hora_24 = df.iloc[23,4]

                titulo, caso, labels, label1, label2, label3, label4, values, values2, values3, values4 = formato_grafica("WSODC", resultados)
                dataset = [{label1: values}]
                df.index=[x for x in range(1,25)]
                return render_template('/dashboard/WS_ODC.html', Metodo_promedio="display:block" ,Metodo_dia="display:none",
                                       display="display:block",display2="display:none",displayODC="display:block", fechas_porcentaje_style = "display:none"
                                     ,labels=labels, dataset=dataset  ,TOTAL=Total,
                                     hora_1=hora_1 , hora_2=hora_2 , hora_3=hora_3
                            , hora_4=hora_4, hora_5=hora_5, hora_6=hora_6, hora_7=hora_7, hora_8=hora_8
                            , hora_9=hora_9, hora_10=hora_10, hora_11=hora_11, hora_12=hora_12, hora_13=hora_13
                            , hora_14=hora_14, hora_15=hora_15, hora_16=hora_16, hora_17=hora_17, hora_18=hora_18
                            , hora_19=hora_19, hora_20=hora_20, hora_21=hora_21, hora_22=hora_22, hora_23=hora_23
                            , hora_24=hora_24, ODC=df.to_html(classes=["table-bordered", "table-striped", "table-hover", "table"]))
        
        elif request.form['boton_submit'] == "Regresar":
            return redirect(url_for('dashboard_WS_ODC'))

        elif request.form["boton_submit"]=="Recalcular":
           

            #Obtener las fehcas que se consultaron
            fechas=request.form['label_fechas']
            cliente=request.form['label_cliente']
            fechas=fechas.split(',')

            #Obtener el metodo que se esta trabajando
            tipo_de_encabezado=request.form['metodo']
            lista_hora=[]
            for contador in range(1,25):
                string_hora="hora_{0}".format(contador)
                valor_hora=request.form.get(string_hora)
                lista_hora.append(float(valor_hora))
        
            #Obtener porcentaje y porcentaje anterior de los inputs horas
            porcentaje=request.form['porcentaje']
            porcentaje_anterior=request.form['porcentaje_anterior']

            
            dataset, dataFrame_ODC, TOTAL, etiquetas, lista_hora=odc.recalcular(fechas, tipo_de_encabezado, porcentaje, porcentaje_anterior, lista_hora, cliente)
            #Agregar lineas al dataset de la gráfica
            if VariablesGlobales.caso_odc == 1:
                for x in range(len(fechas)-1):
                    dataset.append({fechas[x]: dataFrame_ODC.iloc[:,x].to_list()})
            else:
                for x in range(len(fechas)):
                    dataset.append({fechas[x]: dataFrame_ODC.iloc[:,x].to_list()})

            
                
            return render_template('/dashboard/WS_ODC.html', Metodo_promedio="display:none" ,Metodo_dia="display:none",
                                   display="display:none",display2="display:block",displayODC="display:block",cuadro_opcion_style="display:none", TOTAL=TOTAL,etiquetas=etiquetas, dataset=dataset,
                                   hora_1=lista_hora[0] , hora_2=lista_hora[1] , hora_3=lista_hora[2]
                            , hora_4=lista_hora[3], hora_5=lista_hora[4], hora_6=lista_hora[5], hora_7=lista_hora[6], hora_8=lista_hora[7]
                            , hora_9=lista_hora[8], hora_10=lista_hora[9], hora_11=lista_hora[10], hora_12=lista_hora[11], hora_13=lista_hora[12]
                            , hora_14=lista_hora[13], hora_15=lista_hora[14], hora_16=lista_hora[15], hora_17=lista_hora[16], hora_18=lista_hora[17]
                            , hora_19=lista_hora[18], hora_20=lista_hora[19], hora_21=lista_hora[20], hora_22=lista_hora[21], hora_23=lista_hora[22]
                            , hora_24=lista_hora[23], fechas_porcentaje_style = "display:block", label_fechas_valor=listToString(fechas), porcentaje=porcentaje, 
                            porcentaje_anterior=porcentaje,metodo=tipo_de_encabezado, 
                            label_cliente=cliente, ODC=dataFrame_ODC.to_html(classes=["table-bordered", "table-striped", "table-hover", "table"]))

        elif request.form["boton_submit"] == "Enviar":
            
            
            fechaODC = request.form['fechaODC']
            fechaODC= datetime.strptime(fechaODC, '%Y-%m-%d') 
            fechaODC= datetime.strftime(fechaODC, '%d/%m/%Y') 
            #print(fechaODC)
            hora_1 = request.form['hora_1']
            hora_2 = request.form['hora_2']
            hora_3 = request.form['hora_3']
            hora_4 = request.form['hora_4']
            hora_5 = request.form['hora_5']
            hora_6 = request.form['hora_6']
            hora_7 = request.form['hora_7']
            hora_8 = request.form['hora_8']
            hora_9 = request.form['hora_9']
            hora_10 = request.form['hora_10']
            hora_11 = request.form['hora_11']
            hora_12 = request.form['hora_12']
            hora_13 = request.form['hora_13']
            hora_14 = request.form['hora_14']
            hora_15 = request.form['hora_15']
            hora_16 = request.form['hora_16']
            hora_17 = request.form['hora_17']
            hora_18 = request.form['hora_18']
            hora_19 = request.form['hora_19']
            hora_20 = request.form['hora_20']
            hora_21 = request.form['hora_21']
            hora_22 = request.form['hora_22']
            hora_23 = request.form['hora_23']
            hora_24 = request.form['hora_24']


            now = datetime.now() - timedelta(.208333)
            Metodos = request.form['Metodos']
            TOTAL = request.form['total']
            fechas = request.form['label_fechas']
            
            cliente=request.form['label_cliente']

            mensaje=odc.enviar(fechaODC, hora_1, hora_2, hora_3, hora_4, hora_5, hora_6, hora_7, hora_8, hora_9, hora_10, hora_11, hora_12, hora_13, hora_14, hora_15, hora_16, hora_17, hora_18, hora_19, hora_20, hora_21, hora_22, hora_23, hora_24, now, Metodos, TOTAL, fechas, cliente)
            ubuntu="display:block; background-color:rgb(111 105 105 / 50%);"
            loki="show"
            return render_template('/dashboard/WS_ODC.html',mensaje = mensaje, ubuntu=ubuntu,loki=loki)

    return render_template('/dashboard/WS_ODC.html')

###################WS Para ofertas de compra
    
@login_required
def dashboard_commodities():
    """Esta función renderiza y procesa los datos de la página templates/dashboard/commodities.html."""

    if request.method == "GET":


        return render_template("dashboard/commodities.html", fechas_max='2030-01-01')#VariablesGlobales.fechas_c_max)


    elif request.method == "POST" and validar_formulario(request):

        # Adquirir datos (fecha con formato dd/mm/aaaa)
        #fecha = date.fromisoformat(request.form["fecha"]).strftime("%d/%m/%Y")
        #fecha_2 = date.fromisoformat(request.form["fecha_2"]).strftime("%d/%m/%Y")

        # Hack temporal para sustituir el método .strftime("%d/%m/%Y")
        #------------------------------------------------------------#
        fecha = request.form["fecha"]
        fecha = fecha[8:10] + "/" + fecha[5:7] + "/" + fecha[0:4]
        fecha_2 = request.form["fecha_2"]
        fecha_2 = fecha_2[8:10] + "/" + fecha_2[5:7] + "/" + fecha_2[0:4]
        #------------------------------------------------------------#
        commodity = request.form["commodity"]
        intervalo = request.form["intervalo"]

        try:

            resultados, titulo_grafica, excel=pdM.obtener_commodities(fecha, fecha_2, commodity, intervalo)

            if request.form["boton_submit"] == "Consulta":

                excel_lista_global = excel
                etiquetas, dataset = graficaZoom("Commodities", resultados)
                print(resultados)

                return render_template("dashboard/z.html", title="Commodities", titulo_grafica=titulo_grafica, dataset=dataset, etiquetas=etiquetas)

            elif request.form["boton_submit"] == "Descarga":

                return descargar_excel(excel)

        except Exception:

            return render_template("dashboard/commodities.html", fechas_max='2030-01-01')#VariablesGlobales.fechas_c_max)
    else:

        return redirect(request.url)


# Se debe mejorar este codigo pues al insertar datos a un DataFrame en un ciclo el tiempo de carga es mucho mayor (usar listas en el ciclo en su lugar)
@login_required
def dashboard_webservice():
    """Esta función renderiza y procesa los datos de la página templates/dashboard/web_service.html."""

    if request.method == "GET":

        fecha_limite_1 = str(date.today())
        fechas_limite_superior = (fecha_limite_1,)

        return render_template("/dashboard/web_service.html", fechas_limite_superior=fechas_limite_superior)

    elif request.method == "POST":

        # Adquirir datos (fechas con formato yyyy-mm-dd)
        #fecha = date.fromisoformat(request.form["fecha"])
        #fecha_2 = date.fromisoformat(request.form["fecha_2"])
        fecha = request.form["fecha"]
        periodo = int(fecha[0:4])
        mes = int(fecha[5:7])
        dia = int(fecha[8:10])
        #print(dia)
        fecha = datetime.date(periodo, mes, dia)
        fecha_2 = request.form["fecha_2"]
        fecha_2 = datetime.date(int(fecha_2[0:4]), int(fecha_2[5:7]), int(fecha_2[8:10]))
        bd_tabla = request.form["bd_tabla"]

        # PML
        if bd_tabla == "PML":

            # Hack temporal para sustituir el método .strftime("%Y/%m/%d")
            #------------------------------------------------------------#
            tmp_fecha = str(fecha.year) + "/" + str(fecha.month) + "/" + str(fecha.day)
            #print(tmp_fecha)
            #------------------------------------------------------------#

            # Crear DataFrame vacío
            df = pd.DataFrame(columns=["nodo", "fecha", "hora", "pml", "pml_ene", "pml_per", "pml_cng"])

            # Adquirir valor del nodo
            nodos_lista = request.form.getlist("nodo")
            total_nodos = len(nodos_lista)
            nodos = ",".join(nodos_lista)
            cont = 0

            # Iterar rango de fechas
            while fecha <= fecha_2:

                # Hacer consulta (url, fechas con formato yyyy/mm/dd)
                #datos = requests.get("https://ws01.cenace.gob.mx:8082/SWPML/SIM/SIN/MDA/"+nodos+"/"+fecha.strftime("%Y/%m/%d")+"/"+fecha.strftime("%Y/%m/%d")+"/json")
                datos = requests.get("https://ws01.cenace.gob.mx:9082/SWPML/SIM/SIN/MDA/"+nodos+"/"+tmp_fecha+"/"+tmp_fecha+"/json")

                # Procesar información
                datos_json = datos.json()

                # Iterar nodos
                for pos in range(total_nodos):

                    # Contenido de nodo i
                    df_aux = datos_json["Resultados"][pos]["Valores"]
                    nodo_nombre = datos_json["Resultados"][pos]["clv_nodo"]

                    # Rango de tablas (para asignar nombre de los nodos)
                    rango_1 = 24 * (cont * total_nodos + pos) 
                    rango_2 = 24 * (cont * total_nodos + pos + 1)

                    # Agregar la información nueva a los datos existentes
                    df = df.append(df_aux, ignore_index=True)
                    df.iloc[rango_1:rango_2, 0] = nodo_nombre

                # Pasar al día siguiente
                fecha = fecha + timedelta(days=1)
                cont += 1

            # Renombrar correctamente las columnas
            df.rename(columns={df.columns[0]:"Nodo", df.columns[1]:"Fecha", df.columns[2]:"Hora", 
                               df.columns[3]:"PML", df.columns[4]:"PML energía", df.columns[5]:"PML pérdida", 
                               df.columns[6]:"PML congestión"}, inplace=True)

        # PMZ
        elif bd_tabla == "PMZ":

            # Hack temporal para sustituir el método .strftime("%Y/%m/%d")
            #------------------------------------------------------------#
            tmp_fecha = str(fecha.year) + "/" + str(fecha.month) + "/" + str(fecha.day)
            #print(tmp_fecha)
            #------------------------------------------------------------#

            # Crear DataFrame vacío
            df = pd.DataFrame(columns=["fecha", "hora", "pz", "pz_ene", "pz_per", "pz_cng"])

            # Adquirir valor de la zona
            zona = request.form["zona"]

            # Iterar rango de fechas
            while fecha <= fecha_2:

                # Hacer consulta (url)
                #datos = requests.get("https://ws01.cenace.gob.mx:8082/SWPEND/SIM/SIN/MDA/"+zona+"/"+fecha.strftime("%Y/%m/%d")+"/"+fecha.strftime("%Y/%m/%d")+"/json")
                datos = requests.get("https://ws01.cenace.gob.mx:8082/SWPEND/SIM/SIN/MDA/"+zona+"/"+tmp_fecha+"/"+tmp_fecha+"/json")

                # Procesar información
                datos_json = datos.json()
                df_aux = datos_json["Resultados"][0]["Valores"]

                # Agregar la información nueva a los datos existentes
                df = df.append(df_aux, ignore_index=True)

                # Pasar al día siguiente
                fecha = fecha + timedelta(days=1)

            # Renombrar correctamente las columnas
            df.rename(columns={df.columns[0]:"Fecha", df.columns[1]:"Hora", df.columns[2]:"PMZ", 
                               df.columns[3]:"PMZ energía", df.columns[4]:"PMZ pérdida", df.columns[5]:"PMZ congestión"}, inplace=True)

        resultados = df
        excel = {"nombre":"Web service - " + bd_tabla, "hojas":[{"titulo":bd_tabla, "dataframe":resultados}]}

        if request.form["boton_submit"] == "Consulta":

            return render_template("/datatable.html", title="Web service", resultados=resultados.to_html(classes=['table table-bordered dataTable" id = "dataTable']))

        elif request.form["boton_submit"] == "Descarga":

            return descargar_excel(excel)


#---------- Demanda ----------#
@login_required
def dashboard_demanda():
    """Esta función renderiza y procesa los datos de la página dashboard/demanda.html."""

    if request.method == "GET":

        #return render_template("/dashboard/demanda.html", fechas_limite_superior=VariablesGlobales.fechas_d_max)
        return render_template("/dashboard/demanda.html", fechas_limite_superior=0)

    elif request.method == "POST" and validar_formulario(request):
        
        fecha = request.form["fecha"]
        fecha_2 = request.form["fecha_2"]
        bd_tabla = request.form["bd_tabla"]

        cnxn = crear_conexion_SQL()

        with cnxn:

            # Demanda pronosticada
            if bd_tabla == "Demanda Pronosticada":

                consulta_asignada = request.form["consulta_asignada"]
                
                

            # Demanda maxima diaria MIM
            elif bd_tabla == "Demanda Real":
                consulta_asignada = request.form["consulta_asignada_2"]
               

            elif bd_tabla == "Demanda Maxima Diaria MIM":

                consulta_asignada = request.form["consulta_asignada_2"]
                
                

            elif bd_tabla == "Demanda Promedio Diaria MIM":

                consulta_asignada = request.form["consulta_asignada_2"]
                
                

            elif bd_tabla == "Demanda Maxima Instantanea CENACE":

                consulta_asignada = request.form["consulta_asignada_2"]

            excel, titulo_grafica, resultados, titulo_grafica, consulta_asignada=oE.demanda_pronosticada(fecha, fecha_2, bd_tabla, consulta_asignada)
            
            if request.form["boton_submit"] == "Consulta":

                global excel_lista_global
            
                excel_lista_global = excel

                etiquetas, dataset = graficaZoom("Demanda", resultados, consulta_asignada, bd_tabla)

                return render_template("dashboard/z.html", title="Demanda", dataset=dataset, titulo_grafica=titulo_grafica, etiquetas=etiquetas,consulta_asignada=consulta_asignada)

            elif request.form["boton_submit"] == "Descarga":

                return descargar_excel(excel)
    else:

        return redirect(request.url)


#---------- Ofertas de venta ----------#
@login_required
def dashboard_ofertas_de_venta():
    """Esta función renderiza la página templates/dashboard/ofertas_de_venta.html"""

    if request.method == "GET":

        #return render_template("/dashboard/ofertas_de_venta.html", fechas_limite_superior=VariablesGlobales.fechas_odv_max)
        return render_template("/dashboard/ofertas_de_venta.html", fechas_limite_superior=0)


    elif request.method == "POST":

        fecha = request.form["fecha"]
        fecha_2 = request.form["fecha_2"]
        bd_tabla = request.form["bd_tabla"]
        region = request.form["region"]
        consulta_asignada = request.form["consulta_asignada"]
        
        resultados, consulta_asignada, bd_tabla, titulo_grafica, excel = oE.ofertas_de_venta(fecha, fecha_2, bd_tabla, region, consulta_asignada)

        if request.form["boton_submit"] == "Consulta":

            global excel_lista_global
            
            excel_lista_global = excel

            etiquetas, dataset = graficaZoom("Ofertas de venta", resultados, consulta_asignada, bd_tabla)

            return render_template("dashboard/z.html", title="Ofertas de venta", titulo_grafica=titulo_grafica, dataset=dataset, etiquetas=etiquetas)

        elif request.form["boton_submit"] == "Descarga":

            return descargar_excel(excel)

    return render_template("dashboard/ofertas_de_venta.html")


@login_required
def dashboard_Medimem():
    """Renders the dashboard_SC page."""
    return render_template('dashboard/medimem.html', year=datetime.now().year)    


@login_required
def consulta_odc_db():
    """Renders the dashboard_SC page."""
    return render_template('dashboard/DB_ODC_Query.html', year=datetime.now().year)    


#@app.route('/Dashboard_Descargas_CSV', methods = ['POST','GET'])
def dashboard_descargas_excel_csv(wbs, filename, filetype=0):
    """Función de descargas en formato Excel y CSV"""
    
    if filetype == 0:
        for wb in wbs:
            wb.template = False
            wb.save(filename)
            #send_from_directory(app.config['UPLOAD_FOLDER'], filename, as_attachment=True)


@login_required
def dashboard_generacion_de_energia():
    """Esta función renderiza y procesa los datos de la página dashboard/generacion_de_energia.html."""

    if request.method == "GET":

        #return render_template("/dashboard/generacion_de_energia.html", fechas_limite_superior=VariablesGlobales.fechas_gde)
        return render_template("/dashboard/generacion_de_energia.html")
    
    elif request.method == "POST" and validar_formulario(request):

        fecha = request.form["fecha"]
        fecha_2 = request.form["fecha_2"]
        bd_tabla = request.form["bd_tabla"]
        region = ""
        
        cnxn = crear_conexion_SQL()
        
        

        # Energia generada por tipo de tecnologia
        if bd_tabla == "Energia generada por tipo de tecnologia":

            consulta_asignada = request.form["consulta_asignada"]

            

        # PRONÓSTICO DE GENERACIÓN INTERMITENTE (MDA)
        elif bd_tabla == "Pronostico de generacion intermitente (MDA)":

            consulta_asignada = request.form["consulta_asignada_2"]
            
            region = request.form["region"]
                
        excel, titulo_grafica, resultados=gE.generacion_de_energia(bd_tabla, consulta_asignada, fecha, fecha_2, region)
    
        if request.form["boton_submit"] == "Consulta":
            
            global excel_lista_global
                
            excel_lista_global = excel

            etiquetas, dataset = graficaZoom("Generacion de energia", resultados, consulta_asignada, bd_tabla)

            return render_template("dashboard/z.html", title="Generacion de energia", dataset=dataset, etiquetas=etiquetas, consulta_asignada=consulta_asignada)

        elif request.form["boton_submit"] == "Descarga":

            return descargar_excel(excel)
    else:

        return redirect(request.url)


#----- Energia asignada -----#
@login_required
def dashboard_energia_asignada():
    """Esta función renderiza y procesa los datos de la página dashboard/energia_asignada.html."""

    if request.method == "GET":

        #return render_template("/dashboard/energia_asignada.html", fechas_max=VariablesGlobales.fechas_ea_max)
        return render_template("/dashboard/energia_asignada.html")

    elif request.method == "POST" and validar_formulario(request):
    
        fecha = request.form["fecha"]
        fecha_2 = request.form["fecha_2"]
        consulta_asignada = request.form["consulta_asignada"]
     
        resultados, excel, titulo_grafica= gE.energia_asignada(consulta_asignada, fecha, fecha_2)

        if request.form["boton_submit"] == "Consulta":

            global excel_lista_global
                
            excel_lista_global = excel
            etiquetas, dataset = graficaZoom("Energia asignada", resultados, consulta_asignada)

            return render_template("dashboard/z.html", title="Energia asignada", titulo_grafica=titulo_grafica, dataset=dataset, etiquetas=etiquetas)

        elif request.form["boton_submit"] == "Descarga":

            return descargar_excel(excel)
    else:

        return redirect(request.url)


#---------- Ofertas de compra ----------#
@login_required
def dashboard_ofertas_de_compra():
    """Esta función renderiza y procesa los datos de la página dashboard/ofertas_de_compra.html."""

    if request.method == "GET":

        #if VariablesGlobales.fecha_global == False:

            # Test 1
            #datos = True
        
        #return render_template("/dashboard/ofertas_de_compra.html", fechas_limite_superior=VariablesGlobales.fechas_odc_max)
        return render_template("/dashboard/ofertas_de_compra.html")

        #else:

            #VariablesGlobales.fecha_global == False


            #return render_template("/dashboard/ofertas_de_compra.html", fecha_test="2021-04-19")

    elif request.method == "POST":
    
        # Adquirir datos (fecha con formato yyyy-mm-dd)
        fecha = request.form["fecha"]
        fecha_2 = request.form["fecha_2"]
        bd_tabla = request.form["bd_tabla"]
        region=''

        if bd_tabla == "Oferta de compra":
            consulta_asignada = request.form["consulta_asignada"]
            region = request.form["region"]
        else:
            consulta_asignada = request.form["consulta_asignada_2"]


        excel, resultados, consulta_asignada, bd_tabla, titulo_grafica=oE.oferta_de_compra(fecha, fecha_2, bd_tabla, region, consulta_asignada)
        if request.form["boton_submit"] == "Consulta":

            global excel_lista_global
                
            excel_lista_global = excel

            etiquetas, dataset = graficaZoom("Ofertas de compra", resultados, consulta_asignada, bd_tabla)

            return render_template("dashboard/z.html", title="Ofertas de compra", titulo_grafica=titulo_grafica, dataset=dataset, etiquetas=etiquetas)


        elif request.form["boton_submit"] == "Descarga":

            return descargar_excel(excel)


#---------- Servicios conexos ----------#
@login_required
def dashboard_servicios_conexos():
    """Esta función renderiza y procesa los datos de la página dashboard/servicios_conexos.html."""

    if request.method == "GET":
        return render_template("/dashboard/servicios_conexos.html", fechas_limite_superior=0)



    elif request.method == "POST" and validar_formulario(request):

        fecha = request.form["fecha"]
        fecha_2 = request.form["fecha_2"]
        bd_tabla = request.form["bd_tabla"]
        region = request.form["region"]
        consulta_asignada = request.form["consulta_asignada"]

        excel, resultados, consulta_asignada, bd_tabla, titulo_grafica, title=pdM.obtener_servicios_conexos(fecha, fecha_2, bd_tabla, region, consulta_asignada)
        if request.form["boton_submit"] == "Consulta":

            global excel_lista_global
                
            excel_lista_global = excel

            etiquetas, dataset = graficaZoom("Servicios conexos", resultados, consulta_asignada, bd_tabla)
            
            return render_template("dashboard/z.html", title=title, titulo_grafica=titulo_grafica, 
                                   dataset=dataset, etiquetas=etiquetas)

        elif request.form["boton_submit"] == "Descarga":

            return descargar_excel(excel)
    else:

        return redirect(request.url)


################MEDIMEM
@login_required
def dashboard_Medimem_querys():
     
    fecha = request.form['fecha']
    fecha_2 = request.form['fecha_2']
    cliente = request.form['cliente']
    if cliente=='Zubex':

        equipo = request.form['lista_equipos']
        excel,resultados=cinc.medimem_consulta(fecha, fecha_2, cliente,equipo)
    if cliente=='Fandeli':
        equipo = request.form['lista_equipos2']
        excel,resultados=cinc.medimem_consulta(fecha, fecha_2, cliente, equipo)
    if cliente=='Urrea':
        excel,resultados=cinc.medimem_consulta(fecha, fecha_2, cliente)
    
    

    if request.form["boton_submit"] == "Consulta":

        global excel_lista_global
                
        excel_lista_global = excel

        labels, dataset = formato_grafica("Precios de mercado", resultados)

        return render_template("/datatable.html", title="Cincominutales", labels=labels, dataset=dataset)

    elif request.form["boton_submit"] == "Descarga":

        return descargar_excel(excel)


@login_required
def dashboard_medimem_btmnn():

    if request.method == "GET":
        return render_template("/dashboard/medimem_btmnn.html")

    else:
        fecha = request.form['fecha']
        fecha_2 = request.form['fecha_2']
        cliente = request.form['cliente']

        if cliente=='Zubex':

            equipo = request.form['lista_equipos']
            df, excel=cinc.medimem_btmn(fecha,fecha_2, cliente, equipo)
        if cliente=='Fandeli':
            equipo = request.form['lista_equipos2']
            df, excel=cinc.medimem_btmn(fecha,fecha_2, cliente, equipo)
        if cliente=='Urrea':
            df, excel=cinc.medimem_btmn(fecha,fecha_2, cliente)


        if request.form["boton_submit"] == "Consulta":

            global excel_lista_global
                    
            excel_lista_global = excel

            labels, dataset = formato_grafica("Precios de mercado", df)

            return render_template("/datatable.html", title="Cincominutales Beetmann", labels=labels, dataset=dataset)

        elif request.form["boton_submit"] == "Descarga":

            return descargar_excel(excel)


@login_required
def consumo_cliente_beetmann():

    if request.method == "GET":
        return render_template("/dashboard/consulta_consumo_cliente.html")

    else:
        fecha = request.form['fecha']
        fecha_2 = request.form['fecha_2']
        cliente = current_user.get_name()
        print(cliente)
        if cliente=='Zubex':

            equipo = request.form['lista_equipos']
            df, excel=cinc.medimem_cliente(fecha,fecha_2, cliente, equipo)
        if cliente=='Fandeli':
            equipo = request.form['lista_equipos2']
            df, excel=cinc.medimem_cliente(fecha,fecha_2, cliente, equipo)
        if cliente=='Urrea':
            df, excel=cinc.medimem_cliente(fecha,fecha_2, cliente, equipo = 0)

        if request.form["boton_submit"] == "Consulta":

            global excel_lista_global
                    
            excel_lista_global = excel

            labels, dataset = formato_grafica("Precios de mercado", df)

            return render_template("/datatable.html", title="Consumo " + cliente  , labels=labels, dataset=dataset)

        elif request.form["boton_submit"] == "Descarga":

            return descargar_excel(excel)
        elif request.form["boton_submit"] == "Calcular":
            fechas=request.form.get('arreglo_fechas')
            fechas=fechas.split(',')
            if cliente=='Urrea':
                equipo = 0
                print("urrrreea")
            
            VariablesGlobales.caso_odc = 0

            valores_horas,Total,dataFrame_tabla_promedio, etiquetas, dataset = cinc.medimem_btmn_Fechas(fechas, cliente,equipo)

            return render_template('/dashboard/Grafica_Consumos.html',etiquetas=etiquetas,dataset=dataset, ODC=dataFrame_tabla_promedio.to_html(classes=['table table-bordered dataTable" id = "dataTable2']))



######################INFORMACION CLIENTES FIRMADOS###################
#############################################################
######################################################################


@login_required
def dashboard_clientes_firmados():

    if request.method == "GET":
        return render_template("/dashboard/clientes_firmados.html")

    else :
        fecha = request.form['fecha']
        fecha_2 = request.form['fecha_2']
        cliente = request.form['cliente']
        consulta = request.form['tipo_consulta']

        if consulta == 'ConsumoDemandas':
            df, excel=factur.consumo_demanda(fecha,fecha_2, cliente)
            precio_cobertura,costo_cobertura,desv_cobertura,LP,L_P_SQ_actual_month = 0,0,0,0,0
            titulo = ('CONSUMOS - DEMANDAS' , cliente)
            estilo = "display:none"
        if consulta == 'Precios':
            df, excel=factur.precios(fecha,fecha_2, cliente)
            precio_cobertura,costo_cobertura,desv_cobertura,LP,L_P_SQ_actual_month = 0,0,0,0,0
            titulo = ('Precios' , cliente)
            estilo = "display:none"
        if consulta == 'Cobertura_Eolica':

            df, excel ,precio_cobertura,costo_cobertura,desv_cobertura,LP,L_P_SQ_actual_month=factur.riesgo_cobertura_eolica(fecha,fecha_2, cliente)
            precio_cobertura = "${:,.2f}".format(precio_cobertura)
            costo_cobertura = "${:,.2f}".format(costo_cobertura)
            desv_cobertura = "${:,.2f}".format(desv_cobertura)
            LP = "${:,.2f}".format(LP)
            L_P_SQ_actual_month = "${:,.2f}".format(L_P_SQ_actual_month)
            estilo = "display:block"


            titulo = ('Cobertura Eolica' , cliente)


        if request.form["boton_submit"] == "Consulta":

            global excel_lista_global
                    
            excel_lista_global = excel

            labels, dataset = formato_grafica("Precios de mercado", df)

            return render_template("/datatable2.html", title=titulo, labels=labels, dataset=dataset,
                                  Precio_Medio =precio_cobertura, Costo_Medio = costo_cobertura, Desv = desv_cobertura, LP = LP,
                                  L_P_SQ_actual_month = L_P_SQ_actual_month,estilo = estilo)

        elif request.form["boton_submit"] == "Descarga":

            return descargar_excel(excel)

##############################################################################



#@app.route("/Tarfia_CFE_Consulta", methods=["GET", "POST"])
@login_required
#def dashboard_Tarifa_CFE_querys():
def dashboard_Tarifa_CFE():

    if request.method == "GET":

        return render_template("/dashboard/Tarifa_cfe.html", fechas_max='2030-01-01',#VariablesGlobales.fechas_t_max, 
                                                                fechas_min='2018-01-01')#VariablesGlobales.fechas_t_min)

    elif request.method == "POST" and validar_formulario(request):
        Metodo=request.form['Metodos']
        if Metodo=='Rango de fechas':

            Tarifa = request.form["bd_tabla"]
            fecha = request.form["fecha"]
            fecha = fecha + "-01"
            fecha_2 = request.form["fecha_2"]
            fecha_2 = fecha_2 + "-01"
            region = request.form["region"]

            cnxn = crear_conexion_SQL()
                
            with cnxn:

                if Tarifa == "GDMTH":

                    consulta_sql = """
                                    SELECT * 
                                    FROM [dbo].[Tarifa GDMTH] 
                                    WHERE fecha >= '{fecha}' AND fecha <= '{fecha_2}'  
                                    and region = '{region}'
                                    ORDER BY fecha ASC
                                    """
                
                if Tarifa == "DIST":

                    consulta_sql = """
                                    SELECT * 
                                    FROM [dbo].[Tarifa DIST] 
                                    WHERE fecha >= '{fecha}' AND fecha <= '{fecha_2}' 
                                    and region = '{region}'
                                    ORDER BY fecha ASC
                                    """

                consulta_sql = """
                                SELECT [Región]
                              ,[Fecha] as 'fecha'
                              ,[Precio Base]
                              ,[Precio Intermedia]
                              ,[Precio Punta]
                              ,[Capacidad]
                              ,[Distribución]
                              ,[Fijo]
                                FROM [dbo].[Tarifa GDMTH] 
                                WHERE fecha >= '{fecha}' AND fecha <= '{fecha_2}'  
                                and región = '{region}'
                                ORDER BY fecha ASC
                                """
            
            consulta_sql = consulta_sql.format(region=region, fecha=fecha, fecha_2=fecha_2)
            resultados = pd.read_sql(consulta_sql, cnxn)
            excel = {"nombre": ("Tarifa " + Tarifa), "hojas": [{"titulo": Tarifa, "dataframe": resultados}]}
            titulo_grafica = Tarifa + " " + region

            if request.form["boton_submit"] == "Consulta":
                consulta_sql = """
                                SELECT [Región]
                                ,[Fecha] as 'fecha'
                                ,[Precio Base]
                                ,[Precio Intermedia]
                                ,[Precio Punta]
                                ,[Capacidad]
                                ,[Distribución]
                                ,[Fijo]
                                FROM [dbo].[Tarifa GDMTO] 
                                WHERE fecha >= '{fecha}' AND fecha <= '{fecha_2}' 
                                and región = '{region}'
                                ORDER BY fecha ASC
                                """

                global excel_lista_global
                        
                excel_lista_global = excel
                etiquetas, dataset = graficaZoom("Tarifas CFE", resultados)

                return render_template("dashboard/z.html", title="Tarifas CFE", titulo_grafica=titulo_grafica, dataset=dataset, etiquetas=etiquetas)

            elif request.form["boton_submit"] == "Descarga":

                return descargar_excel(excel)

        elif Metodo=='Individual':

            Tarifa = request.form["bd_tabla"]
            meses=request.form.get('arreglo_meses')
            meses=meses.split(',')
            region = request.form["region"]
            
            meses=[x + '-01' for x in meses]
            
            
            cnxn=crear_conexion_SQL()
            dataFrame_final=pd.DataFrame()

            consulta_sql = """
                                SELECT * 
                                FROM [dbo].[Tarifa DIT] 
                                WHERE fecha >= '{fecha}' AND fecha <= '{fecha_2}' 
                                and region = '{region}'
                                ORDER BY fecha ASC
                                """
        
            consulta_sql = consulta_sql.format(region=region, fecha=fecha, fecha_2=fecha_2)
            resultados = pd.read_sql(consulta_sql, cnxn)
            excel = {"nombre": ("Tarifa " + Tarifa), "hojas": [{"titulo": Tarifa, "dataframe": resultados}]}
            titulo_grafica = Tarifa + " " + region

            consulta_sql = """
                                    SELECT * 
                                    FROM [dbo].[Tarifa GDMTH] 
                                    WHERE Región = '{region}' and (fecha = '{fecha}')
                                    ORDER BY fecha asc
                                    """
                
            if Tarifa == "GDMTO":

                consulta_sql = """
                                SELECT * 
                                FROM [dbo].[Tarifa GDMTO] 
                                WHERE Región = '{region}' and (fecha = '{fecha}')
                                ORDER BY fecha asc
                                """

            if Tarifa == "DIST":

                consulta_sql = """
                                SELECT * 
                                FROM [dbo].[Tarifa DIST] 
                                WHERE Región = '{region}' and (fecha = '{fecha}')
                                ORDER BY fecha asc
                                """
            
            if Tarifa == "DIT":

                consulta_sql = """
                                SELECT * 
                                FROM [dbo].[Tarifa DIT] 
                                WHERE Región = '{region}' and (fecha = '{fecha}')
                                ORDER BY fecha asc
                                """
            consulta_sql = consulta_sql.format(region=region, fecha=x)
            resultados = pd.read_sql(consulta_sql, cnxn)
            dataFrame_final=pd.concat([dataFrame_final, resultados], axis=0)
        
        titulo_grafica = Tarifa + " " + region
        etiquetas, dataset = graficaZoom("Tarifas CFE", dataFrame_final)
        

        return render_template("dashboard/z.html", title="Tarifas CFE", titulo_grafica=titulo_grafica, dataset=dataset, etiquetas=etiquetas)


    else:

        return redirect(request.url)


@login_required
def dashboard_ODB_DB_querys():
     
    fecha = request.form["fecha"]
    fecha_2 = request.form["fecha_2"]
    cliente = request.form["cliente"]

    excel, resultados=odc.consultar(fecha, fecha_2, cliente)

    if request.form["boton_submit"] == "Consulta":

        global excel_lista_global
                
        excel_lista_global = excel


        return render_template("/datatable.html",
                               cobertura= resultados.to_html(classes=[ "records_list table table-striped table-bordered table-hover", "mydatatable"], justify='center', table_id='mydatatable'))

    elif request.form["boton_submit"] == "Descarga":

        return descargar_excel(excel)


@login_required
def repositorio(archivo=None):

    if request.method == "GET" and not archivo:

        ruta = "python_webapp_flask/files/repository"
        lista_archivos = os.listdir(ruta)
        lista_pdf = []

        for archivo in lista_archivos:

            if os.path.isfile(os.path.join(ruta, archivo)) and archivo.endswith(".pdf"):

                lista_pdf.append(archivo)

        return render_template("repositorio.html", resultados=lista_pdf)

    elif archivo:

        #directorio = os.path.join(app.root_path, app.config["UPLOAD_FOLDER"]) # Concatenar ruta de la app y del folder de archivos (ruta absoluta)
        directorio = "files/repository"
        
        return send_from_directory(directory=directorio, filename=archivo)


# No eliminar esta funcion
#@app.route("/re/<path:nombre>", methods=["GET", "POST"])
#@login_required
def re():

    #print("Algo")

    # Test 2 (ultima opcion)
    #global VariablesGlobales.fecha_global

    VariablesGlobales.fecha_global = True

    #print(request.values)
    #print(request.url)

    return "Variable"
    #return render_template("/dashboard/ofertas_de_compra.html", )
    #return redirect(url_for("dashboard_ofertas_de_compra"))
    #return redirect(request.url)redirect
    #return redirect(url_for(redirect))
    
    
def chart():

    return render_template("dashboard/z.html")



def graficaZoom(encabezado, df, consulta_asignada=None, bd_tabla=None, tipo_de_datos=None):

    nombres_columnas_a_evaluar = ["Fecha"]
    dict_datos_columnas_a_evaluar = {}
    datos_multiples = False

    #print(encabezado)
    #print(df)
    #print(df.columns)
    #for i in dataframe_to_rows(df):
        #print(i)


    if encabezado == "Precios de mercado":

        if tipo_de_datos == "Todos":

            if consulta_asignada ==  "Datos":

                columna_fechas = df["Fecha"]
                nombre_columna_datos_multiples = df.columns[2]
                #print(nombre_columna_zonas)
                #print(df[nombre_columna_zonas])
                lista_horas = df["Hora"].map(int)
                columna_fechas = [val+timedelta(hours=int(lista_horas.iloc[pos])-1) for pos, val in enumerate(columna_fechas)]
                dict_datos_columnas_a_evaluar["Fecha"] = columna_fechas
                datos_multiples = True
            
                #print("Eurecka!!!!")

            elif consulta_asignada ==  "Promedio 24h (diario)":
            
                columna_fechas = df["Año"]
                lista_anios = df["Año"].map(int)
                lista_meses = df["Mes"].map(int)
                lista_dias = df["Día"].map(int)
                lista_horas = df["Hora"].map(int)
                columna_fechas = [datetime(lista_anios.iloc[pos], lista_meses.iloc[pos], lista_dias.iloc[pos])+timedelta(hours=int(lista_horas.iloc[pos])-1) for pos, val in enumerate(columna_fechas)]
                dict_datos_columnas_a_evaluar["Fecha"] = columna_fechas

            elif consulta_asignada ==  "Promedio 24h (mensual)":

                columna_fechas = df["Año"]
                lista_anios = df["Año"].map(int)
                lista_meses = df["Mes"].map(int)
                lista_horas = df["Hora"].map(int)
                columna_fechas = [datetime(lista_anios.iloc[pos], lista_meses.iloc[pos], 1)+timedelta(hours=int(lista_horas.iloc[pos])-1) for pos, val in enumerate(columna_fechas)]
                dict_datos_columnas_a_evaluar["Fecha"] = columna_fechas

            elif consulta_asignada ==  "Promedio diario":

                columna_fechas = df["Año"]
                lista_anios = df["Año"].map(int)
                lista_meses = df["Mes"].map(int)
                lista_dias = df["Día"].map(int)
                columna_fechas = [datetime(lista_anios.iloc[pos], lista_meses.iloc[pos], lista_dias.iloc[pos]) for pos, val in enumerate(columna_fechas)]
                dict_datos_columnas_a_evaluar["Fecha"] = columna_fechas

            elif consulta_asignada == "Promedio mensual":

                columna_fechas = df["Año"]
                lista_anios = df["Año"].map(int)
                lista_meses = df["Mes"].map(int)
                columna_fechas = [datetime(lista_anios.iloc[pos], lista_meses.iloc[pos], 1) for pos, val in enumerate(columna_fechas)]
                dict_datos_columnas_a_evaluar["Fecha"] = columna_fechas

        elif tipo_de_datos == "Individual":

            if consulta_asignada ==  "Datos":
            
                columna_fechas = df["Fecha"]
                lista_horas = df["Hora"].map(int)
                columna_fechas = [val+timedelta(hours=int(lista_horas.iloc[pos])-1) for pos, val in enumerate(columna_fechas)]
                dict_datos_columnas_a_evaluar["Fecha"] = columna_fechas

            elif consulta_asignada ==  "Promedio 24h (diario)":
            
                columna_fechas = df["Año"]
                lista_anios = df["Año"].map(int)
                lista_meses = df["Mes"].map(int)
                lista_dias = df["Día"].map(int)
                lista_horas = df["Hora"].map(int)
                columna_fechas = [datetime(lista_anios.iloc[pos], lista_meses.iloc[pos], lista_dias.iloc[pos])+timedelta(hours=int(lista_horas.iloc[pos])-1) for pos, val in enumerate(columna_fechas)]
                dict_datos_columnas_a_evaluar["Fecha"] = columna_fechas

            elif consulta_asignada ==  "Promedio 24h (mensual)":

                columna_fechas = df["Año"]
                lista_anios = df["Año"].map(int)
                lista_meses = df["Mes"].map(int)
                lista_horas = df["Hora"].map(int)
                columna_fechas = [datetime(lista_anios.iloc[pos], lista_meses.iloc[pos], 1)+timedelta(hours=int(lista_horas.iloc[pos])-1) for pos, val in enumerate(columna_fechas)]
                dict_datos_columnas_a_evaluar["Fecha"] = columna_fechas

            elif consulta_asignada ==  "Promedio diario":

                columna_fechas = df["Año"]
                lista_anios = df["Año"].map(int)
                lista_meses = df["Mes"].map(int)
                lista_dias = df["Día"].map(int)
                columna_fechas = [datetime(lista_anios.iloc[pos], lista_meses.iloc[pos], lista_dias.iloc[pos]) for pos, val in enumerate(columna_fechas)]
                dict_datos_columnas_a_evaluar["Fecha"] = columna_fechas

            elif consulta_asignada == "Promedio mensual":

                columna_fechas = df["Año"]
                lista_anios = df["Año"].map(int)
                lista_meses = df["Mes"].map(int)
                columna_fechas = [datetime(lista_anios.iloc[pos], lista_meses.iloc[pos], 1) for pos, val in enumerate(columna_fechas)]
                dict_datos_columnas_a_evaluar["Fecha"] = columna_fechas

        else:

            columna_fechas = df["Fecha"]
            lista_horas = df["Hora"].map(int)
            columna_fechas = [val+timedelta(hours=int(lista_horas.iloc[pos])-1) for pos, val in enumerate(columna_fechas)]
            dict_datos_columnas_a_evaluar["Fecha"] = columna_fechas

    elif encabezado == "Servicios conexos":
        if consulta_asignada ==  "Datos":
                columna_fechas = df["Fecha"]
                lista_horas = df["Hora"].map(int)
                columna_fechas = [val+timedelta(hours=int(lista_horas.iloc[pos])-1) for pos, val in enumerate(columna_fechas)]
                dict_datos_columnas_a_evaluar["Fecha"] = columna_fechas
                print(df)
        elif consulta_asignada ==  "Promedio horario mensual por reserva":
                columna_fechas = df["Año"]
                lista_anios = df["Año"].map(int)
                lista_meses = df["Mes"].map(int)
                nombre_columna_datos_multiples = df.columns[2]
                #print(nombre_columna_zonas)
                #print(df[nombre_columna_zonas])
                columna_fechas = [datetime(lista_anios.iloc[pos], lista_meses.iloc[pos], 1) for pos, val in enumerate(columna_fechas)]
                dict_datos_columnas_a_evaluar["Fecha"] = columna_fechas
        elif consulta_asignada ==  "Promedio 24 hrs mensual":

                columna_fechas = df["Año"]
                lista_anios = df["Año"].map(int)
                lista_meses = df["Mes"].map(int)
                lista_horas = df["Hora"].map(int)
                columna_fechas = [datetime(lista_anios.iloc[pos], lista_meses.iloc[pos],1)+timedelta(hours=int(lista_horas.iloc[pos])-1) for pos, val in enumerate(columna_fechas)]
                dict_datos_columnas_a_evaluar["Fecha"] = columna_fechas

    elif encabezado == "Demanda":

        if bd_tabla == "Demanda Pronosticada":

            if consulta_asignada == "Datos":

                # Pendiente
                pass

            elif consulta_asignada ==  "Promedio unitario horario semanal":

                # Pendiente
                pass

                columna_fechas = df["Año"]
                lista_anios = df["Año"].map(int)
                lista_meses = df["Mes"].map(int)
                lista_semanas = df["Semana"].map(int)
                columna_fechas = [datetime(lista_anios.iloc[pos], 1, 1)+timedelta(days=7*int(lista_semanas.iloc[pos])) for pos, val in enumerate(columna_fechas)]
                dict_datos_columnas_a_evaluar["Fecha"] = columna_fechas

            elif consulta_asignada ==  "Promedio diario de la demanda total horaria":

                columna_fechas = df["Año"]
                lista_anios = df["Año"].map(int)
                lista_meses = df["mes"].map(int)
                lista_dias = df["dia"].map(int)
                columna_fechas = [datetime(lista_anios.iloc[pos], lista_meses.iloc[pos], lista_dias.iloc[pos]) for pos, val in enumerate(columna_fechas)]
                dict_datos_columnas_a_evaluar["Fecha"] = columna_fechas


            elif consulta_asignada ==  "Promedio horario mensual":

                columna_fechas = df["Año"]
                lista_anios = df["Año"].map(int)
                lista_meses = df["Mes"].map(int)
                columna_fechas = [datetime(lista_anios.iloc[pos], lista_meses.iloc[pos], 1) for pos, val in enumerate(columna_fechas)]
                dict_datos_columnas_a_evaluar["Fecha"] = columna_fechas

            elif consulta_asignada ==  "Promedio diario mensual":

                columna_fechas = df["Año"]
                lista_anios = df["Año"].map(int)
                lista_meses = df["Mes"].map(int)
                columna_fechas = [datetime(lista_anios.iloc[pos], lista_meses.iloc[pos], 1) for pos, val in enumerate(columna_fechas)]
                dict_datos_columnas_a_evaluar["Fecha"] = columna_fechas

            elif consulta_asignada ==  "Promedio diario mensual por area":
                
                columna_fechas = df["Año"]
                lista_anios = df["Año"].map(int)
                lista_meses = df["Mes"].map(int)
                columna_fechas = [datetime(lista_anios.iloc[pos], lista_meses.iloc[pos], 1) for pos, val in enumerate(columna_fechas)]
                dict_datos_columnas_a_evaluar["Fecha"] = columna_fechas

            elif consulta_asignada ==  "Promedio horario mensual por area":
                columna_fechas = df["Año"]
                lista_anios = df["Año"].map(int)
                lista_meses = df["Mes"].map(int)
                columna_fechas = [datetime(lista_anios.iloc[pos], lista_meses.iloc[pos], 1) for pos, val in enumerate(columna_fechas)]
                dict_datos_columnas_a_evaluar["Fecha"] = columna_fechas
            elif consulta_asignada ==  "Promedio unitario horario mensual":

                columna_fechas = df["Año"]
                lista_anios = df["Año"].map(int)
                lista_meses = df["Mes"].map(int)
                columna_fechas = [datetime(lista_anios.iloc[pos], lista_meses.iloc[pos], 1) for pos, val in enumerate(columna_fechas)]
                dict_datos_columnas_a_evaluar["Fecha"] = columna_fechas

            elif consulta_asignada ==  "Promedio horario semanal por area":

                # Pendiente
                pass

            elif consulta_asignada ==  "Promedio horario semanal total":

                # Pendiente
                pass

        #elif bd_tabla == "Demanda Maxima Diaria MIM":
        else:

            if consulta_asignada ==  "Promedio semanal":

                # Pendiente
                pass

            if consulta_asignada ==  "Promedio mensual":

                columna_fechas = df["Año"]
                lista_anios = df["Año"].map(int)
                lista_meses = df["Mes"].map(int)
                columna_fechas = [datetime(lista_anios.iloc[pos], lista_meses.iloc[pos], 1) for pos, val in enumerate(columna_fechas)]
                dict_datos_columnas_a_evaluar["Fecha"] = columna_fechas

    elif encabezado == "Ofertas de venta":

        if consulta_asignada == "Datos":

            # Pendiente
            pass

        elif consulta_asignada == "Promedio horario mensual por un generador":

            columna_fechas = df["Año"]
            lista_anios = df["Año"].map(int)
            lista_meses = df["Mes"].map(int)
            columna_fechas = [datetime(lista_anios.iloc[pos], lista_meses.iloc[pos], 1) for pos, val in enumerate(columna_fechas)]
            dict_datos_columnas_a_evaluar["Fecha"] = columna_fechas

        elif consulta_asignada == "Promedio horario mensual por todos los generadores":

                columna_fechas = df["Año"]
                lista_anios = df["Año"].map(int)
                lista_meses = df["Mes"].map(int)
                columna_fechas = [datetime(lista_anios.iloc[pos], lista_meses.iloc[pos], 1) for pos, val in enumerate(columna_fechas)]
                dict_datos_columnas_a_evaluar["Fecha"] = columna_fechas

    elif encabezado == "Commodities":

        dict_datos_columnas_a_evaluar["Fecha"] = df["Date"].tolist()

    elif encabezado == "Energia asignada":

        if consulta_asignada == "Datos":

            #Pendiente
            pass

        elif consulta_asignada == "Promedio horario mensual por una zona de carga":
        
            columna_fechas = df["Año"]
            lista_anios = df["Año"].map(int)
            lista_meses = df["Mes"].map(int)
            columna_fechas = [datetime(lista_anios.iloc[pos], lista_meses.iloc[pos], 1) for pos, val in enumerate(columna_fechas)]
            dict_datos_columnas_a_evaluar["Fecha"] = columna_fechas

        elif consulta_asignada == "Promedio horario mensual por todas las zonas de carga":

            columna_fechas = df["Año"]
            lista_anios = df["Año"].map(int)
            lista_meses = df["Mes"].map(int)
            columna_fechas = [datetime(lista_anios.iloc[pos], lista_meses.iloc[pos], 1) for pos, val in enumerate(columna_fechas)]
            dict_datos_columnas_a_evaluar["Fecha"] = columna_fechas

        elif consulta_asignada == "Promedio horario mensual por zona":

            #Pendiente
            pass

        elif consulta_asignada == "Promedio diario mensual por zona":

            #Pendiente
            pass

        elif consulta_asignada == "Sumatoria mensual por zona":

            #Pendiente
            pass

    elif encabezado == "Generacion de energia":

        if consulta_asignada == "Datos":

            pass

        elif consulta_asignada == "Promedio 24 hrs diario":

            columna_fechas = df["Año"]
            lista_anios = df["Año"].map(int)
            lista_meses = df["Mes"].map(int)
            lista_dias = df["Día"].map(int)
            lista_horas = df["Hora"].map(int)
            columna_fechas = [datetime(lista_anios.iloc[pos], lista_meses.iloc[pos], lista_dias.iloc[pos])+timedelta(hours=int(lista_horas.iloc[pos])-1) for pos in range(len(columna_fechas.index))]
            dict_datos_columnas_a_evaluar["Fecha"] = columna_fechas

        elif consulta_asignada == "Promedio 24 hrs mensual":

            columna_fechas = df["Año"]
            lista_anios = df["Año"].map(int)
            lista_meses = df["Mes"].map(int)
            lista_horas = df["Hora"].map(int)
            columna_fechas = [(datetime(lista_anios.iloc[pos], lista_meses.iloc[pos], 1)+timedelta(hours=int(lista_horas.iloc[pos])-1)) for pos in range(len(columna_fechas.index))]
            dict_datos_columnas_a_evaluar["Fecha"] = columna_fechas

        elif consulta_asignada == "Promedio horario mensual":

            columna_fechas = df["Año"]
            lista_anios = df["Año"].map(int)
            lista_meses = df["Mes"].map(int)
            columna_fechas = [datetime(lista_anios.iloc[pos], lista_meses.iloc[pos], 1) for pos in range(len(columna_fechas.index))]
            dict_datos_columnas_a_evaluar["Fecha"] = columna_fechas

        elif consulta_asignada == "Promedio diario mensual por tecnologia":

            columna_fechas = df["Año"]
            lista_anios = df["Año"].map(int)
            lista_meses = df["Mes"].map(int)
            columna_fechas = [datetime(lista_anios.iloc[pos], lista_meses.iloc[pos], 1) for pos in range(len(columna_fechas.index))]
            dict_datos_columnas_a_evaluar["Fecha"] = columna_fechas

        elif consulta_asignada == "Promedio horario mensual de generacion total":

            columna_fechas = df["Año"]
            lista_anios = df["Año"].map(int)
            lista_meses = df["Mes"].map(int)
            columna_fechas = [datetime(lista_anios.iloc[pos], lista_meses.iloc[pos], 1) for pos in range(len(columna_fechas.index))]
            dict_datos_columnas_a_evaluar["Fecha"] = columna_fechas

        elif consulta_asignada == "Suma mensual de generacion total":

            columna_fechas = df["Año"]
            lista_anios = df["Año"].map(int)
            lista_meses = df["Mes"].map(int)
            columna_fechas = [datetime(lista_anios.iloc[pos], lista_meses.iloc[pos], 1) for pos in range(len(columna_fechas.index))]
            dict_datos_columnas_a_evaluar["Fecha"] = columna_fechas

        elif consulta_asignada == "Suma mensual por tecnologia":

            columna_fechas = df["Año"]
            lista_anios = df["Año"].map(int)
            lista_meses = df["Mes"].map(int)
            columna_fechas = [datetime(lista_anios.iloc[pos], lista_meses.iloc[pos], 1) for pos in range(len(columna_fechas.index))]
            dict_datos_columnas_a_evaluar["Fecha"] = columna_fechas

    elif encabezado == "Tarifas CFE":

        columna_fechas = df["fecha"]
        columna_fechas = [val for val in columna_fechas]
        dict_datos_columnas_a_evaluar["Fecha"] = columna_fechas

    elif encabezado == "Ofertas de compra":

        if consulta_asignada == "Promedio mensual":

            columna_fechas = df["Año"]
            lista_anios = df["Año"].map(int)
            lista_meses = df["Mes"].map(int)
            columna_fechas = [datetime(lista_anios.iloc[pos], lista_meses.iloc[pos], 1) for pos, val in enumerate(columna_fechas)]
            dict_datos_columnas_a_evaluar["Fecha"] = columna_fechas
        elif consulta_asignada == "Promedio total horario mensual":

            columna_fechas = df["Año"]
            lista_anios = df["Año"].map(int)
            lista_meses = df["Mes"].map(int)
            columna_fechas = [datetime(lista_anios.iloc[pos], lista_meses.iloc[pos], 1) for pos, val in enumerate(columna_fechas)]
            dict_datos_columnas_a_evaluar["Fecha"] = columna_fechas

      
    
    lista_columnas = df.columns

    for pos, col in enumerate(lista_columnas):

        nombre_col = col.lower()

        if (df[col].dtypes == "float64" or df[col].dtypes == "int64") and nombre_col != "fecha" and nombre_col != "año" and nombre_col != "mes" and nombre_col != "día" and nombre_col != "hora" and nombre_col != "semana":

            #if fecha_especifica:

                #col = 'str' + col.astype(str)
                #print("RENAME")
                #df.rename(columns = {list(df)[pos]:'new_name' + col}, inplace=True)

            nombres_columnas_a_evaluar.append(col)

    for i in nombres_columnas_a_evaluar[1:len(nombres_columnas_a_evaluar)]:
        dict_datos_columnas_a_evaluar[i] = df[i]

    #print("jnni")
    #print(dict_datos_columnas_a_evaluar)
    df_columnas_a_evaluar = pd.DataFrame(data=dict_datos_columnas_a_evaluar)
    #print(df_columnas_a_evaluar)
    lista_final = [{nombres_columnas_a_evaluar[u]: df_columnas_a_evaluar.iloc[pos][nombre_col] for u, nombre_col in enumerate(nombres_columnas_a_evaluar)} for pos in range(len(df_columnas_a_evaluar.index))]
    #print(lista_final)
    #print(nombres_columnas_a_evaluar)
    #for i in lista_final:
        #print(i)
    #lista_final = [{"x": new Date(2020, 07, 01, 20), "y": 14}, {"x": new Date(2020, 07, 01, 21), "y": 12.5}]
    #lista_final = [{"x": 10, "y": 14}, {"x": 12, "y": 30}]
    #print(lista_final)
    for i in lista_final:
        print(i)

    return nombres_columnas_a_evaluar, lista_final


def fromdate(x):
    y=dateutil.parser.parse(x).date() #-----------------------------------------------------------------------------------#
    return (y)

def formMoneda(x):
    return'${:,.2f}'.format(x)
    
def formInt(x):
    return'{:,.0f}'.format(x)

def formPorcen(x):
    return'{:,.2f}%'.format(x)

def formx(x):
    return "${:.1f}K".format(x/1000)

def formMW(x):
    return'{:,.2f}MW'.format(x)

def listToString(s): 
    
    x = ",".join(s)

    return x



@login_required
def dashboard_Rentabilidad():

    if request.method == "GET":

        nodos_sin, nodos_bca, nodos_bcs, zonas_sin, zonas_bca, zonas_bcs = pdM.obtener_nodos()


        return render_template("/dashboard/rentabilidad.html",nodos_sin=nodos_sin, nodos_bca=nodos_bca, nodos_bcs=nodos_bcs, 
                                                                        zonas_sin=zonas_sin, zonas_bca=zonas_bca, zonas_bcs=zonas_bcs)

    else:
        fecha = request.form['fecha']
        fecha_2 = request.form['fecha_2']
        consulta = request.form['bd_tabla']

        if consulta == "Zona":
            region = request.form['zona']

        

        dfzonas , dfnodos = rentability.get_rentabilidad(fecha,fecha_2, consulta,region)

        if request.form["boton_submit"] == "Consulta":

            dfzonas= dfzonas.to_html(classes=["table table-bordered table-stripped table-data"])
            dfnodos= dfnodos.to_html(classes=["table table-bordered table-stripped table-data"])


            return render_template("/rentabilidad_tablas.html" ,dfzonas =dfzonas ,dfnodos = dfnodos , region = region)
            


           

        


    return render_template("/dashboard/rentabilidad.html")

import pdfplumber
@login_required
def upload_file_cfe():
    if request.method == "GET":
        return render_template("/dashboard/formulario.html")
    if request.method == "POST":
        consumoBase2 = []
        consumoIntermedio2 = []
        consumoPunta2 = []
        kv2 = []
        consumoMaxBase2 = []
        consumoMaxIntermedio2 = []
        consumoMaxPunta2 = []
        subtotal2 = []
        total2 = []
        names2 = []
        address2 = []
        files = []
        date2 = []
        date2_limite_pago = []
        tarifa2 = []
        suministro2 = []
        distribucion2 = []
        transmision2 = []
        cenace2 = []
        genB2 = []
        genI2 = []
        genP2 = []
        capacidad2 = []
        s2 = []
        fijo2 = []
        energia2 = []
        bonificacion2 = []
        iva2 = []
        facturacion2 = []
        adeudo2 = []
        pago2 = []
        servicio2 = []


        #a = request.files.getlist('files')
        #folder = dirname(a[0].filename)
        #print(a)

        #os.chdir(folder)
        for file in request.files.getlist('files'):
            #if (file == ('application/pdf')):
            files.append(file)

            with pdfplumber.open(file) as pdf:
                consumoBase = []
                consumoIntermedio = []
                consumoPunta = []
                kv = []
                consumoMaxBase = []
                consumoMaxIntermedio = []
                consumoMaxPunta = []
                subtotal = []
                total = []
                names = []
                address = []
                new_ad2 =[]
                date =[]
                date_limite_pago=[]
                df = []
                tarifas = []
                suministros = []
                distribuciones = []
                transmisiones = []
                cenaces = []
                genB = []
                genI = []
                genP = []
                capacidades = []
                s = []
                fijos = []
                energias = []
                bonificaciones = []
                ivas = []
                facturaciones = []
                adeudos = []
                pagos = []
                servicios = []
                pos = 0

                for page in pdf.pages:
                    pages = pdf.pages[0]
                    data = pages.extract_text()
                    d = data.split('\n')
                    for i in d:
                        f = i.split(' ')
                        df.append(f)
                        #print(df)

                for i in df:
                    for j in i:
                        if (j == "TARIFA:"):
                            pos = i.index(j)
                            tarifa = i[pos + 1]
                            tarifas.append(tarifa)
                        
                        if (j == "Fijo³") or (j == "Fijo(³)"):
                            pos = i.index(j)
                            fijo = i[pos + 1]
                            fijos.append(fijo)
                            
                            
                        if (j == "Energía"):
                            pos = i.index(j)
                            energia = i[pos + 1]
                            energias.append(energia)
                        
                        if (j == "Potencia³"):
                            pos = i.index(j)
                            potencia = i[pos + 1]
                            bonificaciones.append(potencia)
                        
                        if (j == "16%"):
                            pos = i.index(j)
                            iva = i[pos + 1]
                            ivas.append(iva)
                        
                        if (j == "Periodo"):
                            pos = i.index(j)
                            fac = i[pos + 1]
                            facturaciones.append(fac)
                        
                        if (j == "Anterior"):
                            pos = i.index(j)
                            adeudo = i[pos + 1]
                            adeudos.append(adeudo)
                        
                        
                        if(j == "Su"):
                            pos = i.index(j)
                            if(i[pos + 1] == "Pago"):
                                pos = i.index(j)
                                pagado = i[pos + 2]
                                pagos.append(pagado)
                        
                        if(j == "SERVICIO"):
                            pos = i.index(j)
                            if(i[pos + 1] == ":"):
                                pos = i.index(j)
                                servicio = i[pos + 2]
                                servicios.append(servicio)

                            
                        if (j == "TOTAL"):
                            pos = i.index(j)
                            for j in range(0, pos):
                                name = i[j]
                                names.append(name)
                            if(pos==0):
                                pos = df.index(i)
                                for m in df[0]:
                                    names.append(m)
                                

                        if(j == "NO."):
                            new_ad =[]
                            star=0
                            end=0
                            pos = i.index(j)
                            if(i[pos + 1] == "DE"):
                                pos = df.index(i)
                                for m in range(2, pos):
                                    address.append(' '.join(df[m]))
                                ad = ' '.join(address) 
                                for a in ad:
                                    if(a=="("):
                                        star=ad.index(a)
                                    if(a==")"):
                                        end= ad.index(a)
                                for r in range(0,star):
                                    new_ad.append(ad[r])
                                for u in range((end+1),(int(len(ad)/2))):
                                    new_ad.append(ad[u])
                                    new_ad2.append(''.join(new_ad))
                        
                        
                        if(j == "LÍMITE"):
                            pos = i.index(j)
                            de = i[pos + 1]
                            pago = i[pos + 2]
                            numero = pago.split(':')
                            mes = i[pos + 3]
                            ano = i[pos + 4]
                            if(mes == "ENE"):
                                mes = "DIC"
                                ano = str(int(ano)-1)                        
                            elif(mes == "FEB"):
                                mes = "ENE"
                            elif(mes == "MAR"):
                                mes = "FEB"
                            elif(mes == "ABR"):
                                mes = "MAR"
                            elif(mes == "MAY"):
                                mes = "ABR"
                            elif(mes == "JUN"):
                                mes = "MAY"
                            elif(mes == "JUL"):
                                mes = "JUN"
                            elif(mes == "AGO"):
                                mes = "JUL"
                            elif(mes == "SEP"):
                                mes = "AGO"
                            elif(mes == "OCT"):
                                mes = "SEP"
                            elif(mes == "NOV"):
                                mes = "OCT"
                            elif(mes == "DIC"):
                                mes = "NOV"    
                            date.append(mes)
                            date.append(ano)


                                                
                        if(j == "LÍMITE"):
                            pos = i.index(j)
                            de = i[pos + 1]
                            pago = i[pos + 2]
                            numero = pago.split(':')
                            date_limite_pago.append(numero[1])
                            mes = i[pos + 3]
                            ano = i[pos + 4] 
                            date_limite_pago.append(mes)
                            date_limite_pago.append(ano)


                        if(j == "Suministro"):
                            pos = i.index(j)
                            suministro = i[pos + 4]
                            suministros.append(suministro)
                        

                        if(j == "Distribución"):
                            pos = i.index(j)
                            distribucion = i[pos + 4]
                            distribuciones.append(distribucion)

                        if(j == "Transmisión"):
                            pos = i.index(j)
                            transmision = i[pos + 4]
                            transmisiones.append(transmision)
                            

                        if(j == "CENACE"):
                            pos = i.index(j)
                            cenace = i[pos + 4]
                            cenaces.append(cenace)

                        if(j == "Generación"):
                            pos = i.index(j)
                            if(i[pos + 1] == "B"):
                                pos = i.index(j)
                                B = i[pos + 5]
                                genB.append(B)

                            if(i[pos + 1] == "I"):
                                pos = i.index(j)
                                I = i[pos + 5]
                                genI.append(I)

                        
                            if(i[pos + 1] == "P"):
                                pos = i.index(j)
                                P = i[pos + 5]
                                genP.append(P)
                            

                        if(j == "Capacidad"):
                            pos = i.index(j)
                            capacidad = i[pos + 4]
                            capacidades.append(capacidad)

                        if(j == "SCnMEM(¹)"):
                            pos = i.index(j)
                            sc = i[pos + 4]
                            s.append(sc)

                        if(j == "kWh"):
                            pos = i.index(j)
                            if(i[pos + 1] == "base"):
                                pos = i.index(j)
                                kWhBase = i[pos + 2]
                                consumoBase.append(kWhBase)

                            if(i[pos + 1] == "intermedia"):
                                pos = i.index(j)
                                kWhIntermedia = i[pos + 2]
                                consumoIntermedio.append(kWhIntermedia)

                            if(i[pos + 1] == "punta"):
                                pos = i.index(j)
                                kWhPunta = i[pos + 2]
                                consumoPunta.append(kWhPunta)
                        
                        if(j == "kW"):
                            pos = i.index(j)
                            if(i[pos + 1] == "base"):
                                pos = i.index(j)
                                kWBase = i[pos + 2]
                                consumoMaxBase.append(kWBase)

                            if(i[pos + 1] == "intermedia"):
                                pos = i.index(j)
                                kWIntermedia = i[pos + 2]
                                consumoMaxIntermedio.append(kWIntermedia)

                        
                            if(i[pos + 1] == "punta"):
                                pos = i.index(j)
                                kWPunta = i[pos + 2]
                                consumoMaxPunta.append(kWPunta)
                                
                        if (j == "kVArh"):
                            pos = i.index(j)
                            kVArh = i[pos + 1]
                            kv.append(kVArh)
                        
                        if (j == "Subtotal"):
                            pos = i.index(j)
                            Subtotal = i[pos + 1]
                            subtotal.append(Subtotal)
                            
                        if (j == "Total"):
                            pos = i.index(j)
                            Total = i[-1]
                            total.append(Total)

                if(len(date) == 0):
                    date.append('NaN')
                else:
                    new_date = []
                    lim = int(len(date)/2)
                    for i in range(0,lim):
                        new_date.append(date[i])
                    new_da =' '.join(new_date)
                    date2.append(new_da)


                if(len(date_limite_pago) == 0):
                    date.append('NaN')
                else:
                    new_date = []
                    lim = int(len(date_limite_pago)/2)
                    for i in range(0,lim):
                        new_date.append(date_limite_pago[i])
                    new_da =' '.join(new_date)
                    date2_limite_pago.append(new_da)

                if(len(new_ad2)!=0):
                    address2.append(new_ad2[len(new_ad2)-1])
                if(len(new_ad2)==0):
                    address2.append('NaN')

                if(len(consumoBase) == 0):
                    consumoBase.append('NaN')
                else:
                    consumoBase.pop(0)
                
                if(len(consumoIntermedio) == 0):
                    consumoIntermedio.append('NaN')
                else:
                    consumoIntermedio.pop(0)  

                if(len(consumoPunta) == 0):
                    consumoPunta.append('NaN')
                else:
                    consumoPunta.pop(0)  

                if(len(kv) == 0):
                    kv.append('NaN')
                else:
                    kv.pop(0)  

                if(len(consumoMaxBase) == 0):
                    consumoMaxBase.append('NaN')
                else:
                    consumoMaxBase.pop(0)  

                if(len(consumoMaxIntermedio) == 0):
                    consumoMaxIntermedio.append('NaN')
                else:
                    consumoMaxIntermedio.pop(0)  
                    
                if(len(consumoMaxPunta) == 0):
                    consumoMaxPunta.append('NaN')
                else:
                    consumoMaxPunta.pop(0)

                if(len(subtotal) == 0):
                    subtotal.append('NaN')
                else:
                    subtotal.pop(0)  

                if(len(total) == 0):
                    total.append('NaN')
                else:
                    for i in range(2):
                        total.pop(i) 
                    total.pop(0)

                if(len(names) == 0):
                    names.append('NaN')
                else:
                    new_names = []
                    lim = int(len(names)/2)
                    for i in range(0,lim):
                        new_names.append(names[i])
                    new_name =' '.join(new_names)
                    names2.append(new_name)
                if(len(tarifas) == 0):
                    tarifas.append('NaN')
                else:
                    tarifas.pop(0)  
                if(len(suministros) == 0):
                    suministros.append('NaN')
                else:
                    suministros.pop(0)  
                if(len(distribuciones) == 0):
                    distribuciones.append('NaN')
                else:
                    distribuciones.pop(0)  
                if(len(transmisiones) == 0):
                    transmisiones.append('NaN')
                else:
                    transmisiones.pop(0)  
                if(len(cenaces) == 0):
                    cenaces.append('NaN')
                else:
                    cenaces.pop(0)  
                if(len(genB) == 0):
                    genB.append('NaN')
                else:
                    genB.pop(0)  
                if(len(genI) == 0):
                    genI.append('NaN')
                else:
                    genI.pop(0)  
                if(len(genP) == 0):
                    genP.append('NaN')
                else:
                    genP.pop(0)  
                if(len(capacidades) == 0):
                    capacidades.append('NaN')
                else:
                    capacidades.pop(0)  
                if(len(s) == 0):
                    s.append('NaN')
                else:
                    s.pop(0)  
                if(len(fijos) == 0):
                    fijos.append('NaN')
                else:
                    fijos.pop(0)  
                if(len(energias) == 0):
                    energias.append('NaN')
                else:
                    energias.pop(0)  
                if(len(bonificaciones) == 0):
                    bonificaciones.append('NaN')
                else:
                    bonificaciones.pop(0)  
                if(len(ivas) == 0):
                    ivas.append('NaN')
                else:
                    ivas.pop(0)  
                if(len(facturaciones) == 0):
                    facturaciones.append('NaN')
                else:
                    facturaciones.pop(0)  
                if(len(adeudos) == 0):
                    adeudos.append('NaN')
                else:
                    adeudos.pop(0)  
                if(len(pagos) == 0):
                    pagos.append('NaN')
                else:
                    pagos.pop(0)  
                if(len(servicios) == 0):
                    servicios.append('NaN')
                else:
                    servicios.pop(0)  
                
            
                for n in consumoBase:
                    consumoBase2.append(float(n.replace(',','')))
                for n in consumoIntermedio:
                    consumoIntermedio2.append(float(n.replace(',','')))
                for n in consumoPunta:
                    consumoPunta2.append(float(n.replace(',','')))
                for n in kv:
                    kv2.append(float(n.replace(',','')))
                for n in consumoMaxBase:
                    consumoMaxBase2.append(float(n.replace(',','')))
                for n in consumoMaxIntermedio:
                    consumoMaxIntermedio2.append(float(n.replace(',','')))
                for n in consumoMaxPunta:
                    consumoMaxPunta2.append(float(n.replace(',','')))
                for n in subtotal:
                    subtotal2.append(float(n.replace(',','')))
                for n in total:
                    new_total = n.replace(',','')
                    total2.append(float(new_total.replace('$','')))
                for n in tarifas:
                    tarifa2.append(n.replace(',',''))
                for n in suministros:
                    suministro2.append(float(n.replace(',','')))
                for n in distribuciones:
                    distribucion2.append(float(n.replace(',','')))
                for n in transmisiones:
                    transmision2.append(float(n.replace(',','')))
                for n in cenaces:
                    cenace2.append(float(n.replace(',','')))
                for n in genB:
                    genB2.append(float(n.replace(',','')))
                for n in genI:
                    genI2.append(float(n.replace(',','')))
                for n in genP:
                    genP2.append(float(n.replace(',','')))
                for n in capacidades:
                    capacidad2.append(float(n.replace(',','')))
                for n in s:
                    s2.append(float(n.replace(',','')))
                for n in fijos:
                    fijo2.append(float(n.replace(',','')))
                for n in energias:
                    energia2.append(float(n.replace(',','')))
                for n in bonificaciones:
                    bonificacion2.append(n.replace(',',''))
                for n in ivas:
                    iva2.append(float(n.replace(',','')))
                for n in facturaciones:
                    facturacion2.append(float(n.replace(',','')))
                for n in adeudos:
                    adeudo2.append(float(n.replace(',','')))
                for n in pagos:
                    pago2.append(n.replace(',',''))
                for n in servicios:
                    servicio2.append(n.replace(',',''))
                
                    
            # print(len(consumoBase2))
            # print(len(consumoIntermedio2))
            # print(len(consumoPunta2))
            # print(len(kv2))
            # print(len(consumoMaxBase2))
            # print(len(consumoMaxIntermedio2))
            # print(len(consumoMaxPunta2))
            # print(len(subtotal2))
            # print(len(total2))
            # print(len(suministro2))
            # print(len(distribucion2))
            # print(len(transmision2))
            # print(len(cenace2))
            # print(len(genB2))
            # print(len(genI2))
            # print(len(genP2))
            # print(len(capacidad2))
            # print(len(s2))
                

        dataFrame2 = pd.DataFrame(
            {'CFE': files,
            'Nombre': names2,
            'Direccion': address2,
            'Fecha': date2,
            'Fecha limite de pago': date2_limite_pago,
            'No. De Servicio': servicio2,
            'Tarifa': tarifa2,
            'Consumo Base': consumoBase2,
            'Consumo Intermedio': consumoIntermedio2,
            'Consumo Punta': consumoPunta2,
            'kVArh': kv2,
            'Demanda máxima sbase': consumoMaxBase2,
            'Demanda máxima intermedia': consumoMaxIntermedio2,
            'Demanda máxima punta': consumoMaxPunta2,
            'Subtotal': subtotal2,
            'Total': total2,
            'Suministro': suministro2,
            'Distribución': distribucion2,
            'Transmisión': transmision2,
            'CENACE': cenace2,
            'Generación B': genB2,
            'Generación I': genI2,
            'Generación P': genP2,
            'Capacidad': capacidad2,
            'SCnMEM(¹)': s2,
            'Fijo³': fijo2,
            'Energía': energia2,
            'Bonificador de Potencia': bonificacion2,
            'IVA 16%': iva2,
            'Facturación': facturacion2,
            'Adeudo Anterior': adeudo2,
            'Su Pago': pago2,
            
            
            }).set_index('CFE')

    cfeExcel = {"nombre":  "INPUTS_" + str(servicio2[0]), "hojas": [{"titulo": "_" + str(servicio2[0]), "dataframe": dataFrame2}]}
    #print(cfeExcel)
    return descargar_excel(cfeExcel)
