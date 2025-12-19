import json
import pandas as pd
import os
from typing import Dict, Any
from .logger import manejar_error, LoggerPersonalizado

logger = LoggerPersonalizado().get_logger()

class CargadorDatos:
    """Clase para cargar datos transformados"""
    
    def __init__(self):
        self.formatos_soportados = ['csv', 'json', 'parquet', 'excel']
#este constructor inicializa una lista de formatos de archivo soportados para la carga de datos.
#es una lista que contiene las extensiones de archivo que la clase CargadorDatos puede manejar al guardar datos.

    @manejar_error
    def guardar_como_json(self, df: pd.DataFrame, nombre_archivo: str):
        """
        Guarda DataFrame como JSON
        
        Args:
            df: DataFrame a guardar
            nombre_archivo: Nombre del archivo (sin extensión)
        """
        # Crear directorio si no existe, exist_ok=True evita error si ya existe
        os.makedirs("data/processed", exist_ok=True)
        
        ruta = f"data/processed/{nombre_archivo}.json" # Construye la ruta completa del archivo JSON donde se guardarán los datos.
        
        # Convertir a diccionario orientado a registros
        datos_json = df.to_dict(orient='records') # Convierte el DataFrame de pandas (df) a una lista de diccionarios, donde cada diccionario representa una fila del DataFrame. El parámetro orient='records' especifica este formato.
                                                    #.to_dict es un método de pandas que convierte un DataFrame en varios formatos de diccionario. y orient='records' indica que cada fila del DataFrame se convertirá en un diccionario separado dentro de una lista.
                                                    #orient=record , usando los nombres de las columnas como claves, lo que es ideal para trabajar con datos fila por fila o para generar formatos como JSON. 

        # Guardar con formato legible, abre ruta y escribe el diccionario de json a formato json en el archivo
        with open(ruta, 'w', encoding='utf-8') as f: # with open(ruta, 'w', encoding='utf-8') as f: abre el archivo en la ruta especificada (ruta) en modo de escritura ('w') con codificación UTF-8. El uso de with asegura que el archivo se cierre correctamente después de escribir en él.
            json.dump(datos_json, f, indent=2, ensure_ascii=False) # json.dump(datos_json, f, indent=2, ensure_ascii=False) escribe la lista de diccionarios (datos_json) en el archivo abierto (f) en formato JSON. El parámetro indent=2 hace que el JSON sea más legible al agregar sangría de 2 espacios, y ensure_ascii=False permite que los caracteres no ASCII se escriban correctamente en el archivo.
                                                                    # json.dump es una función del módulo json de Python que convierte un objeto de Python (en este caso, una lista de diccionarios) en una cadena JSON y la escribe en un archivo. y ascii false permite que los caracteres especiales se guarden correctamente en el archivo JSON. si fuera true, todos los caracteres no ASCII se escaparían, lo que puede dificultar la lectura de idiomas con caracteres especiales.
        
        logger.info(f"Datos guardados como JSON en: {ruta}") # Registra un mensaje informativo indicando que los datos se han guardado correctamente como JSON y muestra la ruta del archivo donde se almacenaron.
        logger.info(f"Total registros guardados: {len(datos_json)}") # Registra un mensaje informativo indicando el total de registros guardados en el archivo JSON.
        
        return ruta # Devuelve la ruta del archivo JSON donde se guardaron los datos.

#json.dump:
# Serialización: Convierte estructuras de datos de Python (diccionarios, listas) a su representación en formato JSON (JavaScript Object Notation), un formato ligero y legible por humanos para intercambio de datos.
# Escritura en archivo: Toma el objeto Python y el objeto archivo como argumentos, y vuelca los datos JSON en ese archivo.
# Persistencia de datos: Permite guardar el estado de tu aplicación o datos de forma estructurada para uso futuro. 


    @manejar_error
    def guardar_como_csv(self, df: pd.DataFrame, nombre_archivo: str): # se le pasan dos parámetros: df (el DataFrame de pandas que se desea guardar) y nombre_archivo (el nombre que se le dará al archivo CSV, sin la extensión).
        """Guarda DataFrame como CSV"""                             #en este caso self se refiere a la instancia de la clase CargadorDatos y se pone como convención en Python para métodos dentro de clases pero realmente no se usa dentro del método.
        os.makedirs("data/processed", exist_ok=True) #crea la carpeta data/processed si no existe. exist_ok=True evita que se lance un error si la carpeta ya existe.
        
        ruta = f"data/processed/{nombre_archivo}.csv" # construye la ruta completa del archivo CSV donde se guardarán los datos. nombre_archivo es el nombre proporcionado para el archivo, y se le añade la extensión .csv. ese nombre se obtiene al llamar a la función.
        df.to_csv(ruta, index=False, encoding='utf-8') # df.to_csv(ruta, index=False, encoding='utf-8') guarda el DataFrame (df) como un archivo CSV en la ruta especificada (ruta). El parámetro index=False asegura que los índices del DataFrame no se guarden como una columna adicional en el archivo CSV. encoding='utf-8' garantiza que los caracteres especiales se manejen correctamente al escribir en el archivo.
        
        logger.info(f"Datos guardados como CSV en: {ruta}") # registra un mensaje informativo indicando que los datos se han guardado correctamente como CSV y muestra la ruta del archivo donde se almacenaron.
        return ruta

#de donde proviene el nombre del archivo? Viene del parámetro nombre_archivo que se pasa a la función guardar_como_csv cuando se llama.

    @manejar_error
    def guardar_como_excel(self, df: pd.DataFrame, nombre_archivo: str):
        """Guarda DataFrame como Excel"""
        os.makedirs("data/processed", exist_ok=True) #crea la carpeta data/processed si no existe.
        
        ruta = f"data/processed/{nombre_archivo}.xlsx" # construye la ruta completa del archivo Excel donde se guardarán los datos.
        
        # Crear un Excel writer con pandas
        with pd.ExcelWriter(ruta, engine='openpyxl') as writer: # pd.ExcelWriter crea un objeto que permite escribir DataFrames de pandas en archivos Excel. El parámetro engine='openpyxl' especifica que se utilizará la biblioteca openpyxl para manejar archivos .xlsx. with asegura que el archivo se cierre correctamente después de escribir en él.
            df.to_excel(writer, sheet_name='Datos', index=False) # df.to_excel(writer, sheet_name='Datos', index=False) escribe el DataFrame (df) en una hoja llamada 'Datos' dentro del archivo Excel. El parámetro index=False asegura que los índices del DataFrame no se guarden como una columna adicional en la hoja de Excel. writer es el objeto ExcelWriter que maneja la escritura en el archivo Excel.
            
            # Opcional: agregar un resumen
            resumen = pd.DataFrame({ #pd.DataFrame crea un nuevo DataFrame de pandas que contiene un resumen de los datos guardados.
                'Métrica': ['Total Filas', 'Total Columnas', 'Fecha Generación'], #'Métrica' es una columna que describe las métricas del resumen: el total de filas, el total de columnas y la fecha de generación del archivo.
                'Valor': [len(df), len(df.columns), pd.Timestamp.now()] #'Valor' es otra columna que contiene los valores correspondientes a cada métrica: el número de filas (len(df)), el número de columnas (len(df.columns)) y la fecha y hora actuales (pd.Timestamp.now()).
            })
            resumen.to_excel(writer, sheet_name='Resumen', index=False) # escribe el DataFrame de resumen en una hoja llamada 'Resumen' dentro del mismo archivo Excel. index=False asegura que los índices del DataFrame de resumen no se guarden como una columna adicional.
        
        logger.info(f"Datos guardados como Excel en: {ruta}")
        return ruta # Devuelve la ruta del archivo Excel donde se guardaron los datos.

#eta funcion es la que maneja el guardado en multiples formatos al llamar a las otras tres funciones.
    @manejar_error
    def guardar_multiple_formatos(self, df: pd.DataFrame, nombre_base: str):
        """Guarda en múltiples formatos"""
        rutas = {} # Diccionario para almacenar las rutas de los archivos guardados
        
        for formato in ['csv', 'json', 'excel']: # Itera sobre una lista de formatos deseados (csv, json, excel).Esta lista se obtiene de forma estática, pero podría modificarse para aceptar formatos dinámicos según las necesidades.
            if formato == 'csv':  #si el formato es 'csv', llama al método guardar_como_csv y almacena la ruta devuelta en el diccionario rutas bajo la clave 'csv'.
                rutas['csv'] = self.guardar_como_csv(df, nombre_base) #entonces este metodod llama al método guardar_como_csv pasando el DataFrame (df) y el nombre base (nombre_base) como argumentos., la funcion que llama crea la carpeta, la ruta y guarda el archivo. y devuelve la ruta del archivo guardado. y esta lo lamcxena en el diccionario rutas con la clave 'csv'.
            elif formato == 'json': 
                rutas['json'] = self.guardar_como_json(df, nombre_base) #la ruta que devulve la funcion se almacena en el diccionario rutas bajo la clave 'json' y asi con todos los formatos.
            elif formato == 'excel':
                rutas['excel'] = self.guardar_como_excel(df, nombre_base)
        
        return rutas # Devuelve el diccionario rutas que contiene las rutas de los archivos guardados en los diferentes formatos.
#el método guardar_multiple_formatos es útil cuando se desea guardar los mismos datos en varios formatos para diferentes propósitos o audiencias, asegurando flexibilidad en el acceso y uso de los datos almacenados.
#si usara rutas.items en main.py podria iterar sobre las rutas devueltas y mostrar o procesar cada archivo guardado según sea necesario. ya que rutas es un diccionario donde las claves son los formatos de archivo y los valores son las rutas correspondientes a los archivos guardados.