import pandas as pd
import requests
from typing import Union, Dict, Any
# El módulo typing se usa para añadir anotaciones de tipo (o "type hints") al código. Estas anotaciones no cambian cómo funciona el programa cuando se ejecuta, pero sirven para dos propósitos vitales:
# Documentación y Legibilidad: Hacen que el código sea mucho más claro para otros programadores (¡o para ti mismo en el futuro!). Indican claramente qué espera una función como entrada y qué tipo de dato devolverá.
from .logger import manejar_error, LoggerPersonalizado

logger = LoggerPersonalizado().get_logger() #se obtiene el logger personalizado para registrar eventos en este módulo.
# esto es una inyección de dependencia, donde se crea una instancia del logger personalizado para ser utilizado en el módulo extractor.py.

class ExtractorDatos:
    """Clase para extraer datos de diversas fuentes."""
    def __init__(self):None
# el método __init__ es el constructor de la clase ExtractorDatos. En este caso, no realiza ninguna inicialización específica.
#Dentro del constructor, esta línea inicializa una variable de instancia (un atributo)
#  llamada datos_extraidos y le asigna el valor None. 
# Esto significa que cada nuevo objeto ExtractorDatos comienza con un contenedor vacío para los datos que eventualmente extraerá.

#typing se muestra aqui url: str: Le dice al lector que la variable url debe ser una cadena de texto (string)

    @manejar_error #se aplica el decorador manejar_error a la función descargar_csv_publico para agregar manejo de errores y logging automáticamente.
    def descargar_csv_publico(self, url: str) -> pd.DataFrame: #self es una referencia a la instancia actual de la clase ExtractorDatos. url es un parámetro que representa la URL desde donde se descargará el archivo CSV. -> pd.DataFrame indica que esta función devolverá un objeto DataFrame de pandas.
        """
        Descarga un CSV desde una URL pública
        
        Args:
            url: URL del CSV
            
        Returns:
            DataFrame de pandas
        """
        logger.info(f"Descargando CSV desde: {url}") #Registra un mensaje informativo antes de intentar la descarga del CSV.
        
        # Opción 1: Desde URL directa
        try:
            df = pd.read_csv(url) #intenta usar pd.read_csv(url) para leer directamente el CSV desde la URL proporcionada y almacenarlo en un DataFrame llamado df.
            logger.info(f"CSV descargado. Filas: {len(df)}, Columnas: {len(df.columns)}")
            self.datos_extraidos = df #si tien exito, asigna el DataFrame df al atributo datos_extraidos de la instancia actual.
            return df #devuelve el DataFrame df.
        except Exception as e:
            logger.warning(f"No se pudo descargar desde URL: {e}")
            
            # Opción 2: Para datos locales de respaldo
            logger.info("Usando datos de respaldo locales...") #si la descarga directa falla, archivo no existe o url esta caida, registra un mensaje informativo indicando que se usará un conjunto de datos de respaldo local.
            # Datos de ejemplo # se registra u na advertencia y luego crea un DataFrame de ejemplo con datos ficticios. Para que el programa siga funcionando sin depender de la red.
            datos_ejemplo = {
                'id': [1, 2, 3, 4, 5],
                'nombre': ['Juan Pérez', 'María García', 'Pedro López', None, 'Ana Martínez'],
                'edad': [25, 30, None, 40, 35],
                'ciudad': ['Madrid', 'Barcelona', 'Madrid', 'Valencia', 'Sevilla'],
                'salario': [30000, 35000, 28000, 42000, 32000],
                'fecha_ingreso': ['2020-01-15', '2019-03-20', None, '2018-06-10', '2021-09-05']
            }
            df = pd.DataFrame(datos_ejemplo) #crea un DataFrame de pandas a partir del diccionario datos_ejemplo. con pd.DataFrame(datos_ejemplo)
            return df #devuelve el DataFrame df creado con los datos de ejemplo.
#Las claves del diccionario se convierten en los nombres de las columnas del DataFrame.
#Los valores asociados a cada clave (que deben ser listas o arrays de igual longitud) se convierten en los datos de esas columnas. 



#Su propósito es leer datos que ya están almacenados en el disco duro local, en lugar de descargarlos de internet como hacía el método anterior.
#Función: Envuelve automáticamente la función leer_archivo_local. Si ocurre algún problema durante la ejecución de esta función (por ejemplo, si la ruta del archivo es incorrecta, si el archivo está dañado, o si el tipo de codificación es erróneo), el decorador captura el error, lo registra usando el logger y evita que el programa se detenga bruscamente.
# self: Es la referencia a la instancia actual de la clase.
# ruta: str: Espera un argumento ruta que debe ser una cadena de texto (ej. "./datos/mi_archivo.csv").
# tipo: str = 'csv': Espera un argumento tipo, que también es una cadena de texto. Tiene un valor por defecto de 'csv', lo que significa que si no especificas el tipo al llamar la función, asumirá que es un CSV.
# -> pd.DataFrame: Indica que el método devolverá un DataFrame de Pandas.
    @manejar_error
    def leer_archivo_local(self, ruta: str, tipo: str = 'csv') -> pd.DataFrame:
        """
        Lee archivos locales
        
        Args:
            ruta: Ruta del archivo
            tipo: Tipo de archivo (csv, json, excel)
            
        Returns:
            DataFrame de pandas
        """
        logger.info(f"Leyendo archivo {tipo} desde: {ruta}")
        
        if tipo == 'csv':
            df = pd.read_csv(ruta, encoding='utf-8')
        elif tipo == 'json':
            df = pd.read_json(ruta)
        elif tipo == 'excel':
            df = pd.read_excel(ruta, engine='openpyxl') #Pandas necesita el motor 'openpyxl' para manejar archivos .xlsx
        else:
            raise ValueError(f"Tipo de archivo no soportado: {tipo}") #Si el tipo especificado no es ninguno de los anteriores (ej. alguien pasa "pdf"), lanza un error (raise ValueError) indicando que el tipo de archivo no está soportado.
        
        logger.info(f"Archivo leído. Filas: {len(df)}, Columnas: {len(df.columns)}")
        self.datos_extraidos = df
        return 
#Almacena el DataFrame resultante en el atributo de la instancia self.datos_extraidos, asegurándose de que los datos estén disponibles para otros métodos de la clase más tarde.
#Devuelve el DataFrame (return df).
    

#Su propósito es tomar los datos que se acaban de extraer (o cualquier DataFrame que le pases) y guardarlos en el sistema de archivos local en un formato consistente, típicamente como un archivo CSV simple, en una carpeta específica.
# self: Referencia a la instancia de la clase.
# df: pd.DataFrame: Este método espera un argumento df que debe ser un DataFrame de Pandas. Estos son los datos que se van a guardar.
# nombre: str = "datos_raw": Espera un argumento nombre que es una cadena de texto. Si no se proporciona un nombre al llamar a la función, usará "datos_raw" por defecto.

# def guardar_raw(self, df: pd.DataFrame, nombre: str = "datos_raw"):
#         """Guarda los datos extraídos en formato raw"""
#         ruta = f"data/raw/{nombre}.csv" #Construye la ruta completa del archivo donde se guardarán los datos. Usa una carpeta llamada data/raw y el nombre proporcionado para el archivo, con extensión .csv.
#         df.to_csv(ruta, index=False) #Esta es la función central de pandas. Toma el DataFrame (df) y lo convierte en un archivo CSV, ruta le dice a pandas donde guardar el archivo. El parámetro index=False asegura que los índices del DataFrame no se guarden como una columna adicional en el archivo CSV.
#         logger.info(f"Datos raw guardados en: {ruta}") #Registra un mensaje informativo indicando que los datos se han guardado correctamente y muestra la ruta del archivo donde se almacenaron.

#El método guardar_raw es un paso crucial en un flujo de trabajo de datos.
# Se encarga de persistencia de datos (guardar datos de la memoria RAM al disco duro),
#  asegurando que los datos extraídos estén respaldados en un formato simple (.csv) antes de pasar a etapas
#  de procesamiento o limpieza más complejas.

#esta funcion maneja varios formatos de guardado: csv, json, excel la anterior solo era para csv
    def guardar_raw(self, df: pd.DataFrame, nombre: str = "datos_raw", formato: str = "csv"): #la funcion necesita tres parametros: self, df (el DataFrame a guardar), nombre (el nombre del archivo sin extension) y formato (el formato en que se guardara el archivo, por defecto es 'csv').
        """Guarda los datos extraídos en formato raw (csv, json, excel)"""

        if formato == 'csv':
            ruta = f"data/raw/{nombre}.csv"   #si el formatio es 'csv', construye la ruta del archivo con extensión .csv en la carpeta data/raw del proyecto.
            df.to_csv(ruta, index=False)      # guarda el DataFrame (df) como un archivo CSV en la ruta especificada (ruta). El parámetro index=False asegura que los índices del DataFrame no se guarden como una columna adicional en el archivo CSV.
        elif formato == 'json':
            ruta = f"data/raw/{nombre}.json"
            df.to_json(ruta, orient='records', lines=True) # Ejemplo de opciones para JSON
        elif formato == 'excel':
            ruta = f"data/raw/{nombre}.xlsx"
            df.to_excel(ruta, index=False, engine='openpyxl')
        else:
            raise ValueError(f"Formato de guardado no soportado: {formato}")
            
        logger.info(f"Datos raw ({formato}) guardados en: {ruta}")