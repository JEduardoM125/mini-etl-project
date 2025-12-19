#En Python, el módulo logging usa Loggers para generar mensajes y Handlers para decidir a dónde van esos mensajes 
# (consola, archivo, email, etc.) y cómo se ven (formato), 
# permitiendo controlar el flujo de información para depurar, auditar o monitorear una aplicación, 
# configurando distintos destinos para diferentes niveles de severidad (DEBUG, INFO, WARNING, ERROR, CRITICAL). 

#Logger (registrador):
# Es el objeto principal que llamas en tu código (ej. logging.getLogger(__name__)).
# Recibe los mensajes de tu aplicación (info, advertencias, errores).
# Los clasifica por niveles de severidad (DEBUG, INFO, WARNING, ERROR, CRITICAL).
# Envía estos mensajes a sus handlers asociados.
# Handler (manejador):
# Es el componente que decide el destino del mensaje.
# Determina dónde se escribirá el log (ej. a la consola con StreamHandler, a un archivo con FileHandler, por correo con SMTPHandler).
# Se le puede configurar un nivel mínimo de severidad; solo los mensajes de ese nivel o superiores pasarán a través de él.
# Puede tener un Formatter asociado para definir el aspecto del mensaje (fecha, nivel, módulo, mensaje). 

import logging # este modulo se utiliza para registrar eventos que ocurren durante la ejecución de un programa
from datetime import datetime #sirve para trabajar con fechas y horas
import os
from venv import logger #sirve para interactuar con el sistema operativo

#logging y datetime  son técnicamente módulos que forman parte de la Biblioteca Estándar de Python.
#igual os
#src es un paquete personalizado creado para este proyecto ETL específico. es mi proyecto local que luego importamos en el main.py

#logger.py es un módulo dentro del paquete src que define la configuración y funcionalidad del sistema de logging personalizado para el proyecto ETL.
class LoggerPersonalizado:
    def __init__(self, nombre_logger='ETL_Logger'):
        """Inicializa el sistema de logging personalizado"""
        self.logger = logging.getLogger(nombre_logger) #logging es un módulo de Python que proporciona una forma flexible de emitir mensajes de log desde programas Python.
        self.logger.setLevel(logging.DEBUG)
# self es una convención en Python que se refiere a la instancia actual de la clase.
# nombre_logger es el nombre que se le da al logger para identificarlo.
#self.logger es el atributo de la clase que almacena el objeto logger. y logging.getLogger(nombre_logger) crea o recupera un logger con el nombre especificado.
# self.logger.setLevel(logging.DEBUG) establece el nivel de severidad del logger a DEBUG, lo que significa que registrará todos los mensajes de nivel DEBUG y superiores.
#el nivel debug es el nivel más bajo de severidad en el sistema de logging, lo que significa que capturará todos los mensajes de log, desde los más detallados hasta los más críticos.

 # Evitar múltiples handlers
        if not self.logger.handlers: #si no hay handlers asociados al logger, entonces se procede a configurar los handlers. los handlers son responsables de enviar los mensajes de log a diferentes destinos, como archivos o la consola.
            # Crear carpeta de logs si no existe
            os.makedirs("logs", exist_ok=True) # os.makedirs crea un directorio recursivamente. Si el directorio ya existe, no lanza una excepción gracias a exist_ok=True.
            
            # Handler para archivo
            fecha_actual = datetime.now().strftime("%Y%m%d") # datetime.now() obtiene la fecha y hora actuales. .strftime("%Y%m%d") formatea la fecha en una cadena con el formato "AñoMesDía".
            file_handler = logging.FileHandler( # logging.FileHandler crea un handler que escribe los mensajes de log en un archivo.
                f"logs/etl_{fecha_actual}.log", #imprime el nombre del archivo de log con la fecha actual.
                encoding='utf-8' # se usa para asegurarse de que los caracteres especiales se manejen correctamente al escribir en el archivo.
            )
            file_handler.setLevel(logging.DEBUG) # aqui denuevo se establece el nivel de severidad del handler a DEBUG. por ende el handler registrará todos los mensajes de nivel DEBUG y superiores en el archivo de log.
            
            # Handler para consola
            console_handler = logging.StreamHandler() # logging.StreamHandler crea un handler que envía los mensajes de log a la consola (stdout).
            console_handler.setLevel(logging.INFO) #setLevel(logging.INFO) establece el nivel de severidad del handler a INFO. por lo tanto, este handler solo registrará mensajes de nivel INFO y superiores en la consola.
            
            # Formato
            formato = logging.Formatter( # logging.Formatter define el formato de los mensajes de log.
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s'  # este formato incluye la fecha y hora del log (asctime), el nombre del logger (name), el nivel de severidad del mensaje (levelname) y el mensaje en sí (message).
            )
            file_handler.setFormatter(formato) # se asigna el formato definido al handler de archivo.
            console_handler.setFormatter(formato) # se asigna el formato definido al handler de consola.
            
            self.logger.addHandler(file_handler) # se agrega el handler de archivo al logger.
            self.logger.addHandler(console_handler) # se agrega el handler de consola al logger.
    
    def get_logger(self): # devuelve el logger configurado
        return self.logger
    
    
    # Función para manejo de errores
def manejar_error(func):
    """Decorador para manejo de errores en funciones"""
    def wrapper(*args, **kwargs): #*args y **kwargs permiten pasar un número variable de argumentos posicionales y de palabras clave a la función decorada. wrapper es una función interna que envuelve la función original (func) para agregar funcionalidad adicional, en este caso, manejo de errores.
        logger = LoggerPersonalizado().get_logger() #se crea una instancia del logger personalizado y se obtiene el logger configurado.
        try:                                            #se hace un try-except para capturar cualquier excepción que ocurra durante la ejecución de la función decorada.
                logger.info(f"Ejecutando {func.__name__}")  #se registra un mensaje de información indicando que se está ejecutando la función.
                resultado = func(*args, **kwargs)          #se llama a la función decorada con los argumentos proporcionados y se almacena el resultado.
                logger.info(f"{func.__name__} completado exitosamente") #se registra un mensaje de información indicando que la función se completó exitosamente.
                return resultado    #se devuelve el resultado de la función decorada.
        except Exception as e:
                logger.error(f"Error en {func.__name__}: {str(e)}")
                raise  #si ocurre una excepción, se registra un mensaje de error con los detalles de la excepción y luego se vuelve a lanzar la excepción para que pueda ser manejada más arriba en la pila de llamadas.
    return wrapper #se devuelve la función wrapper, que ahora incluye el manejo de errores alrededor de la función original.
#la funcion manejar_error es un decorador que se puede aplicar a cualquier función para agregarle manejo de errores y registro de eventos sin modificar el código original de la función.
# la funcion wrapper se usa para envolver la función original y agregar la funcionalidad adicional de logging y manejo de errores.

# Función Decoradora (el decorador): Es una función que acepta otra función (func) como parámetro.
# Función Interna (Wrapper): Dentro del decorador, se define otra función (comúnmente llamada wrapper) que envuelve a func. Aquí es donde se añade la nueva lógica (código antes/después de llamar a func).
# Retorno: El decorador devuelve la función wrapper.
# Aplicación: Al usar @mi_decorador sobre def mi_funcion():, Python ejecuta mi_funcion = mi_decorador(mi_funcion) de forma transparente. 

