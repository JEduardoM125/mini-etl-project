# este __init_.py indica que src es un paquete de Python. Un paquete es una forma de organizar módulos relacionados en un solo directorio.
#Por ende, este archivo permite que los módulos dentro de src (como extractor.py, transformador.py, loader.py, logger.py) sean importados fácilmente desde otros lugares en el proyecto o desde fuera de él.    
#Cuando un paquete es importable, Python ejecuta el código dentro de __init__.py, lo que puede incluir la inicialización del paquete, la configuración de variables de paquete, o la importación de submódulos para facilitar su acceso.    
# Además, este archivo puede definir qué se exporta cuando se usa from src import * mediante la variable __all__.
"""
Paquete ETL - Mini proyecto de Data Engineering
"""
# Importar las clases y funciones principales para exponerlas en el paquete src
# al importar src, se pueden acceder directamente a estas clases y funciones sin necesidad de importar cada módulo individualmente.
from .extractor import ExtractorDatos
from .transformador import TransformadorDatos
from .loader import CargadorDatos
from .logger import LoggerPersonalizado, manejar_error

__version__ = "1.0.0"
__author__ = "Data Engineer en formación"
__all__ = [
    'ExtractorDatos',
    'TransformadorDatos', 
    'CargadorDatos',
    'LoggerPersonalizado',
    'manejar_error'
]


#En este caso, __all__ define una lista de nombres que se exportarán cuando alguien use from src import *. Esto ayuda a controlar qué partes del paquete son accesibles desde fuera.
# Al definir __all__, se mejora la encapsulación y se evita la exposición accidental de módulos o funciones internas que no deberían ser accesibles directamente.
#para construir el all primero se debe imprortar todo lo que se quiere incluir en el paquete.

#Entonces en lugar de hacer from src.extractor import ExtractorDatos, se puede hacer simplemente from src import ExtractorDatos desde otro módulo o script.

