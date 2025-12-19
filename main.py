"""
Script principal del Mini ETL
Demuestra todas las funcionalidades aprendidas
"""
#importamos los modulos necesarios
import os #os es un módulo que proporciona una forma portátil de usar funcionalidades dependientes del sistema operativo, como manipulación de rutas y creación de directorios. por ejemplo os.path.join se utiliza para construir rutas de archivos de manera segura en diferentes sistemas operativos.    
import sys #sys es un módulo que proporciona acceso a algunas variables utilizadas o mantenidas por el intérprete de Python y a funciones que interactúan fuertemente con el intérprete. por ejemplo, sys.path se utiliza para manipular las rutas de búsqueda de módulos.
from datetime import datetime

# Añadir src al path para imports
sys.path.append(os.path.join(os.path.dirname(__file__), 'src')) 
#sys.path es una lista de cadenas que especifican las rutas de búsqueda para módulos. Al modificar sys.path, podemos agregar directorios personalizados donde Python buscará módulos para importar.
#con .append(os.path.join(os.path.dirname(__file__), 'src')) estamos agregando la ruta absoluta de la carpeta src al path de búsqueda de módulos.
# os.path.dirname(__file__) obtiene el directorio del script actual (main.py). __file__ es una variable especial en Python que contiene la ruta del archivo actual. en espa;ol os.path.dirname(file) obtiene el directorio que contiene el archivo actual.
# os.path.join(...) combina ese directorio con 'src' para formar la ruta completa a la carpeta src.
# Esto permite importar módulos desde src sin importar desde dónde se ejecute el script main.py. ahora con src en el path, podemos importar módulos desde esa carpeta directamente.

from src import ExtractorDatos, TransformadorDatos, CargadorDatos #importamos las clases principales del paquete src para usarlas en el pipeline ETL.
from src.logger import LoggerPersonalizado #importamos el logger personalizado para registrar eventos durante la ejecución del ETL. se importa diferente porque no es una clase principal del paquete src, sino una utilidad específica para logging.   
#una utilidad es una función o clase que proporciona funcionalidades auxiliares o de soporte para el programa principal. en este caso, LoggerPersonalizado es una utilidad para manejar el logging de manera consistente en todo el proyecto ETL.
#pero se podría importar igual que las otras clases principales si se quisiera.

def main(): #esta es la función principal que orquesta todo el proceso ETL (Extracción, Transformación, Carga).
    """Función principal del ETL"""
    
    # Inicializar logger
    logger = LoggerPersonalizado().get_logger() #crea una instancia del logger personalizado y obtiene el logger configurado para registrar eventos durante la ejecución del ETL.
    logger.info("=" * 50) #registra una línea de separación en el log para mejorar la legibilidad. el =* 50 crea una cadena de 50 caracteres '='.
    logger.info("INICIANDO PIPELINE ETL") #registra un mensaje informativo indicando el inicio del pipeline ETL.
    logger.info(f"Fecha y hora: {datetime.now()}") #registra la fecha y hora actuales en el log.
    logger.info("=" * 50) #registra otra línea de separación en el log.
    
    try:
        # ========== 1. EXTRACCIÓN ==========
        logger.info("\n🔍 FASE 1: EXTRACCIÓN") 
        extractor = ExtractorDatos()
        
        # Opción 1: Descargar CSV público (descomentar para usar)
        # url_ejemplo = "https://raw.githubusercontent.com/datasets/covid-19/master/data/time-series-19-covid-combined.csv"
        # datos_crudos = extractor.descargar_csv_publico(url_ejemplo)
        
        # Opción 2: Usar datos de ejemplo (para practicar)
        datos_crudos = extractor.descargar_csv_publico("nourl") #la funcion aqui ya lee el csv y retorna un dataframe. 
        
        # Guardar datos raw
        extractor.guardar_raw(datos_crudos, "datos_originales") # guarda los datos crudos en formato CSV en la carpeta data/raw con el nombre "datos_originales.csv"., se le pasa el dataframe y el nombre del archivo sin extension por defecto es csv en al funcion guardar_raw.
        
        # Mostrar información de los datos crudos
        logger.info("\n📊 RESUMEN DATOS CRUDOS:")
        logger.info(f"  • Total registros: {len(datos_crudos)}") #len(datos_crudos) obtiene el número total de filas (registros) en el DataFrame datos_crudos.
        logger.info(f"  • Total columnas: {len(datos_crudos.columns)}") #len(datos_crudos.columns) obtiene el número total de columnas en el DataFrame datos_crudos.
        logger.info(f"  • Columnas: {list(datos_crudos.columns)}") #list(datos_crudos.columns) convierte el índice de columnas del DataFrame en una lista para mostrar los nombres de las columnas.
        logger.info(f"  • Tipos de datos:\n{datos_crudos.dtypes}") #datos_crudos.dtypes devuelve una Serie que contiene los tipos de datos de cada columna en el DataFrame datos_crudos. esto ayuda a entender la estructura de los datos. dtypes es un atributo de los DataFrames de pandas que proporciona información sobre el tipo de datos almacenados en cada columna.
                                                                #un dataframe es una estructura de datos bidimensional en pandas que puede almacenar datos de diferentes tipos (como enteros, cadenas, flotantes) en columnas etiquetadas. mientras que una serie es una estructura de datos unidimensional que puede almacenar datos de un solo tipo con etiquetas de índice. como una columna de un DataFrame.    
        # ========== 2. TRANSFORMACIÓN ==========
        logger.info("\n🔄 FASE 2: TRANSFORMACIÓN")
        transformador = TransformadorDatos()  #crea una instancia de la clase TransformadorDatos para manejar las transformaciones de los datos.
        
        # Limpieza básica
        datos_limpios = transformador.limpiar_datos(datos_crudos) #llama al método limpiar_datos de la instancia transformador, pasando los datos crudos (datos_crudos) como argumento que es el df. este método realiza una limpieza básica de los datos y devuelve un nuevo DataFrame con los datos limpios, que se almacena en la variable datos_limpios.
        
        # Agregar columnas calculadas
        datos_transformados = transformador.agregar_columnas_calculadas(datos_limpios) #llama al método agregar_columnas_calculadas de la instancia transformador, pasando los datos limpios (datos_limpios) como argumento. este método agrega nuevas columnas calculadas al DataFrame y devuelve un nuevo DataFrame con las transformaciones aplicadas, que se almacena en la variable datos_transformados.
        
        # Mostrar información de transformación
        logger.info("\n📈 RESUMEN TRANSFORMACIÓN:")
        logger.info(f"  • Registros después de limpieza: {len(datos_transformados)}")
        logger.info(f"  • Transformaciones aplicadas:")
        for i, transform in enumerate(transformador.transformaciones_aplicadas, 1): #enumerate(transformador.transformaciones_aplicadas, 1) itera sobre la lista de transformaciones aplicadas, proporcionando un índice (i) que comienza en 1 y el nombre de la transformación (transform).
            logger.info(f"    {i}. {transform}") #registra cada transformación aplicada con su índice correspondiente. se usa enumerate para numerar las transformaciones en el log, en este caso es posible usar enumerate porque transformaciones_aplicadas es una lista si fuera un diccionario no se podria usar enumerate directamente ya que un diccionario no tiene un orden fijo.
        
        # Mostrar muestra de datos transformados
        logger.info(f"\n  • Muestra de datos transformados:")
        logger.info(datos_transformados.head().to_string()) #datos_transformados.head() obtiene las primeras 5 filas del DataFrame datos_transformados. .to_string() convierte esas filas en una representación de cadena formateada para que se pueda registrar en el log de manera legible.
        
        # ========== 3. CARGA ==========
        logger.info("\n💾 FASE 3: CARGA")
        cargador = CargadorDatos() #crea una instancia de la clase CargadorDatos para manejar la carga de los datos transformados.
        
        # Guardar en múltiples formatos
        fecha_procesamiento = datetime.now().strftime("%Y%m%d_%H%M%S") #datetime.now().strftime("%Y%m%d_%H%M%S") obtiene la fecha y hora actuales y las formatea como una cadena en el formato "YYYYMMDD_HHMMSS". esto se utiliza para crear un nombre de archivo único basado en la fecha y hora de procesamiento.
        nombre_base = f"datos_procesados_{fecha_procesamiento}" #f"datos_procesados_{fecha_procesamiento}" crea un nombre base para los archivos procesados, incorporando la fecha y hora de procesamiento para asegurar que cada conjunto de datos guardado tenga un nombre único.
        
        rutas_guardadas = cargador.guardar_multiple_formatos( #esta línea llama al método guardar_multiple_formatos de la instancia cargador, pasando los datos transformados (datos_transformados) y el nombre base (nombre_base) como argumentos. este método guarda los datos en múltiples formatos (csv, json, excel) y devuelve un diccionario con las rutas de los archivos guardados, que se almacena en la variable rutas_guardadas.
            datos_transformados, 
            nombre_base
        ) #devuelve un diccionario con las rutas de los archivos guardados en diferentes formatos.
        
        # Resumen final
        logger.info("\n" + "=" * 50)
        logger.info("✅ PIPELINE COMPLETADO EXITOSAMENTE")
        logger.info("=" * 50)
        logger.info("\n📁 ARCHIVOS GENERADOS:") # 
        for formato, ruta in rutas_guardadas.items(): #rutas_guardadas.items() itera sobre los pares clave-valor en el diccionario rutas_guardadas, donde la clave es el formato del archivo (formato) y el valor es la ruta del archivo guardado (ruta).
            logger.info(f"  • {formato.upper()}: {ruta}") #registra la ruta de cada archivo guardado, mostrando el formato en mayúsculas (formato.upper()) que es la clave y la ruta correspondiente (ruta) que es el valor.
        
        logger.info(f"\n📊 ESTADÍSTICAS FINALES:")
        logger.info(f"  • Registros procesados: {len(datos_transformados)}") #len(datos_transformados) obtiene el número total de filas (registros) en el DataFrame datos_transformados.
        logger.info(f"  • Columnas finales: {len(datos_transformados.columns)}") #len(datos_transformados.columns) obtiene el número total de columnas en el DataFrame datos_transformados.
        logger.info(f"  • Columnas: {list(datos_transformados.columns)}") #list(datos_transformados.columns) convierte el índice de columnas del DataFrame en una lista para mostrar los nombres de las columnas finales.
        
        return { #ella función main devuelve un diccionario con un resumen del resultado del pipeline ETL.
            'success': True, #indica que el pipeline se completó exitosamente.
            'registros_procesados': len(datos_transformados), #devuelve el número total de registros procesados.
            'archivos_generados': rutas_guardadas, #devuelve un diccionario con las rutas de los archivos generados.
            'transformaciones': transformador.transformaciones_aplicadas #devuelve la lista de transformaciones aplicadas durante el proceso ETL.
        }
        
    except Exception as e: #si ocurre cualquier excepción durante la ejecución del bloque try, se captura aquí.
        logger.error(f"❌ ERROR EN EL PIPELINE: {str(e)}") #registra un mensaje de error en el log con los detalles de la excepción.
        return { #devuelve un diccionario con el resumen del error ocurrido durante el pipeline ETL.
            'success': False, #indica que el pipeline no se completó exitosamente.
            'error': str(e) #devuelve el mensaje de error como una cadena.
        }

if __name__ == "__main__": #name es una variable especial en Python que contiene el nombre del módulo actual. Si el módulo se está ejecutando como el programa principal, name se establece en "__main__". si es verdadero, significa que este script se está ejecutando directamente (no importado como un módulo en otro script), por lo que se ejecuta el bloque de código dentro de esta condición.
    # Ejecutar el pipeline
    resultado = main()  #llama a la función main() para ejecutar el pipeline ETL y almacena el resultado en la variable resultado.
    
    # Mostrar resultado en consola
    print("\n" + "=" * 50)
    if resultado['success']: #si el valor asociado a la clave 'success' en el diccionario resultado es True, significa que el pipeline se completó exitosamente.
        print("🎉 ¡Proyecto ETL completado exitosamente!") #esto se imprime 
        print(f"📈 Registros procesados: {resultado['registros_procesados']}")
        print("📁 Revisa la carpeta 'data/processed' para ver los resultados")
    else:
        print(f"❌ Error: {resultado['error']}")
    print("=" * 50)