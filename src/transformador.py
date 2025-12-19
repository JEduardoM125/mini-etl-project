import pandas as pd
import numpy as np
from typing import List, Dict, Any
from datetime import datetime
from .logger import manejar_error, LoggerPersonalizado

logger = LoggerPersonalizado().get_logger()

class TransformadorDatos:
    """Clase para transformar y limpiar datos"""
    
    def __init__(self):
        self.transformaciones_aplicadas = []
#este constructor inicializa una lista vacía llamada transformaciones_aplicadas para llevar un registro de las transformaciones realizadas en los datos.

    @manejar_error
    def limpiar_datos(self, df: pd.DataFrame) -> pd.DataFrame: #esta funcion toma un DataFrame de pandas como entrada y devuelve un DataFrame limpio después de aplicar varias transformaciones.
        """
        Realiza limpieza básica de datos
        
        Args:
            df: DataFrame a limpiar
            
        Returns:
            DataFrame limpio
        """
        logger.info("Iniciando limpieza de datos...")
        
        # Crear copia para no modificar el original
        df_limpio = df.copy()
        
        # 1. Registrar estado inicial
        filas_iniciales = len(df_limpio) #se obtiene el número de filas iniciales del DataFrame y se almacena en filas_iniciales.
        columnas_iniciales = list(df_limpio.columns) #se obtiene la lista de nombres de columnas iniciales del DataFrame y se almacena en columnas_iniciales.
        
        # 2. Manejo de valores nulos
        self._manejar_nulos(df_limpio) #se llama a un método privado _manejar_nulos para manejar los valores nulos en el DataFrame.
        
        # 3. Normalizar strings
        self._normalizar_strings(df_limpio) #se llama a otro método privado _normalizar_strings para normalizar las cadenas de texto en el DataFrame. aqui no se asigna el resultado a df_limpio porque la normalización se realiza en el lugar, modificando directamente el DataFrame pasado como argumento.
        
        # 4. Filtrar filas inválidas
        df_limpio = self._filtrar_filas(df_limpio) #se llama a un tercer método privado _filtrar_filas para filtrar filas inválidas del DataFrame y se actualiza df_limpio con el resultado. aqui si se reasigna df_limpio porque el método _filtrar_filas devuelve un nuevo DataFrame con las filas inválidas eliminadas.
        
        # 5. Convertir tipos de datos
        self._convertir_tipos(df_limpio) #se llama a un cuarto método privado _convertir_tipos para convertir los tipos de datos en el DataFrame según sea necesario.
        
        # 6. Eliminar duplicados
        duplicados = df_limpio.duplicated().sum() #se calcula el número de filas duplicadas en el DataFrame y se almacena en duplicados.
        if duplicados > 0: #si hay filas duplicadas, se eliminan usando drop_duplicates() y se registra un mensaje informativo con el número de registros duplicados eliminados.
            df_limpio = df_limpio.drop_duplicates()
            logger.info(f"Eliminados {duplicados} registros duplicados")
        
        # Registrar transformaciones
        transformacion = {   #se crea un diccionario con los detalles de la transformación realizada, incluyendo la fecha, el número de filas iniciales y finales, las columnas iniciales y las transformaciones aplicadas.
            'fecha': datetime.now(),
            'filas_iniciales': filas_iniciales,
            'filas_finales': len(df_limpio),
            'columnas': columnas_iniciales,
            'transformaciones': self.transformaciones_aplicadas.copy() #se hace una copia de la lista de transformaciones aplicadas para evitar modificaciones futuras que viene del metodo  manejar_nulos
        }
        
        logger.info(f"Limpieza completada. Filas: {filas_iniciales} -> {len(df_limpio)}")
        
        return df_limpio

  #en cada funcion se usa como parametro self porque son métodos de instancia de la clase TransformadorDatos y se necesita para acceder a los atributos y otros métodos de la clase.
    def _manejar_nulos(self, df: pd.DataFrame): #se usa self porque es un método de instancia de la clase TransformadorDatos. df es el DataFrame que se va a procesar para manejar los valores nulos.
        """Manejo de valores nulos"""
        nulos_por_columna = df.isnull().sum() #df.isnull() devuelve un DataFrame booleano del mismo tamaño que df, donde cada celda es True si el valor correspondiente en df es nulo (NaN) y False en caso contrario. .sum() luego suma estos valores booleanos a lo largo de las columnas, dando como resultado una Serie donde el índice son los nombres de las columnas y los valores son el conteo de valores nulos en cada columna. es decir fecha puede tener nulos en varias columnas y esta linea cuenta cuantos nulos hay en cada columna.
        
        for columna, cantidad in nulos_por_columna.items(): #itera sobre cada par columna-cantidad en la Serie nulos_por_columna. columna es el nombre de la columna y cantidad es el número de valores nulos en esa columna.
            if cantidad > 0:
                # Estrategias diferentes por tipo de columna
                if df[columna].dtype in ['int64', 'float64']: #si la columna es de tipo numérico (int64 o float64), se calcula la media de la columna y se usan esos valores para reemplazar los nulos.
                    # Para numéricas: reemplazar con media
                    media = df[columna].mean()
                    df[columna].fillna(media, inplace=True) #fillna(media, inplace=True) reemplaza los valores nulos en la columna con la media calculada. inplace=True significa que la operación se realiza directamente en el DataFrame original sin necesidad de asignarlo a una nueva variable.
                    self.transformaciones_aplicadas.append( #se registra la transformación aplicada en la lista transformaciones_aplicadas. se usa self para acceder al atributo de la instancia actual de la clase. es decir al objeto actual de TransformadorDatos.
                        f"Reemplazados {cantidad} nulos en '{columna}' con media: {media:.2f}"
                    )
                elif df[columna].dtype == 'object': #si la columna es de tipo object (generalmente cadenas de texto), se reemplazan los nulos con la cadena "DESCONOCIDO".
                    # Para strings: reemplazar con "DESCONOCIDO"
                    df[columna].fillna('DESCONOCIDO', inplace=True) #fillna('DESCONOCIDO', inplace=True) reemplaza los valores nulos en la columna con la cadena 'DESCONOCIDO'. e inplace=True significa que la operación se realiza directamente en el DataFrame original sin necesidad de asignarlo a una nueva variable.
                    self.transformaciones_aplicadas.append( #se registra la transformación aplicada en la lista transformaciones_aplicadas. self es para acceder al atributo de la instancia actual de la clase y asi se referencia al objeto actual de TransformadorDatos.
                        f"Reemplazados {cantidad} nulos en '{columna}' con 'DESCONOCIDO'"
                    )
                elif 'fecha' in columna.lower(): #si el nombre de la columna contiene la palabra 'fecha' (ignorando mayúsculas y minúsculas), se asume que es una columna de fechas.
                    # Para fechas: reemplazar con fecha actual
                    df[columna].fillna(datetime.now().strftime('%Y-%m-%d'), inplace=True) #fillna(datetime.now().strftime('%Y-%m-%d'), inplace=True) reemplaza los valores nulos en la columna con la fecha actual formateada como 'Año-Mes-Día'. datetime.now() obtiene la fecha y hora actuales, .strftime('%Y-%m-%d') formatea la fecha en una cadena con el formato especificado, e inplace=True significa que la operación se realiza directamente en el DataFrame original sin necesidad de asignarlo a una nueva variable.
                    self.transformaciones_aplicadas.append( #se registra la transformación aplicada en la lista transformaciones_aplicadas. self se usa para acceder al atributo de la instancia actual de la clase y asi referenciar al objeto actual de TransformadorDatos. se usa append para agregar un nuevo elemento a la lista y se pone al final de la lista.
                        f"Reemplazados {cantidad} nulos en '{columna}' con fecha actual"
                    )

 #normalizar strings es una funcion privada de la clase TransformadorDatos que se encarga de normalizar las cadenas de texto en un DataFrame de pandas.  
 #con normalizar se refiere a estandarizar el formato de las cadenas para mejorar la consistencia y facilitar el análisis posterior.
    def _normalizar_strings(self, df: pd.DataFrame): # la funcion espera dos parametros: self, que es una referencia a la instancia actual de la clase TransformadorDatos, y df, que es un DataFrame de pandas que contiene los datos a ser normalizados.
        """Normaliza strings (mayúsculas, espacios, etc.)"""
        columnas_string = df.select_dtypes(include=['object']).columns #df.select_dtypes es un método de pandas que selecciona columnas basadas en sus tipos de datos. include=['object'] indica que se desean seleccionar todas las columnas que son de tipo 'object', que en pandas generalmente representa cadenas de texto. .columns luego extrae los nombres de estas columnas seleccionadas y los almacena en la variable columnas_string.
        
        for columna in columnas_string:  #itera sobre cada nombre de columna en la lista columnas_string.
            # Convertir a string, eliminar espacios, capitalizar
            df[columna] = df[columna].astype(str).str.strip().str.title() #df[columna].astype(str) convierte todos los valores en la columna actual a tipo string. .str.strip() elimina cualquier espacio en blanco al inicio y al final de cada cadena en la columna. .str.title() convierte la primera letra de cada palabra en mayúscula y las demás letras en minúscula. El resultado final se asigna de nuevo a df[columna], actualizando la columna con las cadenas normalizadas.
            
            # Reemplazar múltiples espacios por uno solo
            df[columna] = df[columna].str.replace(r'\s+', ' ', regex=True) #df[columna].str.replace(r'\s+', ' ', regex=True) utiliza una expresión regular para buscar múltiples espacios en blanco (representados por \s+) en cada cadena de la columna y los reemplaza con un solo espacio (' '). El parámetro regex=True indica que el primer argumento es una expresión regular. El resultado se asigna de nuevo a df[columna], actualizando la columna con las cadenas donde los múltiples espacios han sido reemplazados por uno solo.
                                                                                #r indica que la cadena es una cadena raw (cruda), lo que significa que los caracteres de escape se interpretan literalmente. r\s+ es una expresión regular que coincide con uno o más espacios en blanco consecutivos. r'\s+', ' ' reemplaza esos múltiples espacios con un solo espacio.
        self.transformaciones_aplicadas.append("Strings normalizados (strip, title)") #se registra la transformación aplicada en la lista transformaciones_aplicadas.
        
        #("Strings normalizados (strip, title)") es una cadena que describe la transformación realizada, indicando que se han normalizado las cadenas de texto aplicando las funciones strip (eliminar espacios) y title (capitalizar).

# filtrar filas es una funcion privada de la clase TransformadorDatos que se encarga de filtrar filas inválidas en un DataFrame de pandas.
    def _filtrar_filas(self, df: pd.DataFrame) -> pd.DataFrame: #devuelve un DataFrame de pandas después de filtrar las filas inválidas.
        """Filtra filas inválidas"""
        filas_iniciales = len(df) #se obtiene el número de filas iniciales del DataFrame y se almacena en filas_iniciales.
        
        # Ejemplo: filtrar edades negativas o muy altas
        if 'edad' in df.columns:
            df = df[(df['edad'] > 0) & (df['edad'] < 120)] # si la columna 'edad' existe en el DataFrame, se filtran las filas donde la edad es mayor que 0 y menor que 120. Esto elimina filas con edades negativas o extremadamente altas que se consideran inválidas. y df tendra solo las filas que cumplen con esta condición.
            filas_filtradas = filas_iniciales - len(df) # se calcula el número de filas que fueron filtradas restando el número de filas actuales del DataFrame (después del filtrado) del número de filas iniciales.
            if filas_filtradas > 0:   #si se filtraron filas (es decir, si filas_filtradas es mayor que 0), se registra la transformación aplicada en la lista transformaciones_aplicadas.
                self.transformaciones_aplicadas.append(
                    f"Filtradas {filas_filtradas} filas con edad inválida"
                )
        
        # Ejemplo: filtrar salarios negativos
        if 'salario' in df.columns:
            df = df[df['salario'] > 0] #si la columna 'salario' existe en el DataFrame, se filtran las filas donde el salario es mayor que 0. Esto elimina filas con salarios negativos que se consideran inválidos.
                                        #aqui vemos que para acceder a una columna de un DataFrame se usa df['nombre_columna'] y no df.nombre_columna
        return df


# convertir_tipos es una funcion privada de la clase TransformadorDatos que se encarga de convertir los tipos de datos en un DataFrame de pandas según ciertas reglas.
    def _convertir_tipos(self, df: pd.DataFrame):
        """Convierte tipos de datos correctamente"""
        tipo_conversiones = [] #se inicializa una lista vacía llamada tipo_conversiones para llevar un registro de las conversiones de tipo realizadas en el DataFrame.
        
        # Detectar columnas de fecha
        for columna in df.columns: #itera sobre cada nombre de columna en el DataFrame.
            if 'fecha' in columna.lower(): #si el nombre de la columna contiene la palabra 'fecha' (ignorando mayúsculas y minúsculas), se intenta convertir esa columna a tipo datetime.
                try:
                    df[columna] = pd.to_datetime(df[columna]) #pd.to_datetime(df[columna]) intenta convertir los valores en la columna actual a tipo datetime. El resultado se asigna de nuevo a df[columna], actualizando la columna con los valores convertidos.
                    tipo_conversiones.append(f"'{columna}' a datetime") #si la conversión es exitosa, se registra la conversión realizada en la lista tipo_conversiones.
                except:
                    logger.warning(f"No se pudo convertir {columna} a datetime") #si ocurre un error durante la conversión, se registra una advertencia en el logger indicando que no se pudo convertir la columna a datetime.
        
        if tipo_conversiones:
            self.transformaciones_aplicadas.append( #si se realizaron conversiones de tipo, se registra la lista de conversiones en la lista transformaciones_aplicadas.
                f"Conversiones de tipo: {', '.join(tipo_conversiones)}" #se une la lista tipo_conversiones en una sola cadena separada por comas para facilitar su lectura. se usa .join() para concatenar los elementos de la lista en una sola cadena, separados por comas y espacios. es decir tenemos , espacio y sigueinte cadena.
            ) #join se usa para unir los elementos de una lista en una sola cadena, con un separador especificado (en este caso, una coma seguida de un espacio). por ejempl;o, si tipo_conversiones es ["'fecha_nacimiento' a datetime", "'fecha_ingreso' a datetime"], entonces ', '.join(tipo_conversiones) produciría la cadena "'fecha_nacimiento' a datetime, 'fecha_ingreso' a datetime".


    @manejar_error # se aplica el decorador manejar_error para agregar manejo de errores y logging automáticamente a la función agregar_columnas_calculadas.
    def agregar_columnas_calculadas(self, df: pd.DataFrame) -> pd.DataFrame: # esta función toma un DataFrame de pandas como entrada y devuelve un DataFrame modificado con columnas calculadas adicionales. el df ya limpio es el parametro que recibe.
        """Agrega columnas calculadas"""
        df_modificado = df.copy() # se crea una copia del DataFrame original para no modificarlo directamente.
        
        # Ejemplo: categoría por edad
        if 'edad' in df_modificado.columns: # si la columna 'edad' existe en el DataFrame, se crean condiciones para categorizar las edades en 'Joven', 'Adulto' y 'Senior'.
            condiciones = [ #condiciones es una lista de condiciones que se evaluarán para cada fila del DataFrame.
                (df_modificado['edad'] < 30), # primera condición: edades menores a 30
                (df_modificado['edad'] < 50),
                (df_modificado['edad'] >= 50)
            ]
            valores = ['Joven', 'Adulto', 'Senior'] #valores correspondientes a cada condición. se clasifican las edades en tres categorías: 'Joven' para edades menores a 30, 'Adulto' para edades entre 30 y 49, y 'Senior' para edades de 50 en adelante. valores es una lista que contiene las categorías correspondientes a cada condición definida en la lista condiciones.
            
            df_modificado['categoria_edad'] = np.select(condiciones, valores, default='Desconocido')# np.select(condiciones, valores, default='Desconocido') crea una nueva columna 'categoria_edad' en el DataFrame. Esta función evalúa las condiciones en orden y asigna el valor correspondiente de la lista valores cuando se cumple una condición. Si ninguna condición se cumple, asigna el valor 'Desconocido' como valor predeterminado.
            self.transformaciones_aplicadas.append("Agregada columna 'categoria_edad'") # se registra la transformación aplicada en la lista transformaciones_aplicadas.
                #con numpy np.select se pueden evaluar múltiples condiciones y asignar valores correspondientes de manera eficiente en un DataFrame de pandas.

        # Ejemplo: salario anual (si es mensual)
        if 'salario' in df_modificado.columns: #si la columna de nombre salario esta el dataframe copia
            df_modificado['salario_anual'] = df_modificado['salario'] * 12  #se crea una nueva columna llamada salario_anual que se calcula multiplicando los valores de la columna salario por 12, asumiendo que el salario es mensual.
            self.transformaciones_aplicadas.append("Agregada columna 'salario_anual'") 
        
        return df_modificado #devuelve el DataFrame modificado pero es el copia o el roiginal? es el copia porque se hizo al inicio df_modificado = df.copy() por ende no se modifica el original.
#como no se modifica el df original, esto se usa mas que nada para hacer pruebas y ver como quedan los datos despues de agregar las columnas calculadas sin afectar el df original.