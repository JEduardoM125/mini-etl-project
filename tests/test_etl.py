


import unittest # unittest es el framework de testing que viene con Python. Nos permite verificar que nuestro código funciona correctamente.
import pandas as pd # pandas es una librería para manipulación y análisis de datos. Nos permite trabajar con estructuras de datos como DataFrames.
import sys # nos permite manipular el path de importación de módulos.
import os # nos permite interactuar con el sistema operativo, como manejar rutas de archivos.

# Añadir src al path para poder importar los módulos de ETL
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'src'))
#Esto es como decirle a Python "Oye, cuando busques módulos para importar, también mira en esta carpeta llamada 'src' que está un nivel arriba de donde estamos ahora".

# Importamos las clases que vamos a testear
from src import ExtractorDatos, TransformadorDatos, CargadorDatos

class TestETL(unittest.TestCase): # Creamos una clase de test que hereda de unittest.TestCase que tiene métodos y funcionalidades para crear tests.
    
    def setUp(self):
        """Configuración inicial para cada test"""
        # Instanciamos los objetos de ETL
        # Este método se ejecuta ANTES de CADA test
        # Es como preparar los ingredientes antes de cada receta


        self.extractor = ExtractorDatos() # Instancia para pruebas
        self.transformador = TransformadorDatos() #Otra instancia para pruebas
        self.cargador = CargadorDatos() #Otra instancia para pruebas
        
         # Datos de prueba SIMULADOS (mock data)
        self.datos_prueba = pd.DataFrame({
            'id': [1, 2, 3],
            'nombre': ['  juan pérez  ', 'MARÍA GARCÍA', None],
            'edad': [25, None, 30],
            'salario': [30000, 35000, -1000]
        }) #el diccionario se convierte en un DataFrame de pandas, las filas son los registros/valores y las claves son los nombres de las columnas.

#¿Por qué setUp() antes de cada test?
#Para que cada test empiece con datos FRESCOS, no contaminados por tests anteriores.
    
    def test_limpieza_datos(self):
        """Test de limpieza de datos"""
        # 1. Ejecutar la función que queremos probar
        df_limpio = self.transformador.limpiar_datos(self.datos_prueba)
        
        # 2. VERIFICACIONES (asserts)

        # Verificar que no hay nulos
        self.assertFalse(df_limpio.isnull().any().any()) #assertFalse es un método de unittest que verifica que la condición dada es FALSA. Si la condición es VERDADERA, el test falla.  
         # Traducción: "Afirmo que es FALSO que haya nulos" es decir que NO hay nulos.
        #.isnull() devuelve un DataFrame del mismo tamaño con valores booleanos (True si el valor es nulo, False si no lo es).
        #.any().any() primero verifica si hay algún True en cada columna (primera llamada a any()), y luego verifica si hay algún True en el resultado (segunda llamada a any()). el resultado final es un solo valor booleano que indica si hay algún nulo en todo el DataFrame.

        # Verificar que strings están normalizados
        nombres = df_limpio['nombre'].tolist()  # Convertimos la columna 'nombre' a una lista para facilitar las verificaciones
         # Ahora nombres es una lista con los valores de la columna 'nombre' después de la limpieza.

        self.assertEqual(nombres[0], 'Juan Pérez')  # Espacios eliminados, capitalizado
        #assertEqual verifica que los dos valores sean iguales.
         # Traducción: "Afirmo que el primer nombre es igual a 'Juan Pérez'"

        self.assertEqual(nombres[1], 'María García')  # Mayúsculas corregidas
        
        #self.assertEqual(nombres[2], 'Desconocido')  # Nulos reemplazados
         # CORRECCIÓN: Solo hay 2 filas después de limpiar (tercera se eliminó por salario negativo)
        # No verificamos nombres[2] porque esa fila fue filtrada
        print(f"✅ test_limpieza_datos pasado. Filas: {len(df_limpio)}")


    
    def test_filtrado_filas(self):
        """Test de filtrado de filas inválidas"""
        df_limpio = self.transformador.limpiar_datos(self.datos_prueba)
        
        # Verificar que no hay salarios negativos
        self.assertTrue((df_limpio['salario'] > 0).all())
    #assertTrue verifica que la condición dada es VERDADERA. Si la condición es FALSA, el test falla.
     # Traducción: "Afirmo que TODOS los salarios son mayores a 0"

    # crear un test para la función de agregar columnas calculadas
    def test_agregar_columnas(self):
        """Test de agregado de columnas calculadas"""
        df_con_columnas = self.transformador.agregar_columnas_calculadas(
            self.datos_prueba #agrgar columnas al DataFrame de prueba y verificar que las nuevas columnas existen
        )
        
        # Verificar que se agregaron nuevas columnas
        self.assertIn('categoria_edad', df_con_columnas.columns)
        self.assertIn('salario_anual', df_con_columnas.columns)
    #assertIn verifica que el primer argumento esté contenido en el segundo argumento. Si no lo está, el test falla.
     # Traducción: "Afirmo que 'categoria_edad' está en las columnas del DataFrame"

if __name__ == '__main__':
    unittest.main() # Esto ejecuta todos los tests cuando corremos este archivo directamente.
    # si __name_ es igual a _'_main_'_ significa que este archivo se está ejecutando directamente (no importado como módulo en otro archivo).