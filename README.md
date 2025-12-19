# 🚀 Mini Proyecto ETL - Data Engineering

[![Python](https://img.shields.io/badge/Python-3.9%2B-blue)](https://www.python.org/)
[![Pandas](https://img.shields.io/badge/Pandas-2.0.3-green)](https://pandas.pydata.org/)
[![License](https://img.shields.io/badge/License-MIT-yellow)](LICENSE)

Proyecto educativo que implementa un pipeline ETL completo desde cero, demostrando los fundamentos de Data Engineering con Python.

 Este proyecto fue desarrollado enfrentando y resolviendo problemas reales como conflictos de versiones de NumPy, configuración de entornos virtuales, y debugging de imports en Python.

## 📊 **Demo del Pipeline**


# 🚀 Mini Proyecto ETL - Data Engineering

Proyecto fundamentos de Data Engineering con Python.

## 📋 Características Implementadas

✅ **Extracción de datos** desde múltiples fuentes  
✅ **Limpieza y transformación** de datos  
✅ **Manejo de errores** con logging profesional  
✅ **POO** (Programación Orientada a Objetos)  
✅ **Modularización** del código  
✅ **Entornos virtuales**  
✅ **Múltiples formatos** de salida (CSV, JSON, Excel)  
✅ **Tests unitarios**  

## 🏗️ Estructura del Proyecto
mini-etl-project/
├── src/ # Código fuente modular
├── data/ # Datos (raw y processed)
├── logs/ # Logs de ejecución
├── tests/ # Tests unitarios
├── main.py # Script principal
└── requirements.txt # Dependencias


## 🛠️ **Tecnologías Utilizadas**

- **Python 3.9+** - Lenguaje principal
- **Pandas 2.0.3** - Manipulación de datos
- **NumPy 1.24.3** - Operaciones numéricas
- **OpenPyXL 3.1.2** - Manejo de archivos Excel
- **Requests 2.31.0** - Descarga de datos web
- **Pytest 7.4.3** - Testing automatizado

# Versiones EXACTAS que funcionaron
numpy==1.24.3
pandas==2.0.3
requests==2.31.0
openpyxl==3.1.2
pytest==7.4.3
pytest-cov==4.1.0

## 🚀 **Cómo Ejecutar (PASO A PASO REAL)**

### **Prerrequisitos**
- Python 3.9 o superior instalado
- Git para clonar el repositorio
- Conexión a internet para descargar dependencias

### **Paso 1: Clonar el repositorio**
```bash
git clone https://github.com/JEduardoM125/mini-etl-project.git
cd mini-etl-project

Paso 2: Configurar entorno virtual (FORMA REAL QUE FUNCIONÓ)
bash
# Crear entorno virtual
python -m venv venv

# Activar (Windows PowerShell)
venv\Scripts\Activate.ps1

# Si aparece error de permisos en PowerShell:
Set-ExecutionPolicy -ExecutionPolicy RemoteSigned -Scope CurrentUser
Paso 3: Instalar dependencias (VERSIONES EXACTAS)
bash
# Actualizar pip primero
python -m pip install --upgrade pip

# Instalar setuptools y wheel (IMPORTANTE para evitar errores)
pip install setuptools==65.5.0 wheel==0.38.4

# Instalar NumPy desde binaries (evita problemas de compilación)
pip install numpy==1.24.3 --only-binary=:all:

# Instalar el resto
pip install pandas==2.0.3 requests==2.31.0 openpyxl==3.1.2
O usando requirements.txt:

bash
pip install -r requirements.txt
Paso 4: Ejecutar el pipeline ETL
bash
python main.py
Paso 5: Ejecutar tests (VERIFICAR QUE TODO FUNCIONA)
bash
# Desde la carpeta raíz del proyecto
python -m pytest tests/test_etl.py -v
📊 Salida Esperada
Al ejecutar python main.py, verás:

text
==================================================
INICIANDO PIPELINE ETL
Fecha y hora: 2024-12-19 15:30:00
==================================================

🔍 FASE 1: EXTRACCIÓN
✅ Datos descargados/leídos: 5 registros, 6 columnas

🔄 FASE 2: TRANSFORMACIÓN
✅ Datos limpios: 4 registros después de filtrado
✅ Transformaciones aplicadas: 5

💾 FASE 3: CARGA
✅ Archivos generados:
   • data/processed/datos_procesados_20241219_153000.csv
   • data/processed/datos_procesados_20241219_153000.json
   • data/processed/datos_procesados_20241219_153000.xlsx

🎉 ¡Proyecto ETL completado exitosamente!
==================================================
🧪 Ejecutar Tests
bash
# Todos los tests
python -m pytest tests/ -v

# Tests específicos
python -m pytest tests/test_etl.py::TestETL::test_limpieza_datos -v

# Con cobertura de código
python -m pytest tests/ --cov=src --cov-report=html
🔧 Solución de Problemas Comunes
Error: "numpy.dtype size changed"
bash
# Desinstalar y reinstalar con versiones compatibles
pip uninstall numpy pandas -y
pip install numpy==1.24.3 pandas==2.0.3
Error: "No module named 'src'"
bash
# Ejecutar desde la carpeta raíz del proyecto
cd /ruta/a/mini-etl-project
python -m pytest tests/test_etl.py
Error en PowerShell: "No se puede cargar el script"
bash
# Ejecutar como administrador en PowerShell
Set-ExecutionPolicy -ExecutionPolicy RemoteSigned -Scope CurrentUser

# O usar Command Prompt
venv\Scripts\activate.bat