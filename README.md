# Pipeline de Datos – Caso TNR

**Prueba de concepto funcional · Evaluación Parcial EP5**
**Asignatura:** DAY1102 – Administración y Almacenamiento de Datos
**Año:** 2025

> Implementación de un pipeline de datos que adquiere, explora, limpia, transforma, integra, almacena y despliega información del caso semestral TNR para apoyar la toma de decisiones del negocio.

---

## Tabla de contenidos

1. [Descripción del proyecto](#1-descripción-del-proyecto)
2. [Contexto y problema abordado](#2-contexto-y-problema-abordado)
3. [Patrón de proceso de datos implementado](#3-patrón-de-proceso-de-datos-implementado)
4. [Arquitectura del pipeline](#4-arquitectura-del-pipeline)
5. [Tecnologías utilizadas](#5-tecnologías-utilizadas)
6. [Estructura del repositorio](#6-estructura-del-repositorio)
7. [Requisitos e instalación](#7-requisitos-e-instalación)
8. [Ejecución](#8-ejecución)
9. [Etapas del pipeline](#9-etapas-del-pipeline)
10. [Controles de calidad de datos](#10-controles-de-calidad-de-datos)
11. [Salida funcional para el negocio](#11-salida-funcional-para-el-negocio)
12. [Evaluación de resultados](#12-evaluación-de-resultados)
13. [Limitaciones](#13-limitaciones)
14. [Recomendaciones de mejora](#14-recomendaciones-de-mejora)
15. [Equipo](#15-equipo)

---

## 1. Descripción del proyecto

Este repositorio contiene la tercera entrega integradora del caso semestral. Materializa el diseño de la etapa anterior en una **solución funcional** que:

- Ingiere datos desde múltiples formatos (**CSV, JSON y Excel**).
- Explora y perfila los datos de origen.
- Aplica procesos de limpieza y transformación.
- Consolida un **dataset maestro** integrado.
- Persiste los resultados en **Parquet** (PyArrow) y en una base de datos **SQLite** (SQLAlchemy).
- Genera salidas útiles para el negocio mediante **consultas SQL** y **visualizaciones en Plotly**.

## 2. Contexto y problema abordado

<!-- Completar con el contexto específico del caso TNR -->

- **Organización / caso:** TNR
- **Problema de negocio:** _[Describir el problema que se busca resolver]_
- **Preguntas de negocio que responde la solución:**
  1. _[Pregunta 1]_
  2. _[Pregunta 2]_
  3. _[Pregunta 3]_

## 3. Patrón de proceso de datos implementado

Se implementa un patrón **ETL por capas** (inspirado en la arquitectura *medallion*):

| Capa | Propósito | Formato |
|------|-----------|---------|
| **Raw / Bronze** | Datos originales tal como llegan de las fuentes, sin modificar | CSV, JSON, Excel |
| **Clean / Silver** | Datos limpios, tipados, normalizados y validados | Parquet |
| **Curated / Gold** | Dataset maestro integrado y tablas listas para el consumo analítico | Parquet + SQLite |

**Justificación:** separar las capas permite trazabilidad (siempre se puede volver al dato original), reprocesamiento sin pérdida de información y una clara separación de responsabilidades entre ingesta, transformación y consumo.

<!-- Ajustar si el equipo usó ELT u otro patrón -->

## 4. Arquitectura del pipeline

```
┌──────────────┐   ┌─────────────┐   ┌──────────────┐   ┌──────────────┐   ┌───────────────┐   ┌──────────────┐
│   FUENTES    │──▶│   INGESTA   │──▶│ EXPLORACIÓN  │──▶│  LIMPIEZA Y  │──▶│ INTEGRACIÓN   │──▶│ PERSISTENCIA │
│ CSV/JSON/XLSX│   │   (Pandas)  │   │ Y PERFILADO  │   │TRANSFORMACIÓN│   │DATASET MAESTRO│   │Parquet/SQLite│
└──────────────┘   └─────────────┘   └──────────────┘   └──────────────┘   └───────────────┘   └──────┬───────┘
                                                                                                    │
                                                                                                    ▼
                                                                                          ┌──────────────────┐
                                                                                          │   DESPLIEGUE     │
                                                                                          │ SQL · Plotly ·   │
                                                                                          │ Reportes         │
                                                                                          └──────────────────┘
```

## 5. Tecnologías utilizadas

| Herramienta | Uso en el proyecto |
|-------------|--------------------|
| **Python 3.10+** | Lenguaje principal del pipeline |
| **Google Colab / Jupyter** | Entorno de desarrollo y ejecución |
| **Pandas** | Ingesta, exploración, limpieza y transformación |
| **PyArrow** | Lectura y escritura en formato Parquet |
| **SQLite** | Motor de base de datos para la capa de consumo |
| **SQLAlchemy** | Conexión y carga de DataFrames a SQLite |
| **SQL** | Consultas analíticas sobre los datos persistidos |
| **Plotly** | Visualizaciones interactivas para el negocio |
| **openpyxl** | Lectura de archivos Excel |

## 6. Estructura del repositorio

```
proyecto-tnr/
├── README.md
├── requirements.txt
├── data/
│   ├── raw/                 # Datos originales (CSV, JSON, Excel) — no se modifican
│   ├── processed/           # Datos limpios en Parquet (capa silver)
│   └── curated/             # Dataset maestro en Parquet (capa gold)
├── db/
│   └── tnr.db               # Base de datos SQLite
├── notebooks/
│   ├── 01_ingesta.ipynb
│   ├── 02_exploracion_perfilado.ipynb
│   ├── 03_limpieza_transformacion.ipynb
│   ├── 04_integracion_dataset_maestro.ipynb
│   ├── 05_persistencia.ipynb
│   └── 06_visualizacion_reportes.ipynb
├── src/                     # Funciones reutilizables (opcional)
│   ├── ingesta.py
│   ├── limpieza.py
│   ├── calidad.py
│   └── persistencia.py
├── sql/
│   └── consultas.sql        # Consultas analíticas de negocio
├── outputs/
│   ├── graficos/            # Visualizaciones exportadas (HTML/PNG)
│   └── reportes/            # Tablas consolidadas y reportes
└── docs/
    └── informe_EP5.pdf      # Informe final de la entrega
```

## 7. Requisitos e instalación

### Opción A: Google Colab (recomendada)

1. Subir la carpeta del proyecto a Google Drive.
2. Abrir los notebooks desde Colab.
3. Montar Drive en la primera celda:
   ```python
   from google.colab import drive
   drive.mount('/content/drive')
   ```
4. Instalar dependencias faltantes:
   ```python
   !pip install pyarrow sqlalchemy plotly openpyxl
   ```

### Opción B: Entorno local

```bash
git clone <url-del-repositorio>
cd proyecto-tnr
python -m venv venv
source venv/bin/activate        # En Windows: venv\Scripts\activate
pip install -r requirements.txt
jupyter lab
```

**`requirements.txt`**
```
pandas>=2.0
pyarrow>=14.0
sqlalchemy>=2.0
plotly>=5.0
openpyxl>=3.1
jupyterlab
```

## 8. Ejecución

Ejecutar los notebooks **en orden numérico**, ya que cada etapa depende de la salida de la anterior:

| Paso | Notebook | Entrada | Salida |
|------|----------|---------|--------|
| 1 | `01_ingesta` | `data/raw/*` | DataFrames en memoria |
| 2 | `02_exploracion_perfilado` | DataFrames crudos | Reporte de perfilado |
| 3 | `03_limpieza_transformacion` | DataFrames crudos | `data/processed/*.parquet` |
| 4 | `04_integracion_dataset_maestro` | `data/processed/` | `data/curated/dataset_maestro.parquet` |
| 5 | `05_persistencia` | Dataset maestro | `db/tnr.db` |
| 6 | `06_visualizacion_reportes` | `db/tnr.db` | `outputs/` |

## 9. Etapas del pipeline

### 9.1 Ingesta
Carga de las fuentes del caso TNR en sus distintos formatos:

```python
import pandas as pd

df_csv   = pd.read_csv("data/raw/<archivo>.csv")
df_json  = pd.read_json("data/raw/<archivo>.json")
df_excel = pd.read_excel("data/raw/<archivo>.xlsx", sheet_name=None)  # todas las hojas
```

| Fuente | Formato | Registros | Columnas | Descripción |
|--------|---------|-----------|----------|-------------|
| _[nombre]_ | CSV | _[n]_ | _[n]_ | _[contenido]_ |
| _[nombre]_ | JSON | _[n]_ | _[n]_ | _[contenido]_ |
| _[nombre]_ | Excel | _[n]_ | _[n]_ | _[contenido]_ |

### 9.2 Exploración y perfilado
- Dimensiones (`shape`), tipos de datos (`dtypes`) e `info()`.
- Estadísticas descriptivas (`describe()`).
- Conteo y porcentaje de valores nulos por columna.
- Detección de duplicados.
- Valores únicos y cardinalidad de variables categóricas.
- Identificación de outliers e inconsistencias de formato.

### 9.3 Limpieza y transformación
- Estandarización de nombres de columnas (minúsculas, sin tildes ni espacios).
- Conversión y corrección de tipos de datos (fechas, numéricos, categorías).
- Tratamiento de valores nulos (eliminación, imputación o marcado, según el caso).
- Eliminación de registros duplicados.
- Normalización de textos y categorías (mayúsculas, espacios, sinónimos).
- Tratamiento de outliers.
- Creación de variables derivadas (_[ej.: año, mes, totales, márgenes]_).

### 9.4 Integración y dataset maestro
- Identificación de llaves comunes entre fuentes.
- Unión mediante `merge` / `concat` con validación de cardinalidad (`validate=`).
- Verificación de que no se pierdan ni dupliquen registros tras la integración.
- Resultado: `dataset_maestro.parquet`.

### 9.5 Persistencia y almacenamiento

```python
# Parquet con PyArrow
df_maestro.to_parquet("data/curated/dataset_maestro.parquet", engine="pyarrow", index=False)

# SQLite con SQLAlchemy
from sqlalchemy import create_engine
engine = create_engine("sqlite:///db/tnr.db")
df_maestro.to_sql("dataset_maestro", engine, if_exists="replace", index=False)
```

**Justificación:** Parquet ofrece almacenamiento columnar comprimido y eficiente para análisis; SQLite permite consultar los datos con SQL estándar sin necesidad de un servidor.

### 9.6 Consulta y despliegue

```python
query = """
SELECT <dimension>, SUM(<metrica>) AS total
FROM dataset_maestro
GROUP BY <dimension>
ORDER BY total DESC;
"""
resultado = pd.read_sql(query, engine)
```

## 10. Controles de calidad de datos

| Dimensión | Control aplicado | Resultado |
|-----------|------------------|-----------|
| **Completitud** | % de nulos por columna antes y después de la limpieza | _[valor]_ |
| **Unicidad** | Conteo de duplicados por llave primaria | _[valor]_ |
| **Validez** | Rangos válidos, formatos de fecha y dominios de categorías | _[valor]_ |
| **Consistencia** | Coherencia entre fuentes y entre campos relacionados | _[valor]_ |
| **Integridad** | Conteo de registros antes y después de cada etapa | _[valor]_ |

## 11. Salida funcional para el negocio

- **Visualizaciones Plotly:** _[listar gráficos y qué pregunta de negocio responde cada uno]_
- **Consultas SQL:** _[listar consultas clave en `sql/consultas.sql`]_
- **Tablas consolidadas / reportes:** _[listar archivos generados en `outputs/`]_

## 12. Evaluación de resultados

_[Resumir los hallazgos principales, indicadores obtenidos y cómo responden a las preguntas de negocio planteadas en la sección 2.]_

## 13. Limitaciones

- Ejecución manual y secuencial de notebooks (sin orquestación automática).
- SQLite no está diseñado para alta concurrencia ni grandes volúmenes.
- Datos estáticos: no hay ingesta incremental ni en tiempo real.
- _[Otras limitaciones propias del caso]_

## 14. Recomendaciones de mejora

| Mejora | Beneficio |
|--------|-----------|
| Orquestar el pipeline (Airflow, Prefect o similar) | Automatización y programación de ejecuciones |
| Migrar a un motor como PostgreSQL o un data warehouse en la nube | Escalabilidad y acceso concurrente |
| Implementar carga incremental | Menor tiempo de procesamiento |
| Validaciones automáticas de calidad (p. ej. Great Expectations) | Detección temprana de errores |
| Dashboard interactivo (Dash, Streamlit o Power BI) | Mejor experiencia para usuarios de negocio |
| Logging y monitoreo | Trazabilidad operativa |
| Control de versiones de datos | Reproducibilidad |

## 15. Equipo

| Integrante | Rol / responsabilidad |
|------------|-----------------------|
| _[Nombre 1]_ | _[Ingesta y perfilado]_ |
| _[Nombre 2]_ | _[Limpieza, transformación e integración]_ |
| _[Nombre 3]_ | _[Persistencia, visualización y documentación]_ |

**Docente:** _[Nombre]_
**Sección:** _[Sección]_

---

*Proyecto desarrollado con fines académicos para la asignatura DAY1102 – Administración y Almacenamiento de Datos.*
