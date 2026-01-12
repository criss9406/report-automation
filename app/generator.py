#generator.py

import enum
from docx import Document
from datetime import datetime
import polars as pl
from pathlib import Path
from polars import dataframe

def crear_reporte(df: dataframe) -> str:
    
    #genera la carpeta outputs si no existe
    Path("outputs").mkdir(exist_ok=True)

    #crear documento
    doc = Document()

    #titulo principal
    titulo = doc.add_heading("reporte de la pobración mundial", 0)

    #fecha
    fecha_actual = datetime.now().strftime("%d/%m/%Y %H:%M")
    doc.add_paragraph(f"fecha de generación: {fecha_actual}")
    doc.add_paragraph()  #espacio en blanco

    #estadísticas globales
    doc.add_heading("Estadísticas globales", 1)

    #calcular estadísticas
    poblacion_total = df["poblacion_2024"].sum()
    poblacion_promedio = df["poblacion_2024"].mean()
    total_paises = df.height

    #crear tabla de estadísticas
    tabla_stats = doc.add_table(rows=4, cols=2)
    tabla_stats.style = 'Light Grid Accent 1'

    #encabezados
    tabla_stats.rows[0].cells[0].text = "métrica"
    tabla_stats.rows[0].cells[1].text = "valor"

    #datos
    tabla_stats.rows[1].cells[0].text = "Población mundial"
    tabla_stats.rows[1].cells[1].text = f"{poblacion_total:,}"

    tabla_stats.rows[2].cells[0].text = "población promedio por país"
    tabla_stats.rows[2].cells[1].text = f"{int(poblacion_promedio):,}"

    tabla_stats.rows[3].cells[0].text = "total de países"
    tabla_stats.rows[3].cells[1].text = str(total_paises)

    doc.add_paragraph()

    #top 10 países
    doc.add_heading("top 10 países por población", 1)

    top10 = df.head(10)

    #crear tabla
    tabla_top = doc.add_table(rows=11, cols= 4)
    tabla_top.style = 'Light Grid Accent 1'

    #encabezados
    encabezados = ["País", "Población 2024", "Cambio %", "Continente"]
    for i,encabezado in enumerate(encabezados):
        tabla_top.rows[0].cells[i].text = encabezado

    #datos
    for idx, dato in enumerate(top10.iter_rows(named=True)):
        tabla_top.rows[idx + 1].cells[0].text = dato['pais']
        tabla_top.rows[idx + 1].cells[1].text = f"{dato['poblacion_2024']:,}"
        tabla_top.rows[idx + 1].cells[2].text = f"{dato['cambio_porcentual']:,}"
        tabla_top.rows[idx + 1].cells[3].text = dato['continente']

    #guardar archivo
    fecha_archivo = datetime.now().strftime("%Y-%m-%d_%H-%M")
    nombre_archivo = f"reporte_población_{fecha_archivo}.docx"
    ruta_completa = f"outputs/{nombre_archivo}"

    doc.save(ruta_completa)

    print("reporte generado...")

    return ruta_completa

