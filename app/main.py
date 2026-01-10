# app/main.py

from fastapi import FastAPI
from fastapi.responses import FileResponse
from contextlib import asynccontextmanager
import os

from app.extractor import extraer_tabla_completa
from app.processor import procesar_datos
from app.generator import crear_reporte
from app.scheduler import (
    iniciar_scheduler_background, 
    detener_scheduler,
    activar_scheduler,      # ← NUEVO
    desactivar_scheduler,   # ← NUEVO
    obtener_estado_scheduler # ← NUEVO
)

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Se ejecuta al iniciar
    iniciar_scheduler_background()
    # ← NUEVO: Scheduler inicia PAUSADO por defecto
    yield
    # Se ejecuta al cerrar
    detener_scheduler()

app = FastAPI(
    title="Reporte automatizado",
    description="API para generar reportes de población mundial",
    version="1.0.0",
    lifespan=lifespan
)


@app.get("/")
def inicio():
    """Endpoint de bienvenida"""
    return {
        "mensaje": "Sistema de automatización - API activa",
        "endpoints_disponibles": {
            "/generar-reporte": "Genera y descarga reporte de población mundial",
            "/generar-reporte-ahora": "Ejecuta pipeline manualmente (sin esperar horario)",
            "/scheduler/estado": "Consulta estado del scheduler",
            "/scheduler/activar": "Activa ejecución automática diaria",
            "/scheduler/desactivar": "Pausa ejecución automática",
            "/docs": "Documentación interactiva"
        }
    }


# ← NUEVO: Endpoints de control del scheduler
@app.get("/scheduler/estado")
def estado_scheduler():
    """Consulta el estado actual del scheduler."""
    return obtener_estado_scheduler()


@app.post("/scheduler/activar")
def activar():
    """Activa la ejecución automática del scheduler."""
    activar_scheduler()
    return {
        "mensaje": "Scheduler activado",
        "estado": obtener_estado_scheduler()
    }


@app.post("/scheduler/desactivar")
def desactivar():
    """Desactiva la ejecución automática del scheduler."""
    desactivar_scheduler()
    return {
        "mensaje": "Scheduler desactivado",
        "estado": obtener_estado_scheduler()
    }


@app.get("/generar-reporte-ahora")
def generar_ahora():
    """Ejecuta el pipeline inmediatamente, sin esperar el horario programado"""
    from app.scheduler import ejecutar_pipeline
    ejecutar_pipeline()
    return {"mensaje": "Pipeline ejecutado. Revisa outputs/ para el reporte."}


@app.get("/generar-reporte")
def generar_reporte_completo():
    
    try:
        print("extrayendo datos ...")
        datos_raw = extraer_tabla_completa()

        if not datos_raw:
            return {"error": "no se pudieron extraer datos de la web"}

        print("procesando datos con polars...")
        df_limpio = procesar_datos(datos_raw)

        print("generando reporte")
        ruta_reporte = crear_reporte(df_limpio)

        if not os.path.exists(ruta_reporte):
            return{"error": "el archivo no se generó correctamente"}

        print("reporte listo para descarga")
        return FileResponse(
            path= ruta_reporte,
            filename= os.path.basename(ruta_reporte),
            media_type="application/vnd.openxmlformats-officedocument.wordprocessingml.document"
        )

    except Exception as e:
        return {
            "error": "error al generar reporte",
            "detalle": str(e)
        }


@app.get("/saludo/{nombre}")
def saludar(nombre: str):
    return {"mensaje": f"hola {nombre}, bienvenido al sistema"}
