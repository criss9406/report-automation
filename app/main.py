# main.py

from logging import Logger
from sys import exception
from fastapi import FastAPI
from fastapi.responses import FileResponse
from contextlib import asynccontextmanager
import os
from pathlib import Path
import json

from app.extractor import extraer_tabla_completa
from app.processor import procesar_datos
from app.generator import crear_reporte
from app.scheduler import (
    iniciar_scheduler_background, 
    ejecutar_pipeline,
    detener_scheduler,
    activar_scheduler,      
    desactivar_scheduler,   
    obtener_estado_scheduler 
)

from app.logger import configurar_logger

#configurar logger
logger = configurar_logger('main')



@asynccontextmanager
async def lifespan(app: FastAPI):
    # Se ejecuta al iniciar
    logger.info("=" * 50)
    logger.info("🟢 INICIANDO APLICACIÓN")
    logger.info("=" * 50)

    iniciar_scheduler_background()
    logger.info("   scheduler iniciado en modo PAUSADO por defecto")
    logger.info("   usa POST/scheduler/activar para habilitar ejecución automática")
    
    yield
    
    logger.info("=" * 50)
    logger.info("🔴 CERRANDO APLICACIÓN")
    logger.info("=" * 50)
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
    Logger.info("   endpoint '/encendido' activado")
    
    return {
        "mensaje": "Sistema de automatización - API activa",
        "version": "1.0.0",
        "endpoints_disponibles": {
            "/generar-reporte": "GET - Genera y descarga reporte de población mundial",
            "/generar-reporte-ahora": "GET - Ejecuta pipeline manualmente (sin esperar horario)",
            "/scheduler/estado": "GET - Consulta estado del scheduler",
            "/scheduler/activar": "POST - Activa ejecución automática diaria",
            "/scheduler/desactivar": "POST - Pausa ejecución automática",
            "/health": "GET - health check del sistema",
            "/docs": "GET - Documentación interactiva"
        }
    }


@app.get("/health")
def health_check():
    """
    Health check para validar el estado del sistema
    util para railway y monitoreo externo
    """
    logger.info("   health check solicitado")

    try:
        Path("logs").mkdir(exist_ok=True)
        Path("outputs").mkdir(exist_ok=True)

        estado_scheduler = obtener_estado_scheduler()

        "verifica historial de ejecuciones"
        archivo_historial = None

        if archivo_historial.exists():
            with open(archivo_historial, 'r') as f:
                historial = json.load(f)
                if historial:
                    ultima_ejecucion = historial[-1]

        return {
            "status": "healthy",
            "timestamp": str(__import__('datetime').datetime.now()),
            "scheduler": estado_scheduler,
            "ultima_ejecucion": ultima_ejecucion,
            "carpeta_ok": True
        }

    except Exception as e:
        logger.info(f"❌ healt check falló: {str(e)}")
        return {
            "status": "unhealthy",
            "error": str(e)
        }

@app.get("/scheduler/estado")
def estado_scheduler():
    """Consulta el estado actual del scheduler."""
    logger.info(" consultando el estado actual del scheduler")
    return obtener_estado_scheduler()


@app.post("/scheduler/activar")
def activar():
    """Activa la ejecución automática del scheduler."""
    logger.info(" activando scheduler")
    activar_scheduler()
    return {
        "mensaje": "Scheduler activado - se ejecutará a las 8:00 am diariamente",
        "estado": obtener_estado_scheduler()
    }


@app.post("/scheduler/desactivar")
def desactivar():
    """Desactiva la ejecución automática del scheduler."""
    logger.info(" desactivando scheduler...")
    desactivar_scheduler()

    return {
        "mensaje": "Scheduler desactivado - no se ejecutará hasta reactivar",
        "estado": obtener_estado_scheduler()
    }


@app.get("/generar-reporte-ahora")
def generar_ahora():
    """Ejecuta el pipeline inmediatamente, sin esperar el horario programado"""
    logger.info(" ejecución manual del pipeline solicitada")

    try:
        ejecutar_pipeline()

        return {
            "mensaje": "Pipeline ejecutado. Revisa outputs/ para el reporte.",
            "detalle": "revisar outputs/ para el reporte o consultar logs/ para detalle"
        }
    
    except exception as e:
        logger.error(f"❌ Error de ejecución manual: {str(e)}")

        return{
            "error": "Error al ejecutar pipeline",
            "detalle": str(e)
        }


@app.get("/generar-reporte")
def generar_reporte_completo():
    """
    endpiont principal: ejecutal el pipeline completo y devuelve el archivo
    """

    logger.info("=" * 50)
    logger.info("SOLICITUD DE GENERACIÓN DE REPORTE RECIBIDA")
    logger.info("=" * 50)

    try:
        #PASO 1: extraer
        logger.info("extrayendo datos ...")
        datos_raw = extraer_tabla_completa()

        if not datos_raw:
            logger.error("❌ no se pudieron extraer datos")
            return {"error": "no se pudieron extraer datos de la web"}

        #PASO 2: procesar
        logger.info("procesando datos con polars...")
        df_limpio = procesar_datos(datos_raw)

        if df_limpio is None or df_limpio.height == 0:
            logger.error("❌ procesamiento falló")
            return {"error": "Error al procesar datos"}

        #PASO 3: generar reporte
        logger.info("generando reporte")
        ruta_reporte = crear_reporte(df_limpio)

        if not ruta_reporte or not os.path.exists(ruta_reporte):
            logger.error("❌ el archivo no se generó correctamente")
            return{"error": "el archivo no se generó correctamente"}

        logger.info(f"reporte listo para descarga: {ruta_reporte}")

        return FileResponse(
            path= ruta_reporte,
            filename= os.path.basename(ruta_reporte),
            media_type="application/vnd.openxmlformats-officedocument.wordprocessingml.document"
        )

    except Exception as e:
        logger.error(f"❌ Error crítico: {str(e)}, exc_info=True")
        return {
            "error": "error al generar reporte",
            "detalle": str(e)
        }

