from fastapi import FastAPI
from fastapi.responses import FileResponse
import os

from app.extractor import extraer_tabla_completa
from app.processor import procesar_datos
from app.generator import creat_reporte
from contextlib import asynccontextmanager
from app.scheduler import iniciar_scheduler_background

@asynccontextmanager
async def lifespan(app: FastAPI):
    #se ejecuta al iniciar fastAPI
    iniciar_scheduler_background()
    yield   #permanece en ejecución

    from app.scheduler import detener_scheduler
    detener_scheduler()

app = FastAPI(
    title= "Reporte automatizado",
    description= "API para generar reportes de población mundial",
    version= "1.0.0",
    lifespan=lifespan
)

@app.get("/")
def inicio():
    """endpoint de bienvenida"""
    return {
        "mensaje": "Sistema de automatización - API activa",
        "endpoints_disponibles": {
            "/generar-reporte": "Genera y descarga reporte de población mundial",
            "/docs": "Documentación interactiva"
        }
    }

@app.get("/generar-reporte-ahora")
def generar_ahora():
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
        ruta_reporte = creat_reporte(df_limpio)

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