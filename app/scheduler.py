# scheduler.py

import schedule
import time
from datetime import datetime
from threading import Thread, Event
from app import logger

from app.extractor import extraer_tabla_completa
from app.processor import procesar_datos
from app.generator import crear_reporte
from app.logger import configurar_logger, log_ejecución_pipeline

logger = configurar_logger('scheduler')

# Variables globales
_scheduler_stop_event = Event()
_scheduler_thread = None
_scheduler_enabled = False  

def ejecutar_pipeline():
    """
    Ejecuta el pipeline completo de forma automática.
    Registra métricas de ejecución.
    """
    tiempo_inicio = time.time()
    estado = 'FALLIDO'
    mensaje_error = None

    try:
        logger.info("\n" + "=" * 50)
        logger.info("EJECUCIÓN AUTOMÁTICA INICIADA")
        logger.info(f"🕐 Inicio de ejecución automática: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info("=" * 50 + "\n")
        
        # PASO 1: Extraer
        logger.info("📥 PASO 1/3: Extrayendo datos...")
        datos_raw = extraer_tabla_completa()
        
        if not datos_raw:
            logger.error("❌ Extracción falló: datos_raw es None o esta vacío")
            return
        
        logger.info(f"✅ Extraídos {len(datos_raw)} registros\n")
        
        # PASO 2: Procesar
        logger.info("🔄 PASO 2/3: Procesando datos...")
        df_limpio = procesar_datos(datos_raw)

        if df_limpio is None or df_limpio.height == 0:
            logger.error(f"❌ procesamiento falló: DF vacío o None")
            return

        logger.info("🔄 reporte generado: Procesando datos...")

        # PASO 3: Generar reporte
        logger.info("📄 Generando reporte...")
        ruta_reporte = crear_reporte(df_limpio)
        logger.info(f"✅ Reporte generado: {ruta_reporte}")

        estado = 'EXITOSO'

        duracion = time.time() - tiempo_inicio        
        logger.info("=" * 50)
        logger.info("✅ Pipeline completado exitosamente")
        logger.info(f"      duración total: {duracion:.2f} segundos")
        logger.info("=" * 50 + "\n")
        
    except Exception as e:
        logger.error("=" * 50)
        logger.error("❌ ERROR CRÍTICO EN PIPELINE AUTOMÁTICO")
        logger.error(f"     tipo: {type(e).__name__}")
        logger.error(f"     detalle: {str(e)}")
        logger.error("=" * 50)

    finally:
        #siempre regustrar la ejecución
        duracion_final = time.time() - tiempo_inicio
        log_ejecución_pipeline(
            estado=estado,
            duracion_seg=round(duracion_final,2),
            error=mensaje_error
        )
        logger.info(f"   ejecución registrada en historial: {estado}")


def iniciar_scheduler():
    """Configura y ejecuta el scheduler en un loop."""
    global _scheduler_enabled
    
    # Configurar tarea diaria
    schedule.every().day.at("08:00").do(ejecutar_pipeline)
    
    logger.error("=" * 50)
    logger.info("🕐 Scheduler iniciado")
    logger.info("📅 Tarea programada: Diariamente a las 08:00 AM")
    logger.info(f"📊 Estado: {'ACTIVO' if _scheduler_enabled else 'PAUSADO'}")
    logger.error("=" * 50)
    
    while not _scheduler_stop_event.is_set():
        if _scheduler_enabled:  # ← NUEVO: Solo ejecuta si está habilitado
            schedule.run_pending()
        time.sleep(60)
    
    logger.info("🛑 Scheduler detenido correctamente")


def iniciar_scheduler_background():
    """Inicia el scheduler en un thread de background."""
    global _scheduler_thread
    
    _scheduler_stop_event.clear()
    _scheduler_thread = Thread(target=iniciar_scheduler, daemon=True)
    _scheduler_thread.start()
    logger.info("✅ Scheduler corriendo en background (thread daemon)")


def detener_scheduler():
    """Detiene el scheduler de forma limpia."""
    global _scheduler_thread
    
    if _scheduler_thread and _scheduler_thread.is_alive():
        logger.info("🛑 Deteniendo scheduler...")
        _scheduler_stop_event.set()
        _scheduler_thread.join(timeout=5)
        logger.info("✅ Scheduler detenido")



def activar_scheduler():
    """Activa la ejecución programada del scheduler."""
    global _scheduler_enabled
    _scheduler_enabled = True
    logger.info("✅ Scheduler ACTIVADO - se ejecutará a las 8:00 am diariamente")


def desactivar_scheduler():
    """Desactiva la ejecución programada del scheduler."""
    global _scheduler_enabled
    _scheduler_enabled = False
    logger.info("⏸️ Scheduler PAUSADO - no se ejcutará hasta ser reactivado")


def obtener_estado_scheduler():
    """Retorna el estado actual del scheduler."""
    proxima_ejecucion = str(schedule.next_run()) if schedule.jobs and _scheduler_enabled else "no programado (en pausa)"
    
    return {
        "activo": _scheduler_enabled,
        "thread_corriendo": _scheduler_thread.is_alive() if _scheduler_thread else False,
        "proxima_ejecucion": proxima_ejecucion
    }