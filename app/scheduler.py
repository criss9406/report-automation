# scheduler.py

import schedule
import time
from datetime import datetime
from threading import Thread, Event

# Variables globales
_scheduler_stop_event = Event()
_scheduler_thread = None
_scheduler_enabled = False  # ← NUEVO: Control de activación


def ejecutar_pipeline():
    """Ejecuta el pipeline completo de forma automática."""
    try:
        print("\n" + "=" * 50)
        print(f"🕐 Inicio de ejecución automática: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("=" * 50 + "\n")
        
        from app.extractor import extraer_tabla_completa
        from app.processor import procesar_datos
        from app.generator import crear_reporte
        
        # PASO 1: Extraer
        print("📥 Extrayendo datos...")
        datos_raw = extraer_tabla_completa()
        
        if not datos_raw:
            print("❌ Error: No se pudieron extraer datos")
            return
        
        print(f"✅ Extraídos {len(datos_raw)} registros")
        
        # PASO 2: Procesar
        print("🔄 Procesando datos...")
        df_limpio = procesar_datos(datos_raw)
        print(f"✅ Datos procesados: {df_limpio.height} países")
        
        # PASO 3: Generar reporte
        print("📄 Generando reporte...")
        ruta_reporte = crear_reporte(df_limpio)
        print(f"✅ Reporte generado: {ruta_reporte}")
        
        print("\n" + "=" * 50)
        print("✅ Pipeline completado exitosamente")
        print("=" * 50 + "\n")
        
    except Exception as e:
        print(f"\n❌ Error en pipeline automático: {str(e)}\n")


def iniciar_scheduler():
    """Configura y ejecuta el scheduler en un loop."""
    global _scheduler_enabled
    
    # Configurar tarea diaria
    schedule.every().day.at("08:00").do(ejecutar_pipeline)
    
    print("🕐 Scheduler iniciado")
    print("📅 Tarea programada: Diariamente a las 08:00 AM")
    print(f"📊 Estado: {'ACTIVO' if _scheduler_enabled else 'PAUSADO'}")
    
    while not _scheduler_stop_event.is_set():
        if _scheduler_enabled:  # ← NUEVO: Solo ejecuta si está habilitado
            schedule.run_pending()
        time.sleep(60)
    
    print("🛑 Scheduler detenido correctamente")


def iniciar_scheduler_background():
    """Inicia el scheduler en un thread de background."""
    global _scheduler_thread
    
    _scheduler_stop_event.clear()
    _scheduler_thread = Thread(target=iniciar_scheduler, daemon=True)
    _scheduler_thread.start()
    print("✅ Scheduler corriendo en background")


def detener_scheduler():
    """Detiene el scheduler de forma limpia."""
    global _scheduler_thread
    
    if _scheduler_thread and _scheduler_thread.is_alive():
        print("🛑 Deteniendo scheduler...")
        _scheduler_stop_event.set()
        _scheduler_thread.join(timeout=5)
        print("✅ Scheduler detenido")


# ← NUEVO: Funciones de control
def activar_scheduler():
    """Activa la ejecución programada del scheduler."""
    global _scheduler_enabled
    _scheduler_enabled = True
    print("✅ Scheduler ACTIVADO")


def desactivar_scheduler():
    """Desactiva la ejecución programada del scheduler."""
    global _scheduler_enabled
    _scheduler_enabled = False
    print("⏸️ Scheduler PAUSADO")


def obtener_estado_scheduler():
    """Retorna el estado actual del scheduler."""
    return {
        "activo": _scheduler_enabled,
        "thread_corriendo": _scheduler_thread.is_alive() if _scheduler_thread else False,
        "proxima_ejecucion": str(schedule.next_run()) if schedule.jobs else "No programada"
    }