from sched import Event
import schedule
import time
from datetime import datetime
from threading import Thread, Event

from app.extractor import extraer_tabla_completa
from app.processor import procesar_datos
from app.generator import creat_reporte

_scheduler_stop_event = Event()
_scheduler_thread = None

def ejecutar_pipeline():

    try:   
        print(f"\n{'=' * 50}")
        print(f"🟢inicio de ejecución automática: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}")
        print(f"\n{'=' * 50}")

        print("🟢extrayendo datos ...")
        datos_raw = extraer_tabla_completa()

        if not datos_raw:
            print("❌ error: no se puedieron extraer los datos")
            return

        print(f"✅ {len(datos_raw)} datos extraidos")

        print("🟢extrayendo datos ...")
        df_limpio = procesar_datos(datos_raw)
        print(f"✅ datos procesados: {len(df_limpio)}")
        
        print("🟢generando reporte ...")
        ruta_reporte = creat_reporte(df_limpio)
        print(f"✅ reporte generado en: {ruta_reporte}")

        print(f"\n{'=' * 50}")
        print(f"✅ proceso completado exitosamente")
        print(f"\n{'=' * 50}")

    except Exception as e:
        print(f"\n ❌ Error en automatización: {str(e)}")

def iniciar_scheduler():

    schedule.every(5).minutes.do(ejecutar_pipeline)

    print("🟢scheduler iniciado")

    while not _scheduler_stop_event.is_set():
        schedule.run_pending()
        time.sleep(2)

    print("🔴 scheduler detenido correctamente")

def iniciar_scheduler_background():

    global _scheduler_thread

    _scheduler_stop_event.clear()
    _scheduler_thread = Thread(target=iniciar_scheduler, daemon=True)
    _scheduler_thread.start()
    print("🟢scheduler corriendo en background")

def detener_scheduler():

    global _scheduler_thread

    if _scheduler_thread and _scheduler_thread.is_alive():
        print("🔴 deteniendo scheduler")
        _scheduler_stop_event.set()
        _scheduler_thread.join(timeout=5)
        print("🟢 scheduler detenido")




