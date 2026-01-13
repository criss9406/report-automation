import logging
import sys
import json

from logging import handlers
from pathlib import Path
from datetime import datetime
from logging.handlers import RotatingFileHandler

def configurar_logger(nombre_modulo:str) -> logging.Logger:
    """
    Configura un logger con salida a consola y archivo rotativo.
    
    Args:
        nombre_modulo: Nombre del módulo que usa el logger (ej: 'extractor')
    
    Returns:
        Logger configurado
    """
    #crear directorio de logs si no existe
    Path("logs").mkdir(exist_ok=True)

    #crear logger específico para el módulo
    logger = logging.getLogger(nombre_modulo)
    logger.setLevel(logging.INFO)

    #evitar duplicar handlers si ya existe
    if logger.handlers:
        return logger

    #formaro detallado con timesamp
    formato = logging.Formatter(
        '%(asctime)s | %(name)-12s | %(levelname)-8s | %(message)s',
        datefmt = '%Y-%m-%d %H:%M:%S'
    )

    # handler 1: consola (para railway logs en vivo)
    handler_consola = logging.StreamHandler(sys.stdout)
    handler_consola.setLevel(logging.INFO)
    handler_consola.setFormatter(formato)

    #handler 2: archivo rotativo (persistencia local)
    handler_archivo = RotatingFileHandler(
        filename='logs/app.log',
        maxBytes=5*1024*1024,  #5MB
        backupCount=10,
        encoding='utf-8'
    )

    handler_archivo.setLevel(logging.INFO)
    handler_archivo.setFormatter(formato)

    #agregar ambos handlers
    logger.addHandler(handler_consola)
    logger.addHandler(handler_archivo)

    return logger


def log_ejecución_pipeline(
    estado:str,
    duracion_seg:float,
    error:str = None
):
    """
    Registra resultado de ejecución del pipeline en archivo JSON.
    Útil para dashboard o análisis histórico.
    
    Args:
        estado: 'EXITOSO' | 'FALLIDO' | 'PARCIAL'
        duracion_seg: Tiempo de ejecución en segundos
        error: Mensaje de error si hubo fallo
    """

    archivo_historial = Path("logs/historial_ejecuciones.json")

    #cargar historial existente
    if archivo_historial.exists():
        with open(archivo_historial, 'r', encoding='utf-8') as f:
            historial = json.load(f)
    else:
        historial = []

    #crear registro de esta ejecución
    registro = {
        "timestamp": datetime.now().isoformat(),
        "estado": estado,
        "duracion_segundos": duracion_seg,
        "error": error
    }

    #agregar al historial (mantener últimos 100)
    historial.append(registro)
    historial = historial[-100:]

    #guardar
    with open(archivo_historial, 'w', encoding='utf-8') as f:
        json.dump(historial, f, indent=2, ensure_ascii=False)

    