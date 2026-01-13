#procesor.py

from logging import Logger
from sys import exception
import polars as pl
from app.logger import configurar_logger

logger = configurar_logger('processor')

def procesar_datos(datos_crudos):
    """
    Convierte datos crudos en DataFrame limpio.

    Args:
        datos_crudos: Lista de diccionarios con datos de países
        
    Returns:
        pl.DataFrame: DataFrame limpio y ordenado
        None: Si hay error crítico
    """
    
    try:
        logger.info("=" * 60)
        logger.info("INICIANDO PROCESAMIENTO DE DATOS")
        Logger.info("=" * 60)

        if not datos_crudos:
            logger.error("❌ datos crudos está vacío o es None")
            return None

        logger.info(" Creando DataFrame con polars")
        df = pl.DataFrame(datos_crudos)
        registros_iniciales = len(df)
        logger.info(f"  registros cargados: {registros_iniciales}")

        logger.info("limpienado columna 'población_2023'...")
        df = df.with_columns([
        pl.col("poblacion_2023")
            .str.replace_all(",", "", literal=True)
            .cast(pl.Int64, strict=False)
            .alias("pob_2023_limpia")
        ])

        logger.info("limpienado columna 'población_2024'...")
        df = df.with_columns([
            pl.col("poblacion_2024")
            .str.replace_all(",", "", literal=True)
            .cast(pl.Int64, strict=False)
            .alias("pob_2024_limpia")
        ])

        
        df = df.with_columns([
        pl.col("cambio_porcentual")
            .str.replace_all("%", "", literal=True)
            .str.replace_all("+", "", literal=True)
            .str.replace_all("−", "-", literal=True)
            .cast(pl.Float64, strict=False)
            .alias("cambio_limpio")
        ])

        logger.info("reorganizando columnas finales...")
        df = df.select([
        "pais",
        "continente", 
        "region",
        pl.col("pob_2023_limpia").alias("poblacion_2023"),
        pl.col("pob_2024_limpia").alias("poblacion_2024"),
        pl.col("cambio_limpio").alias("cambio_porcentual")
        ])

        logger.info("filtrando registros válidos...")
        df_limpio = df.filter(
            pl.col("poblacion_2024").is_not_null()
        )
        
        registros_eliminados = len(df) - len(df_limpio)
        tasa_retencion = (len(df_limpio)/registros_iniciales * 100) if registros_iniciales > 0 else 0

        logger.info(f"   - Registros válidos: {len(df_limpio)}")
        logger.info(f"   - Registros eliminados: {registros_eliminados}")
        logger.info(f"   - tasa de retencion: {tasa_retencion}")

        logger.info("ordenando registro de población...")
        df_limpio = df_limpio.sort("poblacion_2024", descending=True)

        #validar que hay datos luego de la limpieza
        if len(df_limpio) == 0:
            logger.error("❌    Crítico: no quedaron registros válidos después de ña limpieza")
            return None

        logger.info("=" * 60)
        logger.info("PROCESAMIENTO COMPLETADO EXITOSAMENTE")
        logger.info("=" * 60)

        return df_limpio

    except  exception as e:
        logger.info("=" * 60)
        logger.info("❌  ERROR CRÍTICO DE PROCESAMIENTO")
        logger.info(f"   tipo: {type(e).__name__}")
        logger.info(f"   detalle: {str(e)}")
        logger.info("=" * 60)
        return None
        

    

