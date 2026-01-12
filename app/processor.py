#procesor.py

import polars as pl

def procesar_datos(datos_crudos):
    """
    Convierte datos crudos en DataFrame limpio.
    """
    print("🧮 Procesando datos con Polars...")
    
    df = pl.DataFrame(datos_crudos)
    print(f"   - Registros cargados: {len(df)}")
    
    # Limpiar poblacion_2023
    df = df.with_columns([
        pl.col("poblacion_2023")
            .str.replace_all(",", "", literal=True)
            .cast(pl.Int64, strict=False)
            .alias("pob_2023_limpia")
    ])
    
    # Limpiar poblacion_2024
    df = df.with_columns([
            pl.col("poblacion_2024")
            .str.replace_all(",", "", literal=True)
            .cast(pl.Int64, strict=False)
            .alias("pob_2024_limpia")
    ])
    
    # Limpiar cambio_porcentual
    df = df.with_columns([
        pl.col("cambio_porcentual")
            .str.replace_all("%", "", literal=True)
            .str.replace_all("+", "", literal=True)
            .str.replace_all("−", "-", literal=True)
            .cast(pl.Float64, strict=False)
            .alias("cambio_limpio")
    ])
    
    # Reemplazar columnas originales
    df = df.select([
        "pais",
        "continente", 
        "region",
        pl.col("pob_2023_limpia").alias("poblacion_2023"),
        pl.col("pob_2024_limpia").alias("poblacion_2024"),
        pl.col("cambio_limpio").alias("cambio_porcentual")
    ])
    
    # Filtrar registros válidos
    df_limpio = df.filter(
        pl.col("poblacion_2024").is_not_null()
    )
    
    registros_eliminados = len(df) - len(df_limpio)
    
    print(f"   - Registros válidos: {len(df_limpio)}")
    print(f"   - Registros eliminados: {registros_eliminados}")
    
    # Ordenar por población descendente
    df_limpio = df_limpio.sort("poblacion_2024", descending=True)
    
    print("✅ Procesamiento completado\n")
    
    return df_limpio

