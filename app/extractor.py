#extractor.py

from playwright.sync_api import sync_playwright
from app import logger
from app.logger import configurar_logger

#configurar logger para este módulo
logger = configurar_logger('extractor')


def extraer_tabla_completa():
    """"
    extrae los datos de la población mundial desde wikipedia
    
    returns:
        list[dict]: lista de países con sus datos demográficos
        none: si hay error crítico
    """
    
    try:
        logger.info("=" * 60)
        logger.info("iniciando extracción de datos")
        logger.info("=" * 60)

        with sync_playwright() as p:
            logger.info("lanzando navegador chromium...")
            browser = p.chromium.launch(headless=True)
            page = browser.new_page()
            
            url = "https://en.wikipedia.org/wiki/List_of_countries_by_population_(United_Nations)"
            logger.info(f"navegando a: {url}")

            #timeout de 30 segundos para navegación
            page.goto(url, timeout=30000)

            logger.info("esperando datos de tabla...")
            page.wait_for_selector("table.wikitable", timeout=15000)
            
            logger.info("extrallendo datos de la tabla...")
            filas = page.query_selector_all("table.wikitable tbody tr")
            logger.info(f"  total de filas encontradas: {len(filas)}")              
            
            datos = []
            errores = 0
            
            # Recorrer cada fila (saltar encabezado)
            for i, fila in enumerate(filas[1:], start=1):
                celdas = fila.query_selector_all("td")
                
                if len(celdas) >= 6:
                    try:
                        pais = {
                            "pais": celdas[0].inner_text().strip(),
                            "poblacion_2023": celdas[1].inner_text().strip(),
                            "poblacion_2024": celdas[2].inner_text().strip(),
                            "cambio_porcentual": celdas[3].inner_text().strip(),
                            "continente": celdas[4].inner_text().strip(),
                            "region": celdas[5].inner_text().strip()
                        }

                        datos.append(pais)

                    except Exception as e:
                        errores += 1
                        logger.warning(f"⚠️     Error al procesar fila {i}: {e}")
                        continue
                else:
                    logger.warning(f"⚠️    fila {i} tiene formato inválido (solo {len(celdas)} celdas)")
        
            browser.close()
            logger.info("🔒 navegador cerrado correctamente")

            #resumen de datos extraidos
            logger.info("=" * 60)
            logger.info("EXTRACCIÓN COMPLETADA")
            logger.info(f"  ✅ países extraídos: {len(datos)}")
            logger.info(f"  ⚠️ fillas con errores: {errores}")
            logger.info(f"  tasa de éxito: {len(datos/(len(datos)+errores))*100}")
            logger.info("=" * 60)

            if len(datos) == 0:
                logger.error("❌ crítico: no se extrajo ningún dato válido")
                return None

            return datos
    
    except Exception as e:
        logger.error("=" * 60)
        logger.error("❌ ERROR DE EXTRACCIÓN")
        logger.error(f"     tipo: {type(e).__name__}")
        logger.error(f"     detalle: {str(e)}")
        logger.error("=" * 60)
        return None

