from playwright.sync_api import sync_playwright

def extraer_tabla_completa():
    print("🤖 Iniciando navegador...")
    
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()
        
        print("🌐 Navegando a Wikipedia...")
        page.goto("https://en.wikipedia.org/wiki/List_of_countries_by_population_(United_Nations)")
        
        page.wait_for_selector("table.wikitable")
        
        print("📊 Extrayendo datos...\n")
        
        filas = page.query_selector_all("table.wikitable tbody tr")
        
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
                    print(f"Error al procesar fila {i}: {e}")
                    continue
        
        browser.close()

        print(f"\n✅ Extracción completada:")
        print(f"   - Países extraídos: {len(datos)}")
        print(f"   - Filas con errores: {errores}")
        
        return datos  # Retornar para usar después
