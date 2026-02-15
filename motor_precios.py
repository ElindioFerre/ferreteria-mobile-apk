"""
motor_precios.py
================
Motor central del sistema de gestión de precios para ferretería.

Responsabilidades:
- Cargar archivos Excel/CSV/PDF de proveedores de forma robusta e inteligente.
- Cargar configuración de márgenes.
- Calcular precios finales.
- Búsqueda fuzzy y exacta de productos.
"""

import os
import json
import pandas as pd
import pdfplumber
import re
import datetime
import io
from thefuzz import fuzz, process
from concurrent.futures import ThreadPoolExecutor
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload

class MotorPrecios:
    """Motor principal para el cálculo de precios de ferretería."""

    def __init__(self, directorio_proveedores: str, archivo_margenes: str):
        """
        Inicializa el motor de precios.
        """
        self.directorio_proveedores = directorio_proveedores
        self.archivo_margenes = archivo_margenes

        # Datos cargados
        self.proveedores: dict[str, pd.DataFrame] = {}
        self.margenes: dict[str, float] = {}
        self.margen_default: float = 20.0
        self.umbral_fuzzy: int = 60
        self.moneda: str = "ARS"
        self.drive_folder_id: str = ""

        # Cargar todo al instanciar
        self._cargar_margenes()
        
        # Sincronizar con Google Drive si hay ID de carpeta
        if self.drive_folder_id:
            self.sincronizar_con_drive()
            
        self._cargar_proveedores()

    # ──────────────────────────────────────────────────────────────
    #  Carga de datos
    # ──────────────────────────────────────────────────────────────
    def _cargar_margenes(self):
        """Carga la configuración de márgenes desde el archivo JSON."""
        try:
            with open(self.archivo_margenes, "r", encoding="utf-8") as f:
                config = json.load(f)

            self.margenes = config.get("margenes_por_proveedor", {})
            self.margen_default = config.get("margen_default", 20.0)
            self.umbral_fuzzy = config.get("umbral_busqueda_fuzzy", 60)
            self.moneda = config.get("moneda", "ARS")
            self.drive_folder_id = config.get("drive_folder_id", "")

        except FileNotFoundError:
            print(f"[AVISO] Archivo de margenes no encontrado: {self.archivo_margenes}")
        except json.JSONDecodeError as e:
            print(f"[AVISO] Error al leer el archivo de margenes: {e}")

    def sincronizar_con_drive(self):
        """Descarga archivos de Google Drive a la carpeta local de proveedores."""
        # Intentamos buscar credentials.json en la misma carpeta que los margenes
        base_dir = os.path.dirname(os.path.abspath(self.archivo_margenes))
        creds_path = os.path.join(base_dir, "credentials.json")
        
        if not os.path.exists(creds_path):
            # Backup: nombre alternativo que usamos a veces
            creds_path = os.path.join(base_dir, "clave_drive.json")

        if not os.path.exists(creds_path):
            print(f"[DRIVE] No se encontró archivo de credenciales en {base_dir}. Saltando sincronización.")
            return

        if not self.drive_folder_id:
            print("[DRIVE] No hay ID de carpeta configurado. Saltando sincronización.")
            return

        print(f"[DRIVE] Sincronizando desde carpeta ID: {self.drive_folder_id}...")
        try:
            creds = service_account.Credentials.from_service_account_file(
                creds_path, scopes=['https://www.googleapis.com/auth/drive.readonly'])
            service = build('drive', 'v3', credentials=creds)

            # Listar archivos en la carpeta
            query = f"'{self.drive_folder_id}' in parents and trashed = false"
            results = service.files().list(q=query, fields="files(id, name, modifiedTime)").execute()
            items = results.get('files', [])

            if not os.path.exists(self.directorio_proveedores):
                os.makedirs(self.directorio_proveedores)

            archivos_en_drive = []
            if not items:
                print("[DRIVE] La carpeta en Drive está vacía.")
            else:
                for item in items:
                    nombre = item['name']
                    file_id = item['id']
                    
                    # Solo bajar archivos relevantes
                    if not nombre.lower().endswith((".xlsx", ".xls", ".csv", ".pdf", ".json")):
                        continue
                    
                    archivos_en_drive.append(nombre)
                    ruta_local = os.path.join(self.directorio_proveedores, nombre)
                    
                    # Sincronización inteligente: solo bajar si es más nuevo o no existe
                    bajar = True
                    if os.path.exists(ruta_local):
                        mtime_local = datetime.datetime.fromtimestamp(os.path.getmtime(ruta_local), tz=datetime.timezone.utc)
                        mtime_drive = datetime.datetime.fromisoformat(item['modifiedTime'].replace('Z', '+00:00'))
                        if mtime_local >= mtime_drive:
                            bajar = False

                    if bajar:
                        print(f"  [DRIVE] Descargando: {nombre}...")
                        request = service.files().get_media(fileId=file_id)
                        fh = io.BytesIO()
                        downloader = MediaIoBaseDownload(fh, request)
                        done = False
                        while done is False:
                            status, done = downloader.next_chunk()
                        
                        with open(ruta_local, "wb") as f:
                            f.write(fh.getbuffer())
                    else:
                        # print(f"  [DRIVE] Al día: {nombre}")
                        pass

            # --- NUEVO: BORRADO SINCRONIZADO ---
            # Eliminar archivos locales que ya no están en Drive
            archivos_locales = [f for f in os.listdir(self.directorio_proveedores) 
                                if f.lower().endswith((".xlsx", ".xls", ".csv", ".pdf", ".json"))]
            
            for f_local in archivos_locales:
                if f_local not in archivos_en_drive:
                    ruta_borrar = os.path.join(self.directorio_proveedores, f_local)
                    print(f"  [DRIVE] Eliminando local (borrado en Drive): {f_local}")
                    try:
                        os.remove(ruta_borrar)
                        # También borrar su cache si existe
                        cache_file = os.path.join(self.directorio_proveedores, "cache", f"{os.path.splitext(f_local)[0]}.pkl")
                        if os.path.exists(cache_file):
                            os.remove(cache_file)
                    except: pass

            print("[DRIVE] Sincronización completada.")

        except Exception as e:
            print(f"[DRIVE] Error durante la sincronización: {e}")

    def _procesar_dataframe_inteligente(self, df: pd.DataFrame, mapeo_manual: dict = None) -> pd.DataFrame:
        """
        ALGORITMO DE DENSIDAD DE DATOS (MEJORADO):
        Analiza el contenido para detectar Producto, Precio y Codigo.
        Distingue entre Codigos Numericos (Enteros) y Precios (Decimales).
        
        Si 'mapeo_manual' esta presente (ej: {'col_codigo': 0, 'col_producto': 1, 'col_precio': 4}),
        se salta la deteccion y usa esas columnas directamente.
        """
        # 1. Limpieza base
        df = df.astype(str)
        
        # NUEVO: Check si ya esta procesado (detectable por nombres de columnas)
        # Esto evita que extractores especificos que ya limpiaron el DF fallen en la deteccion auto
        if "Producto" in df.columns and "Precio de Costo" in df.columns:
             if "Codigo" not in df.columns: df["Codigo"] = ""
             return self._limpieza_final(df)

        total_filas = len(df)
        if total_filas == 0: return pd.DataFrame()

        # --- MODO MANUAL (Si el usuario eligio columnas) ---
        if mapeo_manual:
            try:
                print(f"  [MANUAL] Usando configuracion de columnas: {mapeo_manual}")
                # El mapeo viene como indices de columna (int) o nombres (str) si el df tiene headers
                # Asumimos que leimos header=None, asi que son indices (0, 1, 2...)
                
                # Mapa de renombres
                
                # --- NUEVO: Aplicar recorte de filas (Skip Rows) ---
                skip = int(mapeo_manual.get("skip_rows", 0))
                if skip > 0:
                    print(f"  [MANUAL] Saltando {skip} filas iniciales.")
                    df = df.iloc[skip:].reset_index(drop=True)
                # ---------------------------------------------------

                idx_prod = int(mapeo_manual.get("col_producto", -1))
                idx_precio = int(mapeo_manual.get("col_precio", -1))
                idx_cod = int(mapeo_manual.get("col_codigo", -1))
                
                # Validar indices
                max_idx = len(df.columns) - 1
                if idx_prod > max_idx or idx_precio > max_idx:
                    print(f"  [ERROR MANUAL] Indices fuera de rango (Max {max_idx}): {mapeo_manual}")
                    return pd.DataFrame()

                # Seleccionar columnas por indice
                # df.columns es RangeIndex(0, 1, 2...) o nombres string generados
                col_nombre_prod = df.columns[idx_prod]
                col_nombre_precio = df.columns[idx_precio]
                
                mapa = {col_nombre_prod: "Producto", col_nombre_precio: "Precio de Costo"}
                
                if idx_cod >= 0 and idx_cod <= max_idx:
                    col_nombre_cod = df.columns[idx_cod]
                    mapa[col_nombre_cod] = "Codigo"
                
                # Renombrar
                df_final = df.rename(columns=mapa)
                
                # Asegurar codigo
                if "Codigo" not in df_final.columns: df_final["Codigo"] = ""

                # Ir directo a limpieza final (saltando deteccion)
                return self._limpieza_final(df_final)
                
            except Exception as e:
                print(f"  [ERROR MANUAL] Fallo al aplicar mapeo: {e}. Se intentara auto-detectar.")
                # Si falla, cae al modo automatico

        # --- MODO AUTOMATICO (DENSIDAD) ---
        # Muestreo
        step = max(1, total_filas // 200)
        indices_muestra = range(0, total_filas, step)

        scores_precio = {}
        scores_decimales = {} # Para desempatar codigos vs precios
        scores_producto = {}
        scores_codigo = {}

        # ... (Resto de funciones helpers igual) ...
        def es_numero_posible(texto):
            t = str(texto).strip().replace("$", "").replace("USD", "").replace("EUR", "").lower()
            if not t or t == "nan": return False
            digits = sum(c.isdigit() for c in t)
            return (digits / len(t)) > 0.5 if len(t) > 0 else False

        def tiene_decimales(texto):
            t = str(texto).strip()
            return "," in t or "." in t

        def es_producto(texto):
            t = str(texto).strip()
            if len(t) < 4 or t.lower() == "nan": return False
            letras = sum(c.isalpha() or c.isspace() for c in t)
            return (letras / len(t)) > 0.6 if len(t) > 0 else False

        def es_codigo(texto):
            t = str(texto).strip()
            if t.lower() == "nan": return False
            return 1 < len(t) < 18 and any(c.isdigit() for c in t)

        for col in df.columns:
            # ... (Logica de scores igual) ...
            muestra = df[col].iloc[indices_muestra].tolist()
            total = len(muestra)
            
            # Convertir a string seguro
            muestra_str = [str(x).strip() for x in muestra]
            
            unicos = len(set(muestra_str))
            ratio_unicidad = unicos / total if total > 0 else 0
            avg_len = sum(len(x) for x in muestra_str) / total if total > 0 else 0

            hits_num = sum(1 for x in muestra if es_numero_posible(x))
            hits_dec = sum(1 for x in muestra if es_numero_posible(x) and tiene_decimales(x))
            hits_prod = sum(1 for x in muestra if es_producto(x))
            hits_cod = sum(1 for x in muestra if es_codigo(x))
            
            scores_precio[col] = hits_num / total
            scores_decimales[col] = hits_dec / total 
            scores_producto[col] = (hits_prod / total) * avg_len 
            scores_codigo[col] = (hits_cod / total) * ratio_unicidad

        # ... (Logica de seleccion igual) ...
        # --- SELECCION PRECIO (LOGICA REFINADA) ---
        if not scores_precio: return pd.DataFrame()
        
        col_producto = None
        col_precio = None
        col_codigo = None

        # --- CASO ESPECIAL: 2 COLUMNAS (Muy comun en PDF como 'El Taller') ---
        if len(df.columns) == 2:
            # Si una tiene numeros y la otra texto, es una tabla valida
            s_prod_0 = scores_producto.get(df.columns[0], 0)
            s_prod_1 = scores_producto.get(df.columns[1], 0)
            s_prec_0 = scores_precio.get(df.columns[0], 0)
            s_prec_1 = scores_precio.get(df.columns[1], 0)
            
            if s_prec_1 > 0.3 and s_prod_0 > 0.3:
                col_producto = df.columns[0]
                col_precio = df.columns[1]
            elif s_prec_0 > 0.3 and s_prod_1 > 0.3:
                col_producto = df.columns[1]
                col_precio = df.columns[0]
            else:
                return pd.DataFrame() # No parece tabla de precios
        else:
            candidatos = [c for c, s in scores_precio.items() if s > 0.1]
            if not candidatos: return pd.DataFrame()
            candidatos.sort(key=lambda c: scores_precio[c], reverse=True)
            best_col = candidatos[0]
            match_col = best_col
            for cand in candidatos[1:]:
                if (scores_precio[best_col] - scores_precio[cand]) < 0.2:
                    score_dec_best = scores_decimales[best_col]
                    score_dec_cand = scores_decimales[cand]
                    if score_dec_cand > (score_dec_best + 0.1):
                        match_col = cand
                        best_col = cand 
                else:
                    break
            col_precio = match_col
            
            # --- SELECCION PRODUCTO ---
            posibles_prods = scores_producto.copy()
            if col_precio in posibles_prods: del posibles_prods[col_precio]
            if not posibles_prods: return pd.DataFrame()
            posibles_prods = {k: v for k, v in posibles_prods.items() if v > 0.5}
            if not posibles_prods: return pd.DataFrame()
            col_producto = max(posibles_prods, key=posibles_prods.get)
            
            # --- SELECCION CODIGO (MEJORADA CON POSICION) ---
            col_codigo = None
            posibles_cods = scores_codigo.copy()
            if col_precio in posibles_cods: del posibles_cods[col_precio]
            if col_producto in posibles_cods: del posibles_cods[col_producto]
            candidatos_cod = [c for c, s in posibles_cods.items() if s > 0.05]
            if candidatos_cod:
                try: idx_prod = df.columns.get_loc(col_producto)
                except: idx_prod = 999
                best_cand = None
                best_final_score = -1
                for cand in candidatos_cod:
                    score_base = posibles_cods[cand]
                    try:
                        idx_cand = df.columns.get_loc(cand)
                        if idx_cand < idx_prod: score_base *= 2.0
                        else: score_base *= 0.5
                    except: pass
                    if score_base > best_final_score:
                        best_final_score = score_base
                        best_cand = cand
                col_codigo = best_cand

        if not col_producto or not col_precio:
            return pd.DataFrame()

        print(f"  [AUTO] Prod='{col_producto}' | Precio='{col_precio}' | Cod='{col_codigo}'")

        # RENOMBRAR
        df_final = df.copy()
        mapa = {col_producto: "Producto", col_precio: "Precio de Costo"}
        if col_codigo: mapa[col_codigo] = "Codigo"
        df_final = df_final.rename(columns=mapa)
        if "Codigo" not in df_final.columns: df_final["Codigo"] = ""

        return self._limpieza_final(df_final)

    def _limpieza_final(self, df_final: pd.DataFrame) -> pd.DataFrame:
        """Aplica la limpieza estandar de valores a un DF ya con columnas renombradas."""
        # Parser precio estricto
        def limpiar_precio(val):
            val = str(val).strip().replace("$", "").replace("USD", "").replace("EUR", "")
            try:
                if "," in val and "." in val:
                    if val.rfind(",") > val.rfind("."): # 1.000,50
                        val = val.replace(".", "").replace(",", ".")
                    else: # 1,000.50
                        val = val.replace(",", "")
                elif "," in val: # 100,50 (Español)
                    val = val.replace(",", ".")
                elif "." in val: # Ojo: ¿100.50 o 100.500 (mil)?
                    partes = val.split(".")
                    # Si la ultima parte tiene exactamente 3 digitos y no es la primera, es muy probable que sea separador de miles (ej: 108.200)
                    if len(partes) > 1 and len(partes[-1]) == 3:
                        val = val.replace(".", "")
                
                f = float(val)
                return f if f > 0 else None
            except:
                return None

        df_final["Precio de Costo"] = df_final["Precio de Costo"].apply(limpiar_precio)
        df_final = df_final.dropna(subset=["Precio de Costo"])
        
        df_final["Producto"] = df_final["Producto"].astype(str).str.strip()
        df_final = df_final[df_final["Producto"].str.len() > 1]
        df_final["Codigo"] = df_final["Codigo"].astype(str).str.strip().replace("nan", "")
        
        return df_final

    def _leer_pdf(self, ruta_archivo: str) -> pd.DataFrame:
        """
        Lee un archivo PDF usando estrategias múltiples.
        MEJORADO: Filtra portadas, encabezados repetidos y filas basura.
        """
        import re
        dfs = []
        num_cols_referencia = 0  # Para detectar la estructura "real" de la tabla
        
        # Palabras clave que indican encabezados de tabla (se repiten en cada página)
        KEYWORDS_HEADER = {
            "codigo", "descripcion", "descripción", "producto", "precio", 
            "$ lista", "$lista", "lista", "costo", "art", "articulo",
            "artículo", "detalle", "importe", "p.lista", "p. lista",
            "cod", "code", "item", "ref", "referencia"
        }
        
        try:
            import pdfplumber
            with pdfplumber.open(ruta_archivo) as pdf:
                total_paginas = len(pdf.pages)
                print(f"  [PDF] Abriendo {ruta_archivo}: {total_paginas} paginas.")

                # --- OPTIMIZACION: Extractor especifico para "EL TALLER" ---
                if "EL TALLER" in ruta_archivo.upper():
                    return self._leer_pdf_taller(pdf)
                
                for i, page in enumerate(pdf.pages):
                    # Estrategia 1: Default (Busca lineas/grid)
                    tablas = page.extract_tables()
                    
                    # Estrategia 2: Si no encuentra nada, probar texto
                    if not tablas:
                        tablas = page.extract_tables(table_settings={
                            "vertical_strategy": "text", 
                            "horizontal_strategy": "text",
                            "snap_tolerance": 4
                        })
                    
                    if not tablas:
                        print(f"    Pág {i+1}: Sin tablas detectadas (portada/vacia)")
                        continue
                    
                    for tabla in tablas:
                        if not tabla:
                            continue
                            
                        df_temp = pd.DataFrame(tabla)
                        
                        # --- FILTRO 1: Tabla demasiado pequeña (basura de portada) ---
                        # Una tabla de precios real tiene al menos 2 columnas y 1 fila de datos
                        if len(df_temp.columns) < 2 or len(df_temp) < 1:
                            print(f"    Pág {i+1}: Tabla descartada (muy pequena: {len(df_temp)}x{len(df_temp.columns)})")
                            continue
                        
                        # --- FILTRO 2: Limpiar filas completamente vacias ---
                        df_temp = df_temp.dropna(how='all')
                        df_temp = df_temp[df_temp.apply(
                            lambda x: any(
                                str(v).strip() not in ("", "None", "nan") 
                                for v in x
                            ), axis=1
                        )]
                        
                        if df_temp.empty:
                            continue
                        
                        # --- FILTRO 3: Detectar y remover filas de encabezado repetido ---
                        # Busca filas donde la mayoría de celdas son palabras clave de header
                        filas_a_eliminar = []
                        for idx, row in df_temp.iterrows():
                            celdas = [str(v).strip().lower() for v in row if str(v).strip()]
                            if not celdas:
                                filas_a_eliminar.append(idx)
                                continue
                            
                            # ¿Es una fila de encabezado? (>= 50% de celdas son keywords)
                            hits_header = sum(
                                1 for c in celdas 
                                if any(kw in c for kw in KEYWORDS_HEADER)
                            )
                            if len(celdas) > 0 and hits_header / len(celdas) >= 0.5:
                                filas_a_eliminar.append(idx)
                                continue
                            
                            # ¿Es un título de seccion? (solo 1 celda con texto, el resto vacío)
                            celdas_con_contenido = [
                                c for c in row 
                                if str(c).strip() not in ("", "None", "nan")
                            ]
                            if len(celdas_con_contenido) == 1 and len(df_temp.columns) >= 3:
                                texto = str(celdas_con_contenido[0]).strip()
                                # Si es un texto corto todo en mayúsculas o sin números -> seccion
                                if len(texto) < 50 and not any(c.isdigit() for c in texto):
                                    filas_a_eliminar.append(idx)
                                    continue
                        
                        if filas_a_eliminar:
                            df_temp = df_temp.drop(filas_a_eliminar)
                        
                        # --- FILTRO 4: Verificar que queden datos utiles ---
                        if len(df_temp) < 1:
                            print(f"    Pág {i+1}: Tabla vacia despues de limpiar encabezados")
                            continue
                        
                        # --- FILTRO 5: Consistencia de columnas ---
                        n_cols = len(df_temp.columns)
                        if num_cols_referencia == 0:
                            num_cols_referencia = n_cols
                        
                        # Si esta tabla tiene muchas menos columnas que la referencia, es basura
                        if num_cols_referencia > 0 and n_cols < num_cols_referencia * 0.5:
                            print(f"    Pág {i+1}: Tabla descartada (columnas inconsistentes: {n_cols} vs ref {num_cols_referencia})")
                            continue
                        
                        print(f"    Pag {i+1}: [OK] Tabla valida ({len(df_temp)} filas x {n_cols} cols)")
                        dfs.append(df_temp)
            
                # ── ESTRATEGIA 3: Texto raw (SIEMPRE se intenta) ──
                # Incluso si se encontraron tablas, el texto raw puede encontrar MAS productos.
                # Al final se compara y se elige la mejor fuente.
                total_filas_tablas = sum(len(df) for df in dfs) if dfs else 0
                print(f"  [PDF] Estrategia 3: Extraccion por texto raw... (tablas encontraron {total_filas_tablas} filas)")
                filas_raw = []
                
                # Regex para precio argentino (ej: 17.684,21 o 1.234,56 o 864,05 o 1563,2)
                PRECIO_RE = r'([\d]{1,3}(?:\.?\d{3})*(?:,\d{1,2})?)'
                
                # Patron 1: CODIGO alfanumerico + DESCRIPCION + PRECIO (permite texto extra al final)
                patron_producto = re.compile(
                    r'^([A-Z]{1,5}\d{2,8}[A-Z]?)\s+'   # Codigo alfanumerico
                    r'(.+?)\s+'                          # Descripcion (lazy)
                    + PRECIO_RE +                        # Primer precio
                    r'(?:\s+.*)?$'                       # Permite trailing text
                )
                
                # Patron 2: Codigo numerico puro + DESC + PRECIO
                patron_numerico = re.compile(
                    r'^(\d{3,10})\s+'                    # Codigo numerico
                    r'(.+?)\s+'                          # Descripcion
                    + PRECIO_RE +                        # Precio
                    r'(?:\s+.*)?$'                       # Trailing text
                )
                
                # Patron 3: Sin codigo (solo descripcion + precio)
                patron_sin_codigo = re.compile(
                    r'^(.{10,80}?)\s{2,}'                # Descripcion
                    + PRECIO_RE +                        # Precio
                    r'(?:\s+.*)?$'                       # Trailing text
                )

                for i, page in enumerate(pdf.pages):
                    texto = page.extract_text()
                    if not texto:
                        continue
                    
                    lineas = texto.split("\n")
                    for linea in lineas:
                        linea = linea.strip()
                        if not linea or len(linea) < 10:
                            continue
                        
                        # Intentar patron con codigo alfanumerico
                        m = patron_producto.match(linea)
                        if m:
                            codigo = m.group(1)
                            desc = m.group(2).strip()
                            precio = m.group(3)
                            
                            # Filtrar titulos de seccion (codigo sin precio real o desc muy corta)
                            if precio == "0" or (len(desc) < 3):
                                continue
                            if precio in ("0,00", "0,0"):
                                continue
                                
                            filas_raw.append([codigo, desc, precio])
                            continue
                        
                        # Intentar patron con codigo numerico
                        m = patron_numerico.match(linea)
                        if m:
                            precio = m.group(3)
                            if precio in ("0,00", "0,0", "0"):
                                continue
                            filas_raw.append([m.group(1), m.group(2).strip(), precio])
                            continue
                        
                        # Intentar patron sin codigo
                        m = patron_sin_codigo.match(linea)
                        if m:
                            desc = m.group(1).strip()
                            precio = m.group(2)
                            desc_lower = desc.lower()
                            if any(kw in desc_lower for kw in KEYWORDS_HEADER):
                                continue
                            if len(desc) < 5 or precio in ("0,00", "0,0", "0"):
                                continue
                            filas_raw.append(["", desc, precio])
                            continue
                
                if filas_raw:
                    print(f"  [PDF] Texto raw: {len(filas_raw)} productos encontrados.")
                    df_raw = pd.DataFrame(filas_raw, columns=[0, 1, 2])
                    # Si el texto raw encontro MAS que las tablas, reemplazar
                    if len(filas_raw) > total_filas_tablas:
                        dfs = [df_raw]  # Reemplazar basura
                        print(f"  [PDF] Texto raw reemplaza tablas previas ({total_filas_tablas} -> {len(filas_raw)} filas)")
                    else:
                        dfs.append(df_raw)
                else:
                    print(f"  [PDF] Texto raw: No se encontraron productos.")

            if dfs:
                # Normalizar columnas antes de concatenar
                max_cols = max(len(df.columns) for df in dfs)
                dfs_norm = []
                for df in dfs:
                    if len(df.columns) < max_cols:
                        for c in range(len(df.columns), max_cols):
                            df[c] = ""
                    elif len(df.columns) > max_cols:
                        df = df.iloc[:, :max_cols]
                    df.columns = range(max_cols)
                    dfs_norm.append(df)
                
                df_total = pd.concat(dfs_norm, ignore_index=True)
                print(f"  [PDF] Total procesado: {len(df_total)} filas utiles de {total_paginas} paginas.")
                return df_total
            else:
                print(f"  [PDF] No se detectaron tablas validas en {ruta_archivo}.")
                return pd.DataFrame()
        except Exception as e:
            print(f"  [ERROR PDF] {e}")
            return pd.DataFrame()

    def _leer_pdf_taller(self, pdf) -> pd.DataFrame:
        """Extractor ultra-rapido optimizado para el formato de El Taller."""
        filas = []
        # Palabras que indican que la fila no es un producto
        basura = ["luis", "estela", "gmail", "hidrolavadoras", "lijadoras", "página", "precio", "ofert", "cod.", "o. c", "oferta"]
        
        for page in pdf.pages:
            tablas = page.extract_tables()
            for tabla in tablas:
                for row in tabla:
                    if not row or len(row) < 2: continue
                    # Limpiar celdas de basura visual
                    row = [str(c).replace('\n', ' ').strip() for c in row if c is not None]
                    if len(row) < 2: continue
                    
                    prod = row[0]
                    prec = row[1]
                    
                    # El taller a veces tiene el precio en la col 2 si hay 3 cols
                    if len(row) >= 3 and (not any(c.isdigit() for c in prec) or "$" not in prec):
                         prec = row[2]

                    if len(prod) < 5 or prec.lower() in ("none", "", "nan"): continue
                    if any(b in prod.lower() for b in basura): continue
                    
                    filas.append([prod, prec])
        
        print(f"  [TALLER] {len(filas)} productos extraidos.")
        return pd.DataFrame(filas, columns=["Producto", "Precio de Costo"])

    def _cargar_proveedores(self):
        """Carga y procesa todos los archivos."""
        self.proveedores = {}
        if not os.path.exists(self.directorio_proveedores):
            os.makedirs(self.directorio_proveedores, exist_ok=True)
            return

        archivos = [f for f in os.listdir(self.directorio_proveedores) 
                   if f.lower().endswith((".xlsx", ".xls", ".csv", ".pdf"))]

        for archivo in archivos:
            ruta = os.path.join(self.directorio_proveedores, archivo)
            nombre = os.path.splitext(archivo)[0]
            
            # El taller y DFA tardan mucho, los dejamos para el final o procesamos en paralelo
            archivos_ordenados = archivos.copy()
            # Poner PDFs grandes al final no ayuda en secuencial, pero si en paralelo
            
        def procesar_un_archivo(archivo):
            ruta = os.path.join(self.directorio_proveedores, archivo)
            nombre = os.path.splitext(archivo)[0]
            ruta_cache = os.path.join(self.directorio_proveedores, "cache", f"{nombre}.pkl")
            
            # --- SISTEMA DE CACHE ---
            # Si el cache existe y es más nuevo que el archivo origen, cargar directo
            if os.path.exists(ruta_cache):
                mtime_org = os.path.getmtime(ruta)
                mtime_cache = os.path.getmtime(ruta_cache)
                if mtime_cache > mtime_org:
                    try:
                        df_cached = pd.read_pickle(ruta_cache)
                        # print(f"  [CACHE] {nombre} cargado.")
                        return nombre, df_cached
                    except: pass

            ruta_config = ruta + ".json"
            mapeo_manual = None
            if os.path.exists(ruta_config):
                try:
                    with open(ruta_config, "r") as f:
                        mapeo_manual = json.load(f)
                except: pass

            try:
                print(f"  [PROCESANDO] {nombre}...")
                if archivo.lower().endswith(".csv"):
                    df = pd.read_csv(ruta, header=None, encoding="utf-8", dtype=str)
                elif archivo.lower().endswith(".pdf"):
                    df = self._leer_pdf(ruta)
                else:
                    df = pd.read_excel(ruta, header=None, dtype=str)
                
                df_final = self._procesar_dataframe_inteligente(df, mapeo_manual)
                if not df_final.empty:
                    # Guardar en cache para la próxima vez
                    try:
                        if not os.path.exists(os.path.dirname(ruta_cache)):
                            os.makedirs(os.path.dirname(ruta_cache))
                        df_final.to_pickle(ruta_cache)
                    except: pass
                    return nombre, df_final
            except Exception as e:
                print(f"  [FAIL] {nombre}: {e}")
            return None

        # Carga paralela mas agresiva
        with ThreadPoolExecutor(max_workers=os.cpu_count() or 4) as executor:
            resultados = list(executor.map(procesar_un_archivo, archivos))
            
        for res in resultados:
            if res:
                self.proveedores[res[0]] = res[1]
                print(f"  [OK] {res[0]}: {len(res[1])} productos.")

        print(f"Total proveedores activos: {len(self.proveedores)}")

    # ──────────────────────────────────────────────────────────────
    #  Calculo y Busqueda (Con soporte de Descuentos en Cascada)
    # ──────────────────────────────────────────────────────────────
    def obtener_info_config(self, proveedor: str) -> dict:
        """Devuelve dict con margen y descuentos normalizados."""
        raw = self.margenes.get(proveedor, self.margen_default)
        if isinstance(raw, dict):
            return {
                "margen": float(raw.get("margen", 0)),
                "desc1": float(raw.get("desc1", 0)),
                "desc2": float(raw.get("desc2", 0))
            }
        else:
            return {
                "margen": float(raw),
                "desc1": 0.0,
                "desc2": 0.0
            }

    def calcular_precio_final(self, precio_costo: float, config: dict) -> float:
        """
        Aplica descuentos en cascada y luego el margen de ganancia.
        Formula: Costo * (1 - d1) * (1 - d2) * (1 + margen)
        """
        costo_neto = precio_costo
        
        # Aplicar Descuento 1
        d1 = config.get("desc1", 0)
        if d1 > 0:
            costo_neto = costo_neto * (1 - (d1 / 100))
            
        # Aplicar Descuento 2
        d2 = config.get("desc2", 0)
        if d2 > 0:
            costo_neto = costo_neto * (1 - (d2 / 100))
            
        # Aplicar Margen
        margen = config.get("margen", 0)
        precio_venta = costo_neto * (1 + (margen / 100))
        
        return round(precio_venta, 2)

    def buscar_producto(self, consulta: str, limite: int = 20, proveedor_target: str = None) -> list[dict]:
        """Busca productos en todos los proveedores o en uno especifico."""
        consulta = str(consulta).lower().strip()
        resultados = []
        
        for nom_prov, df in self.proveedores.items():
            # Filtro por proveedor optimizado
            if proveedor_target and nom_prov != proveedor_target:
                continue

            if df.empty: continue
            
            # Obtener configuracion completa
            config = self.obtener_info_config(nom_prov)
            
            # 1. Exacto
            try:
                # Asegurar columnas (aunque deberian estar)
                if "Producto" not in df.columns or "Codigo" not in df.columns: continue
                
                mask = df["Producto"].str.lower().str.contains(consulta, na=False) | \
                       df["Codigo"].astype(str).str.lower().str.contains(consulta, na=False)
                
                df_exact = df[mask].head(limite)
                
                for _, row in df_exact.iterrows():
                    try:
                        pc = float(str(row["Precio de Costo"]).replace("$", "").replace(",", "."))
                    except: pc = 0.0
                    
                    resultados.append({
                        "codigo": row["Codigo"],
                        "producto": row["Producto"],
                        "proveedor": nom_prov,
                        "precio_costo": pc,
                        "config": config,
                        "precio_final": self.calcular_precio_final(pc, config),
                        "score_busqueda": 100
                    })
            except Exception as e:
                print(f"Error buscando en {nom_prov}: {e}")
                continue
            
            if len(resultados) >= limite * 2: break # Optimización

        # 2. Fuzzy (Solo si faltan resultados y query larga)
        if len(resultados) < 5 and len(consulta) > 3:
            for nom_prov, df in self.proveedores.items():
                if proveedor_target and nom_prov != proveedor_target: continue
                if df.empty: continue
                
                config = self.obtener_info_config(nom_prov)
                
                pool = df["Producto"].dropna().astype(str).tolist()
                matches = process.extract(consulta, pool, limit=5, scorer=fuzz.partial_token_sort_ratio)
                
                for m_str, score in matches:
                    if score < self.umbral_fuzzy: continue
                    # Evitar duplicados
                    if any(r["producto"] == m_str and r["proveedor"] == nom_prov for r in resultados): continue
                    
                    try:
                        row = df[df["Producto"] == m_str].iloc[0]
                        try: pc = float(str(row["Precio de Costo"]).replace("$", "").replace(",", "."))
                        except: pc = 0.0
                        
                        resultados.append({
                            "codigo": row["Codigo"],
                            "producto": row["Producto"],
                            "proveedor": nom_prov,
                            "precio_costo": pc,
                            "config": config,
                            "precio_final": self.calcular_precio_final(pc, config),
                            "score_busqueda": score
                        })
                    except: pass

        # Ordenar por score
        resultados.sort(key=lambda x: x["score_busqueda"], reverse=True)
        return resultados[:limite]

    def listar_proveedores(self) -> list[str]:
        return list(self.proveedores.keys())

    def obtener_total_productos(self) -> int:
        return sum(len(df) for df in self.proveedores.values())

    def recargar(self):
        self.proveedores.clear()
        self.margenes.clear()
        self._cargar_margenes()
        self._cargar_proveedores()

    def actualizar_margen(self, proveedor: str, margen: float, desc1: float = 0, desc2: float = 0):
        # Guardamos como dict si hay descuentos, o mantenemos simpleza si no
        if desc1 > 0 or desc2 > 0:
            self.margenes[proveedor] = {
                "margen": margen,
                "desc1": desc1,
                "desc2": desc2
            }
        else:
            self.margenes[proveedor] = margen
            
        self._guardar_margenes()

    def _guardar_margenes(self):
        config = {
            "margenes_por_proveedor": self.margenes,
            "margen_default": self.margen_default,
            "moneda": self.moneda,
            "umbral_busqueda_fuzzy": self.umbral_fuzzy,
        }
        with open(self.archivo_margenes, "w", encoding="utf-8") as f:
            json.dump(config, f, indent=4, ensure_ascii=False)
