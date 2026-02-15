
import json
import os

class MotorMobileLite:
    """
    Motor ligero para Android/iOS.
    No usa pandas ni java. Lee una DB pre-procesada en JSON.
    """
    def __init__(self, directorio_base=None):
        # Si no nos dan directorio, usamos el directorio donde esta ESTE script
        if directorio_base is None:
            directorio_base = os.path.dirname(os.path.abspath(__file__))
            
        self.ruta_db = os.path.join(directorio_base, "base_datos_mobile.json")
        self.ruta_margenes = os.path.join(directorio_base, "margenes.json")
        
        self.productos = []
        self.margenes = {}
        self.margen_default = 30.0

        self._cargar_datos()

    def _cargar_datos(self):
        # 1. Cargar Base de Datos de Productos
        if os.path.exists(self.ruta_db):
            try:
                with open(self.ruta_db, "r", encoding="utf-8") as f:
                    self.productos = json.load(f)
                print(f"[LITE] Cargados {len(self.productos)} productos.")
            except Exception as e:
                print(f"[LITE] Error cargando DB: {e}")
                self.productos = []
        else:
            print(f"[LITE] No se encontro {self.ruta_db}")

        # 2. Cargar Margenes
        if os.path.exists(self.ruta_margenes):
            try:
                with open(self.ruta_margenes, "r", encoding="utf-8") as f:
                    data = json.load(f)
                    self.margenes = data.get("margenes_por_proveedor", {})
                    self.margen_default = data.get("margen_default", 30.0)
            except Exception as e:
                print(f"[LITE] Error cargando margenes: {e}")
    
    def sincronizar_con_drive(self):
        # En version Lite APK, esto es dificil sin librerias externas.
        # Por ahora simulamos o indicamos que se requiere exportar desde PC.
        pass

    def buscar_producto(self, consulta, limite=50):
        consulta = str(consulta).lower().strip()
        if not consulta: return []
        
        resultados = []
        for p in self.productos:
            # Busqueda simple en texto
            match = False
            if consulta in p.get("producto", "").lower(): match = True
            elif consulta in str(p.get("codigo", "")).lower(): match = True
            elif consulta in p.get("proveedor", "").lower(): match = True
            
            if match:
                # Calcular precio actual (por si cambiaron los margenes en el json)
                precio_final = self._calcular_precio_final(p)
                # Crear copia para no mutar el original permanentemente en RAM
                item = p.copy()
                item["precio_final"] = precio_final
                resultados.append(item)
                
                if len(resultados) >= limite:
                    break
        return resultados

    def obtener_info_config(self, proveedor):
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

    def _calcular_precio_final(self, item):
        prov = item.get("proveedor", "")
        costo = float(item.get("precio_costo", 0))
        config = self.obtener_info_config(prov)
        
        costo_neto = costo
        if config["desc1"] > 0: costo_neto *= (1 - config["desc1"]/100)
        if config["desc2"] > 0: costo_neto *= (1 - config["desc2"]/100)
        
        precio_venta = costo_neto * (1 + config["margen"]/100)
        return round(precio_venta, 2)
