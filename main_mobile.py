
import flet as ft
import os
import json
import time

# En Android/APK no usamos pandas. Usamos nuestro motor JSON ligero.
try:
    from motor_mobile_lite import MotorMobileLite
except ImportError:
    # Fallback dummy si no existe el archivo
    class MotorMobileLite:
        def __init__(self, *args): self.productos = []
        def buscar_producto(self, *args): return []

class AppColors:
    BG_PRINCIPAL = "#0F111A"
    BG_CARD = "#1A1C26"
    PRIMARIO = "#6C5CE7"
    ACENTO_VERDE = "#00CEC9"
    TEXTO_ALTA = "#FFFFFF"
    TEXTO_BAJA = "#A0A3BD"
    BORDE = "#2D2F4E"

class FerreteriaMobileApp:
    def __init__(self, page: ft.Page):
        self.page = page
        self.page.title = "El Indio Mobile"
        self.page.theme_mode = ft.ThemeMode.DARK
        self.page.bgcolor = AppColors.BG_PRINCIPAL
        self.page.padding = 0
        
        # Inicializar Motor
        # En APK, los assets se empaquetan en la raiz o en 'assets'
        # Flet en Android monta los assets en un path especial.
        # Por simplicidad, intentamos leer desde el directorio actual primero.
        self.motor = MotorMobileLite(os.getcwd())
        
        if not self.motor.productos:
            # Intentar buscar en assets si estamos empaquetados
            # Hack: Flet a veces cambia el CWD en mobile.
            pass

        self.setup_ui()

    def setup_ui(self):
        self.page.controls.clear()
        
        # --- HEADER ---
        header = ft.Container(
            content=ft.Row([
                ft.Icon(ft.icons.BUILD_BTN, color=AppColors.PRIMARIO, size=28),
                ft.Text("EL INDIO", size=20, weight="bold", color=AppColors.TEXTO_ALTA),
                ft.Container(expand=True),
                ft.IconButton(ft.icons.REFRESH, icon_color=AppColors.ACENTO_VERDE, on_click=self.recargar_datos)
            ], alignment=ft.MainAxisAlignment.START),
            padding=ft.padding.only(left=20, right=20, top=40, bottom=15),
            bgcolor=AppColors.BG_CARD
        )

        # --- BUSCADOR ---
        self.txt_search = ft.TextField(
            hint_text="Buscar productos...",
            bgcolor=AppColors.BG_PRINCIPAL,
            border_color=AppColors.BORDE,
            focused_border_color=AppColors.PRIMARIO,
            prefix_icon=ft.icons.SEARCH,
            on_submit=self.buscar,
            text_size=16,
            content_padding=15,
            expand=True
        )

        search_bar = ft.Container(
            content=ft.Row([self.txt_search]),
            padding=ft.padding.symmetric(horizontal=20, vertical=10)
        )

        # --- INFO ---
        total = len(self.motor.productos)
        self.info_text = ft.Text(f"{total} productos cargados offline", color=AppColors.TEXTO_BAJA, size=12)

        # --- LISTA RESULTADOS ---
        self.results_column = ft.Column(spacing=10, scroll=ft.ScrollMode.AUTO)
        self.results_container = ft.Container(
            content=self.results_column,
            expand=True,
            padding=ft.padding.symmetric(horizontal=15)
        )

        # --- LAYOUT PRINCIPAL ---
        self.main_layout = ft.Column([
            header,
            search_bar,
            ft.Container(self.info_text, padding=ft.padding.symmetric(horizontal=25)),
            self.results_container
        ], expand=True, spacing=5)

        self.page.add(self.main_layout)

    def recargar_datos(self, e):
        self.motor = MotorMobileLite(os.getcwd())
        total = len(self.motor.productos)
        self.info_text.value = f"Recargado: {total} productos."
        self.info_text.update()

    def buscar(self, e):
        query = self.txt_search.value.strip()
        if not query: return

        self.results_column.controls.clear()
        self.info_text.value = f"Buscando '{query}'..."
        self.page.update()

        resultados = self.motor.buscar_producto(query, limite=30)
        
        self.results_column.controls.clear()
        if not resultados:
           self.results_column.controls.append(
               ft.Container(ft.Text("No se encontraron resultados", color="red"), alignment=ft.alignment.center, padding=20)
           )
        else:
           for item in resultados:
               self.results_column.controls.append(self.crear_card(item))
        
        self.info_text.value = f"Se encontraron {len(resultados)} resultados"
        self.page.update()

    def crear_card(self, item):
        precio = item.get("precio_final", 0)
        nombre = item.get("producto", "Sin Nombre")
        prov = item.get("proveedor", "N/A")
        
        return ft.Container(
            content=ft.Column([
                ft.Text(nombre, weight="bold", size=15, color="white", max_lines=2, overflow=ft.TextOverflow.ELLIPSIS),
                ft.Row([
                    ft.Text(f"{prov}", size=11, color="grey"),
                    ft.Text(f"${precio:,.2f}", size=22, weight="bold", color=AppColors.ACENTO_VERDE)
                ], alignment=ft.MainAxisAlignment.SPACE_BETWEEN)
            ], spacing=2),
            bgcolor=AppColors.BG_CARD,
            padding=15,
            border_radius=10,
            border=ft.border.all(1, AppColors.BORDE)
        )

def main(page: ft.Page):
    FerreteriaMobileApp(page)

if __name__ == "__main__":
    ft.app(target=main)
