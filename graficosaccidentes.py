
import csv
import glob
import os
import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt

base = "/home/vboxuser/tarea3"
destino = base + "/graficos"
os.makedirs(destino, exist_ok=True)

def cargar(carpeta):
    archivos = glob.glob(
        f"{base}/resultados/{carpeta}/part-*.csv"
    )
    if not archivos:
        raise FileNotFoundError(carpeta)

    with open(archivos[0], encoding="utf-8-sig") as f:
        return list(csv.DictReader(f))

def grafico(nombre, columna, titulo, archivo, limite=10):
    datos = cargar(nombre)
    datos = sorted(
        datos, key=lambda x: int(x["count"]),
        reverse=True
    )[:limite]

    etiquetas = [d[columna] for d in datos]
    valores = [int(d["count"]) for d in datos]

    plt.figure(figsize=(11, 6))
    plt.barh(etiquetas, valores)
    plt.gca().invert_yaxis()
    plt.title(titulo)
    plt.xlabel("Cantidad de registros")
    plt.tight_layout()
    plt.savefig(
        f"{destino}/{archivo}.png",
        dpi=150
    )
    plt.close()

grafico(
    "tipos", "TIPO_LIMPIO",
    "Vehiculos involucrados por tipo",
    "01_tipos_vehiculo"
)

grafico(
    "departamentos", "DEPARTAMENTO_LIMPIO",
    "Vehiculos involucrados por departamento",
    "02_departamentos"
)

grafico(
    "gravedad", "GRAVEDAD_LIMPIA",
    "Registros segun gravedad del accidente",
    "03_gravedad"
)

datos = sorted(
    cargar("anios"),
    key=lambda x: x["ANIO"]
)

anios = [d["ANIO"] for d in datos]
valores = [int(d["count"]) for d in datos]

plt.figure(figsize=(10, 5))
plt.bar(anios, valores)
plt.title("Registros por año (2026 parcial)")
plt.xlabel("Año")
plt.ylabel("Cantidad de registros")
plt.tight_layout()
plt.savefig(
    f"{destino}/04_distribucion_anual.png",
    dpi=150
)
plt.close()

print("Graficos generados correctamente")
print("Ubicacion:", destino)
