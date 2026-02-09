from db_utils import conectar, obtener_juegos_bd, procesar_juego
from steam_utils import get_appDetail
from game_loader import normalizar_nombre, resolver_appids

def cargar_lista(path):
    with open(path, encoding="utf-8") as f:
        return [l.strip() for l in f if l.strip()]


def main():
    conn = conectar()

    if conn is not None:
        cur = conn.cursor()

        lista = cargar_lista("games.txt")
        juegos_bd = obtener_juegos_bd(cur)

        existentes = []
        faltantes = []

        for nombre in lista:
            key = normalizar_nombre(nombre)
            if key in juegos_bd:
                existentes.append((nombre, juegos_bd[key]))
            else:
                faltantes.append(nombre)

        # Guardar faltantes
        with open("juegos_faltantes.txt", "w", encoding="utf-8") as f:
            for j in faltantes:
                f.write(j + "\n")

        print(f"\nRESUMEN")
        print(f"Total lista: {len(lista)}")
        print(f"En BD: {len(existentes)}")
        print(f"NO en BD: {len(faltantes)}")

        # Ver cuántos steamid se encontraron o no

        nombres_existentes = [nombre for nombre, _ in existentes]
        found_appids, games_without_id = resolver_appids(nombres_existentes)

        print("\nREPORTE STEAM")
        print(f"Juegos con SteamID: {len(found_appids)}")
        print(f"Juegos SIN SteamID: {len(games_without_id)}")
        print("-" * 50)

        # Procesar existentes con appid
        mapa_juegos_bd = {nombre: id_juego for nombre, id_juego in existentes}

        for game in found_appids:
            nombre = game["name"]
            id_juego = mapa_juegos_bd.get(nombre)

            if not id_juego:
                print(f"Juego no encontrado en BD: {nombre}")
                continue

            details, status = get_appDetail(game["appid"])

            if not details:
                print(f"No Steam data para {nombre}: {status}")
                print("*" * 50)
                continue

            procesar_juego(nombre, id_juego, cur, conn, details)

        cur.close()
        conn.close()
        print("FIN")
    else:
        print("No es posible conectarse a la Base de Datos")

if __name__ == "__main__":
    main()
