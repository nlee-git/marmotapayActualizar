import os
from dotenv import load_dotenv
import psycopg2
import unicodedata
from game_loader import normalizar_nombre

def conectar():
    load_dotenv()

    try:
        conexion = psycopg2.connect(
            host=os.getenv("DB_HOST"),
            database=os.getenv("DB_NAME"),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASS"),
            port=os.getenv("DB_PORT")
        )
        return conexion

    except (Exception, psycopg2.Error) as error:
        print(f"Error al intentar conectar a la Base de Datos: {error}")
        return None

def cargar_juegos_bd(cur):
    cur.execute('SELECT "idJuego", nombre FROM "JUEGO";')
    return cur.fetchall()

def buscar_juego_normalizado(cur, nombre):
    nombre_norm = normalizar_nombre(nombre)

    cur.execute("""
        SELECT "idJuego", nombre
        FROM "JUEGO";
    """)
    for idj, db_name in cur.fetchall():
        if normalizar_nombre(db_name) == nombre_norm:
            return idj
    return None

def normalizar(texto):
    if not texto:
        return ""
    texto = texto.lower()
    texto = ''.join(c for c in unicodedata.normalize('NFD', texto)
                    if unicodedata.category(c) != 'Mn')
    return texto.replace("®", "").replace("™", "").strip()

def obtener_juegos_bd(cur):
    cur.execute('SELECT "idJuego", "nombre" FROM "JUEGO";')
    juegos = {}
    for idj, nombre in cur.fetchall():
        juegos[normalizar(nombre)] = idj
    return juegos

def obtener_o_crear_genero(cur, genero):
    genero = genero.strip().title()

    cur.execute("""
        SELECT "idGenero"
        FROM "GENERO"
        WHERE LOWER("nombreGenero") = LOWER(%s);
    """, (genero,))
    row = cur.fetchone()

    if row:
        return row[0]

    cur.execute("""
        INSERT INTO "GENERO" ("nombreGenero")
        VALUES (%s)
        RETURNING "idGenero";
    """, (genero,))
    print(f"➕ Creando género nuevo: {genero}")
    return cur.fetchone()[0]

def procesar_juego(nombre, id_juego, cur, conn, game):
    print(f" Procesando: {nombre} (idJuego={id_juego})")

    try:
        # ID
        if game.get("appid"):
            cur.execute("""
                UPDATE "JUEGO"
                SET "idSteam" = %s
                WHERE "idJuego" = %s;
            """, (game["appid"], id_juego))
        # DESCRIPCIÓN    
        if game.get("short_description"):
            print(" Insertando descripción")
            cur.execute("""
                UPDATE "JUEGO"
                SET descripcion = %s
                WHERE "idJuego" = %s;
            """, (game["short_description"], id_juego))
        else:
            print(" Problema con la descripción")
        # FECHA
        if game.get("release_date"):
            print(" Insertando fecha")
            cur.execute("""
                UPDATE "DETALLEJUEGO"
                SET "fechaLanzamiento" = %s
                WHERE "fkJuegoDetalle" = %s;
            """, (game["release_date"], id_juego))
        # CLASIFICACIÓN EDAD
        rating = game.get("age_rating")
        sistema = game.get("age_rating_system")
        
        ID_SIN_CLASIF = 13

        if rating and sistema:
            nombre_clasif = f"{sistema} {rating}"
            cur.execute("""
                SELECT "idClasificacion"
                FROM "CLASIFICACION"
                WHERE LOWER(nombre) = LOWER(%s);
            """, (nombre_clasif,))
            row = cur.fetchone()

            if row:
                id_clasif = row[0]
            else:
                print(f" Clasificación de edad no existe: {nombre_clasif} → usando default")
                id_clasif = ID_SIN_CLASIF
        else:
            print("ℹ Sin clasificación de edad desde API → usando default")
            id_clasif = ID_SIN_CLASIF

        cur.execute("""
            SELECT 1
            FROM "CLASIFICAJUEGO"
            WHERE "fkJuego" = %s;
        """, (id_juego,))

        if cur.fetchone():
            print(f" El juego ya tiene clasificación de edad, se omite")
        else:
            cur.execute("""
                INSERT INTO "CLASIFICAJUEGO" ("fkClasificacion", "fkJuego")
                VALUES (%s, %s)
                ON CONFLICT DO NOTHING;
            """, (id_clasif, id_juego))
        # GÉNEROS
        for genero in game.get("genres", []):
            id_genero = obtener_o_crear_genero(cur, genero)

            cur.execute("""
                INSERT INTO "LISTAGENERO" ("fkJuegoLista", "fkGeneroLista")
                VALUES (%s, %s)
                ON CONFLICT DO NOTHING;
            """, (id_juego, id_genero))
        print(" Género encontrado")
        conn.commit()
        print(f" Juego procesado OK: {nombre}")
        print("*" * 40)

    except Exception as e:
        conn.rollback()
        print(f" ERROR procesando {nombre}: {e}")
        