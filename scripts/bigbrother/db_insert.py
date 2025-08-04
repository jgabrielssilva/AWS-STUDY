from datetime import date, datetime
import psycopg2
from secrets_manager import get_db_credentials

# Função para conectar ao banco
def conectar():
    secrets_manager = get_db_credentials('arn:aws:secretsmanager:us-east-2:794038229063:secret:postgresql/bigbrother/db-sm-Kog96K')

    conn = psycopg2.connect(
        host=secrets_manager['host'],
        port=secrets_manager['port'],
        dbname=secrets_manager['dbname'],
        user=secrets_manager['username'],
        password=secrets_manager['password']
    )

    cursor = conn.cursor()
    return conn, cursor

# Funções de inserção

def insert_obras(cursor, conn, nome, localizacao, inicio, fim):
    try:
        query = """
        INSERT INTO obras (nome, localizacao, data_inicio, data_fim)
        VALUES (%s, %s, %s, %s)
        """
        cursor.execute(query, (nome, localizacao, inicio, fim))
        conn.commit()
    except Exception as e:
        print(f"[obra] Erro: {e}")
        conn.rollback()

def insert_videos(cursor, conn, obra_id, caminho_s3, data_upload, processado):
    try:
        query = """
        INSERT INTO videos (obra_id, caminho_s3, data_upload, processado)
        VALUES (%s, %s, %s, %s)
        """
        cursor.execute(query, (obra_id, caminho_s3, data_upload, processado))
        conn.commit()
    except Exception as e:
        print(f"[video] Erro: {e}")
        conn.rollback()

def insert_resultados_analise(cursor, conn, video_id, data_analise, pessoas, alerta, detalhes_json):
    try:
        query = """
        INSERT INTO resultados_analise (video_id, data_analise, pessoas_detectadas, alerta, detalhes_json)
        VALUES (%s, %s, %s, %s, %s)
        """
        cursor.execute(query, (video_id, data_analise, pessoas, alerta, detalhes_json))
        conn.commit()
    except Exception as e:
        print(f"[analise] Erro: {e}")
        conn.rollback()

def insert_alertas(cursor, conn, analise_id, tipo, descricao, nivel):
    try:
        query = """
        INSERT INTO alertas (analise_id, tipo, descricao, nivel)
        VALUES (%s, %s, %s, %s)
        """
        cursor.execute(query, (analise_id, tipo, descricao, nivel))
        conn.commit()
    except Exception as e:
        print(f"[alerta] Erro: {e}")
        conn.rollback()

# Bloco de exemplo para execução direta
if __name__ == "__main__":
    try:
        conn, cursor = conectar()

        # Exemplos de uso — você pode comentar ou modificar conforme necessário
        # insert_obras(cursor, conn, "OBRA-TESTE1", "Rua da Aurora, 150 - Recife, PE", date(2025, 7, 1), date(2026, 12, 31))
        # insert_videos(cursor, conn, 1, "s3://bucket/videos/obra1_video1.mp4", datetime.utcnow(), False)
        # insert_resultados_analise(cursor, conn, 1, datetime.utcnow(), 5, "Funcionário sem colete", '{"frames_com_alerta": [12, 29, 45], "score": 0.89}')
        # insert_alertas(cursor, conn, 2, "EPI", "Funcionário sem capacete", "alto")

    except Exception as e:
        print(f"[main] Erro geral: {e}")
    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals():
            conn.close()
