import json
import os
import boto3
from db_insert import insert_videos, insert_alertas, conectar
from datetime import datetime

def validar_variaveis():
    variaveis_esperadas = ["DB_HOST", "DB_USER", "DB_PASSWORD", "DB_NAME"]
    faltando = [var for var in variaveis_esperadas if not os.environ.get(var)]
    if faltando:
        raise Exception(f"Variáveis de ambiente faltando: {', '.join(faltando)}")
    print("✅ Todas as variáveis estão presentes.")

def extract_obra_id_from_filename(object_key):
    # Exemplo de object_key: 'OBRAS/obra001_20250804_01.mp4'
    filename = object_key.split('/')[-1]         # pega 'obra001_20250804_01.mp4'
    obra_prefix = filename.split('_')[0]         # pega 'obra001'
    return obra_prefix

def lambda_handler(event, context):
    validar_variaveis()

    record = event['Records'][0]
    bucket = record['s3']['bucket']['name']
    object_key = record['s3']['object']['key']
    video_path = f"s3://{bucket}/{object_key}"

    obra_id_raw = extract_obra_id_from_filename(object_key)  # ex: 'obra001'
    obra_nome = f"OBRA-{obra_id_raw[-3:]}"  # ex: 'OBRA-001'

    conn, cursor = conectar()

    try:
        cursor.execute("SELECT id FROM obras WHERE nome = %s", (obra_nome,))
        resultado = cursor.fetchone()

        if resultado:
            obra_id_real = resultado[0]
            insert_videos(cursor, conn, obra_id_real, video_path, datetime.utcnow(), False)

            insert_alertas(cursor, conn, analise_id=1, tipo="upload", descricao="Novo vídeo recebido", nivel="médio")

            print(f"[lambda_handler] ✅ Vídeo vinculado à obra {obra_nome} (ID {obra_id_real}).")
        else:
            print(f"[lambda_handler] ❌ Obra '{obra_nome}' não encontrada. Nenhum vídeo foi inserido.")

    except Exception as e:
        print(f"[lambda_handler] Erro: {e}")

    finally:
        cursor.close()
        conn.close()

    return {
        'statusCode': 200,
        'body': json.dumps(f'Processamento finalizado para obra {obra_nome}.')
    }