import asyncio
import aiohttp
import json
import os
from fastapi import FastAPI, HTTPException
from google.cloud import storage
from pydantic import BaseModel
import uvicorn
import logging


logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

app = FastAPI()

OAUTH_USERNAME = os.environ.get("OAUTH_USERNAME")
OAUTH_PASSWORD = os.environ.get("OAUTH_PASSWORD")
BUCKET_NAME = os.environ.get("BUCKET_NAME")
ENDPOINT_URL = os.environ.get("ENDPOINT_URL")


# Definir el modelo de datos para la entrada JSON
class ProcessDataInput(BaseModel):
    fecha_ini: str
    fecha_fin: str
    skip_increment: int = 10
    concurrency_limit: int = 5


async def upload_to_gcs(blob_name, content):
    """Sube un archivo JSON a Google Cloud Storage de manera asíncrona."""
    
    logging.info(f"Enviando {blob_name}")
    storage_client = storage.Client()
    blob = storage_client.bucket(BUCKET_NAME).blob(blob_name)

    await asyncio.to_thread(blob.upload_from_string, content, content_type='application/json')


async def process_response(data):
    """Procesa una respuesta JSON y guarda los resultados en GCS."""
    results = data.get('d', {}).get('results', [])
    upload_tasks = [
        upload_to_gcs(f'{diccionario["resume"]["ownerId"]}.json', json.dumps(diccionario, indent=4))
        for j, diccionario in enumerate(results)
    ]
    await asyncio.gather(*upload_tasks)
    return results


async def fetch_data(session, url):
    try:
        async with session.get(url, auth=aiohttp.BasicAuth(OAUTH_USERNAME, OAUTH_PASSWORD), timeout=aiohttp.ClientTimeout(total=300)) as response:
            return await response.json()
    except asyncio.TimeoutError:
        raise HTTPException(status_code=504, detail="Request timed out")
    except aiohttp.ClientError as e:
        raise HTTPException(status_code=500, detail=f"Client error: {str(e)}")
    

@app.post("/process")
async def process_data(input_data: ProcessDataInput):
    """Obtiene datos de un endpoint de manera asíncrona."""
    url_complemento = "Candidate?$select=resume/ownerId,resume/fileName,resume/fileExtension,resume/softDelete,resume/lastModifiedDateTime,resume/fileContent&$expand=resume"
    url = "{0}{1}&$skip={2}&$top={3}&$format=JSON&$filter=resume/lastModifiedDateTime ge datetime'{4}' and resume/lastModifiedDateTime lt datetime'{5}'"

    async with aiohttp.ClientSession() as session:
        skip = 0
        while True:
            tasks = [
                fetch_data(session, url.format(ENDPOINT_URL, url_complemento, skip + i * input_data.skip_increment, input_data.skip_increment, input_data.fecha_ini, input_data.fecha_fin))
                for i in range(input_data.concurrency_limit)
            ]
            skip += input_data.skip_increment * len(tasks)

            try:
                responses = await asyncio.gather(*tasks)
            except HTTPException as e:
                return {"error": str(e.detail)}
            
            for data in responses:
                result = await process_response(data)

            if len(result) < input_data.skip_increment:
                break

    return {"message": "Procesamiento completado"}


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8080)