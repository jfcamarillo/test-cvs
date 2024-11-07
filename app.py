import asyncio
import aiohttp
import json
import os
from fastapi import FastAPI, HTTPException
from google.cloud import storage
from pydantic import BaseModel, validator
from datetime import datetime
import uvicorn
import logging


logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

app = FastAPI()

def validate_date_format(date_string: str) -> str:
    try:
        datetime.strptime(date_string, "%Y-%m-%dT%H:%M:%S")
        return date_string
    except ValueError:
        raise ValueError("Incorrect date format, should be YYYY-MM-DDTHH:MM:SS")

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

    @validator('fecha_ini', 'fecha_fin')
    def validate_dates(cls, v):
        return validate_date_format(v)

    @validator('skip_increment', 'concurrency_limit')
    def validate_positive_int(cls, v):
        if v <= 0:
            raise ValueError("Must be a positive integer")
        return v


from google.api_core import retry
import tenacity

@retry.Retry(predicate=retry.if_exception_type(Exception))
def upload_to_gcs_with_retry(blob, content):
    blob.upload_from_string(content, content_type='application/json')

async def upload_to_gcs(blob_name, content):
    """Sube un archivo JSON a Google Cloud Storage de manera asíncrona con reintentos."""
    
    logging.info(f"Enviando {blob_name}")
    storage_client = storage.Client()
    blob = storage_client.bucket(BUCKET_NAME).blob(blob_name)

    retry_strategy = tenacity.retry(
        stop=tenacity.stop_after_attempt(5),
        wait=tenacity.wait_exponential(multiplier=1, min=4, max=10),
        retry=tenacity.retry_if_exception_type(Exception),
        before=tenacity.before_log(logging.getLogger(), logging.INFO),
        after=tenacity.after_log(logging.getLogger(), logging.INFO),
    )

    @retry_strategy
    def upload_with_retry():
        return asyncio.to_thread(upload_to_gcs_with_retry, blob, content)

    try:
        await upload_with_retry()
    except Exception as e:
        logging.error(f"Error al subir {blob_name} después de múltiples intentos: {str(e)}")
        raise


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
    logging.info(f"Iniciando procesamiento con parámetros: {input_data}")
    url_complemento = "Candidate?$select=resume/ownerId,resume/fileName,resume/fileExtension,resume/softDelete,resume/lastModifiedDateTime,resume/fileContent&$expand=resume"
    url = "{0}{1}&$skip={2}&$top={3}&$format=JSON&$filter=resume/lastModifiedDateTime ge datetime'{4}' and resume/lastModifiedDateTime lt datetime'{5}'"

    try:
        async with aiohttp.ClientSession() as session:
            skip = 0
            semaphore = asyncio.Semaphore(input_data.concurrency_limit)
            while True:
                tasks = []
                for i in range(input_data.concurrency_limit):
                    task = asyncio.create_task(
                        fetch_and_process_data(
                            session,
                            semaphore,
                            url.format(ENDPOINT_URL, url_complemento, skip + i * input_data.skip_increment, input_data.skip_increment, input_data.fecha_ini, input_data.fecha_fin)
                        )
                    )
                    tasks.append(task)
                skip += input_data.skip_increment * len(tasks)

                results = await asyncio.gather(*tasks, return_exceptions=True)
                if any(isinstance(result, Exception) for result in results):
                    for result in results:
                        if isinstance(result, Exception):
                            logging.error(f"Error en la solicitud: {str(result)}")
                    raise HTTPException(status_code=500, detail="Se produjeron errores durante el procesamiento")

                if any(len(result) < input_data.skip_increment for result in results if result is not None):
                    break

        logging.info("Procesamiento completado exitosamente")
        return {"message": "Procesamiento completado"}
    except Exception as e:
        logging.error(f"Error inesperado: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error interno del servidor: {str(e)}")

async def fetch_and_process_data(session, semaphore, url):
    async with semaphore:
        try:
            data = await fetch_data(session, url)
            return await process_response(data)
        except Exception as e:
            logging.error(f"Error en fetch_and_process_data: {str(e)}")
            raise


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8080)
