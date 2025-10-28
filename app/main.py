import json
import logging
import asyncio # Importado para usar asyncio.sleep
from contextlib import asynccontextmanager

from aiokafka import AIOKafkaProducer
from fastapi import FastAPI, HTTPException

from pydantic import BaseModel
from pydantic_settings import BaseSettings, SettingsConfigDict

# --- Configuração de Retentativa ---
MAX_RETRIES = 5
RETRY_DELAY = 3  # segundos

# Configuração de Logs
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Pydantic para Variáveis de Ambiente
class Settings(BaseSettings):
    """Obtém variáveis de ambiente, incluindo o arquivo .env."""
    KAFKA_BROKER: str
    KAFKA_TOPIC: str

    model_config = SettingsConfigDict(env_file='../.env', extra='ignore')

settings = Settings()

# Schema de Mensagem (Pydantic)
class MessagePayload(BaseModel):
    nome: str
    texto: str

# Inicialização e Desligamento do Produtor
KAFKA_PRODUCER: AIOKafkaProducer = None

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Gerencia o ciclo de vida da aplicação (Producer start/stop) com retentativas."""
    global KAFKA_PRODUCER
    
    logger.info(f"Preparando Produtor Kafka para: {settings.KAFKA_BROKER}")
    KAFKA_PRODUCER = AIOKafkaProducer(
        bootstrap_servers=settings.KAFKA_BROKER,
        # Serializa o valor do JSON para bytes (UTF-8)
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    
    producer_started = False
    
    # Lógica de Retentativa para iniciar o Produtor
    for attempt in range(MAX_RETRIES):
        try:
            logger.info(f"Tentativa {attempt + 1}/{MAX_RETRIES}: Iniciando Produtor Kafka...")
            await KAFKA_PRODUCER.start()
            logger.info("Produtor Kafka iniciado com sucesso.")
            producer_started = True
            break # Sai do loop se a inicialização for bem-sucedida
        except Exception as e:
            logger.warning(f"Erro ao iniciar Produtor Kafka na tentativa {attempt + 1}: {e}")
            if attempt < MAX_RETRIES - 1:
                logger.info(f"Aguardando {RETRY_DELAY} segundos antes de tentar novamente...")
                await asyncio.sleep(RETRY_DELAY)
            else:
                logger.error("Todas as retentativas falharam. Produtor Kafka não pôde ser iniciado.")
    
    # Se o produtor não iniciou após todas as tentativas, o startup da aplicação falha.
    if not producer_started:
        raise RuntimeError("Falha crítica: Não foi possível iniciar o Produtor Kafka após todas as retentativas.")
    
    # O Produtor iniciou com sucesso, cede o controle para a aplicação
    try:
        yield
    finally:
        # Certifica-se de que o produtor será desligado apenas se tiver sido iniciado
        if KAFKA_PRODUCER and producer_started:
            logger.info("Desligando Produtor Kafka.")
            await KAFKA_PRODUCER.stop()
            
# Aplicação FastAPI
app = FastAPI(title="Kafka Producer App", lifespan=lifespan)

@app.get("/")
def health_check():
    return {"status": "ok", "service": "Kafka Producer API"}

# Endpoint REST /enviar (Produtor)
@app.post("/enviar")
async def enviar_mensagem(payload: MessagePayload):
    """
    Recebe um JSON e envia a mensagem para o tópico Kafka.
    """
    message_dict = payload.model_dump()
    topic = settings.KAFKA_TOPIC

    logger.info(f"Recebido para envio: {message_dict}")

    try:
        # Envia a mensagem de forma assíncrona
        await KAFKA_PRODUCER.send_and_wait(topic, message_dict)
        logger.info(f"Mensagem enviada com sucesso para o tópico: {topic}")
        
        return {"status": "Mensagem enviada", "mensagem": message_dict, "topico": topic}
        
    except Exception as e:
        logger.error(f"Erro ao enviar mensagem para o Kafka: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Erro ao enviar mensagem para o Kafka: {e}")