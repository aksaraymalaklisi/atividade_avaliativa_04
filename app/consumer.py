import asyncio
import json
import logging
from aiokafka import AIOKafkaConsumer
from pydantic_settings import BaseSettings, SettingsConfigDict

# Configuração de Logs
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Pydantic para Variáveis de Ambiente
class ConsumerSettings(BaseSettings):
    """Obtém variáveis de ambiente, incluindo o arquivo .env."""
    KAFKA_BROKER_LOCAL: str = 'localhost:29092'
    KAFKA_TOPIC: str = 'fastapi_messages'
    KAFKA_GROUP_ID: str = 'fastapi-group'

    model_config = SettingsConfigDict(extra='ignore')

settings = ConsumerSettings()

# Função de Consumo
async def consume_messages():
    """
    Inicializa o consumidor e lê as mensagens do tópico.
    """
    logger.info(f"Iniciando Consumidor Kafka no tópico '{settings.KAFKA_TOPIC}'...")
    
    # Inicializa o consumidor
    consumer = AIOKafkaConsumer(
        settings.KAFKA_TOPIC,
        bootstrap_servers=settings.KAFKA_BROKER_LOCAL,
        group_id=settings.KAFKA_GROUP_ID,
        auto_offset_reset='earliest',  # Começa lendo desde o início, se for a primeira vez
        # Deserializa os bytes para JSON (UTF-8)
        value_deserializer=lambda m: json.loads(m.decode('utf-8'))
    )

    await consumer.start()
    
    try:
        # Loop principal de consumo
        async for message in consumer:
            # message.value já foi deserializado pelo value_deserializer
            conteudo = message.value
            
            # Exibe no terminal
            print("="*40)
            print(f"✅ Nova Mensagem Recebida")
            print(f"   Tópico: {message.topic}")
            print(f"   Partição: {message.partition}")
            print(f"   Offset: {message.offset}")
            print(f"   Conteúdo: {conteudo}")
            print("="*40)

    except asyncio.CancelledError:
        logger.warning("Consumo de mensagens cancelado.")
    except Exception as e:
        logger.error(f"Erro durante o consumo: {e}", exc_info=True)
    finally:
        logger.info("Desligando Consumidor Kafka.")
        await consumer.stop()

# Execução
if __name__ == "__main__":
    # Roda o loop assíncrono para o consumidor
    asyncio.run(consume_messages())