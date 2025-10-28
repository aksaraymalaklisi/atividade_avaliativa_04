# Atividade Avaliativa 4

## Como rodar

Inicie os contêineres:
`docker compose up -d` (ele irá automaticamente buildar o projeto)

Use `docker logs -f python-consumer` para acessar os prints do consumer.

Rode o `consumer.py` para consumir os dados enviados para o Kafka.

## Notas

`consumer.py` não faz parte da aplicação FastAPI e é apenas um consumidor.
Ele lê de uma .env, mas com os valores padrões ele deve rodar normalmente sem uma.
Ele também possui dependências. Crie uma venv e instale-as:

`cd app/`
`python -m venv venv`
`pip install -r requirements.txt`

E, dentro de `app/`, rode o consumer:

`python consumer.py`

## Adicional

### Estrutura da .env

```bash
KAFKA_BROKER=kafka:9092
KAFKA_BROKER_LOCAL=localhost:29092
KAFKA_TOPIC=fastapi_messages
KAFKA_GROUP_ID=fastapi-group
```
