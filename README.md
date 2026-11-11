# Jingg Zapi

API de WhatsApp multi-dispositivo construída sobre [Baileys](https://github.com/WhiskeySockets/Baileys) 7.
Vários números conectados ao mesmo tempo, webhooks por dispositivo e um desenho em que **nada que entrou
no sistema se perde**: mensagens e eventos são gravados no Postgres antes de qualquer confirmação.

## Subindo

```bash
cp .env.example .env        # preencha ADMIN_API_KEY, POSTGRES_PASSWORD e MINIO_ROOT_PASSWORD
docker compose up -d --build
```

- API: `http://localhost:3000` — documentação interativa (Swagger) em `/docs`
- Console do MinIO: `http://localhost:9001`

Mais capacidade de conexões: `docker compose up -d --scale worker=3`.

## Primeiros passos

```bash
ADMIN=<ADMIN_API_KEY do .env>

# 1. cria o dispositivo já com um webhook; guarde a apiKey devolvida (só aparece aqui)
curl -X POST localhost:3000/v1/devices -H "x-api-key: $ADMIN" -H 'content-type: application/json' \
  -d '{"name":"atendimento","webhook":{"url":"https://meu-sistema/webhooks/whatsapp","secret":"s3gr3do"}}'

# 2. pareia: abra o PNG e escaneie (ou use POST /pairing-code com o número)
curl "localhost:3000/v1/devices/$ID/qr?format=png" -H "x-api-key: $KEY" -o qr.png

# 3. envia
curl -X POST localhost:3000/v1/devices/$ID/messages -H "x-api-key: $KEY" -H 'content-type: application/json' \
  -d '{"to":"5511999990000","type":"text","text":"Olá!"}'
```

Autenticação pelo header `x-api-key` (ou `Authorization: Bearer`). A chave de administrador acessa tudo;
a chave de dispositivo só as rotas `/v1/devices/{id}/…` do próprio dispositivo — é a que você entrega a cada projeto.

## Bibliotecas cliente

Para consumir a API sem escrever HTTP na mão — informam-se a URL e a chave:

- **C# / .NET** — [jingg.zapi-dotnet](https://github.com/4RTHURsantos/jingg.zapi-dotnet)
- **TypeScript / JavaScript** — [jingg.zapi-js](https://github.com/4RTHURsantos/jingg.zapi-js)

## Arquitetura

```
              ┌────────┐  fila de envio (Postgres) + wake (Redis pub/sub)   ┌──────────┐
 cliente ───▶ │  api   │ ─────────────────────────────────────────────────▶ │ worker ×N │ ◀──▶ WhatsApp
              └────────┘ ◀──────────── RPC (Redis pub/sub) ───────────────▶ └────┬─────┘
                                                                                 │ evento + entregas (1 transação)
              ┌────────────┐      jobs (BullMQ/Redis)        ┌──────────┐        ▼
 seu sistema ◀│ dispatcher │ ◀────────────────────────────── │ Postgres │   MinIO (mídia)
   (webhook)  └────────────┘                                 └──────────┘
```

| Serviço      | Papel |
|--------------|-------|
| `api`        | REST + Swagger. Só grava no Postgres e conversa com os workers; não mantém conexões. |
| `worker`     | Mantém os sockets Baileys. Escala horizontalmente. |
| `dispatcher` | Entrega os webhooks com retry. |
| `postgres`   | Fonte da verdade: sessões, mensagens, fila de envio, eventos e entregas. |
| `redis`      | Coordenação: leases de dispositivo, pub/sub, QR codes e fila de webhooks (BullMQ). |
| `minio`      | Mídia recebida e mídia enviada em base64. |

**Distribuição de dispositivos.** Cada dispositivo pertence ao worker que detém o seu *lease* no Redis
(`SET NX PX`, renovado a cada ⅓ do TTL). Worker caiu → o lease expira (30s por padrão) e outro assume,
carregando a sessão do Postgres, sem novo QR. Worker novo → os demais cedem dispositivos até equilibrar.
Um worker que não consegue renovar o lease fecha o socket na hora, para nunca haver duas conexões com a mesma sessão.

## Garantias de integridade

**Envio (API → WhatsApp)**
- `POST /messages` só responde `202` depois que a mensagem está no Postgres. A fila de envio **é** uma tabela;
  o Redis apenas acorda o worker (que também faz polling), então perder o Redis não perde mensagens.
- Ordem preservada por dispositivo. Dispositivo offline → a mensagem espera a reconexão, sem consumir tentativas.
- O id da mensagem no WhatsApp é gravado **antes** do envio; uma retentativa após queda reutiliza o mesmo id,
  então o destinatário não recebe duplicado.
- `idempotencyKey` (corpo ou header `Idempotency-Key`): repetir a chamada devolve a mensagem original.
- Números de telefone são conferidos no WhatsApp antes do envio e trocados pelo JID real da conta (resolve o caso
  do 9º dígito no Brasil); número sem WhatsApp falha na hora com `message.failed`, sem retentativas.
- Falhas são retentadas com backoff (5 tentativas por padrão); ao esgotar, sai o evento `message.failed`
  e a mensagem pode ser recolocada com `POST /messages/outbound/{id}/retry`.

**Recebimento (WhatsApp → você)**
- Cada mensagem é gravada com chave única `(dispositivo, chat, id, fromMe)` — reentregas do WhatsApp viram no-op.
- Mensagem e evento são gravados na **mesma transação** (outbox transacional), junto com uma linha de entrega
  por webhook assinante. Commit feito = evento será entregue.
- Mídia: a mensagem é persistida primeiro, o download acontece depois e só então o evento é emitido. Se o
  worker cair no meio, a próxima conexão retoma de onde parou.
- Mensagens que chegaram com o dispositivo offline são entregues pelo WhatsApp na reconexão e passam pelo mesmo caminho.

**Webhooks**
- Entrega *ao menos uma vez*. Sucesso = resposta `2xx` em até 10s. Qualquer outra coisa é reenviada com backoff
  exponencial de 5s até 1h (12 tentativas ≈ 2h30 de tolerância a indisponibilidade; ajustável).
- A tabela de entregas é o registro durável; o BullMQ só agenda. Um reconciliador reenfileira a cada 30s
  qualquer entrega pendente que não tenha job — cobre falha entre o commit e o enqueue e perda de dados do Redis.
- Esgotou as tentativas → fica como `failed`, visível em `GET /webhook-deliveries?status=failed` e reenviável
  com `POST /webhook-deliveries/retry-failed`.
- **Deduplicação e ordem são do consumidor:** use `id` (único por evento) para descartar repetidos e `seq`
  (sequencial e sem buracos por dispositivo) para ordenar — entregas concorrentes podem chegar fora de ordem.
- Alternativa sem webhook: `GET /events?after=<seq>` devolve o mesmo fluxo por polling.

### Limites conhecidos (sejamos honestos)

- O Baileys confirma o recebimento ao WhatsApp antes de entregar a mensagem à aplicação. Se o processo morrer
  exatamente entre essa confirmação e o commit no Postgres (janela de milissegundos), aquela mensagem não é
  reenviada pelo servidor. Falhas de banco nesse ponto são retentadas por ~30s antes de desistir (com log `EVENT DROPPED`).
- "Enviada" (`sent`) significa aceita pelo socket do WhatsApp. A confirmação de entrega ao destinatário chega
  depois, pelo evento `message.status` (`delivered`, `read`).
- Baileys é uma biblioteca não oficial: o WhatsApp pode banir números que façam spam ou disparo em massa.
  `SEND_INTERVAL_MS` espaça os envios, mas não substitui bom senso.
- As URLs de mídia nos eventos são pré-assinadas e expiram (7 dias, o máximo do S3). Depois disso use
  `GET /messages/{messageId}/media`, que gera uma URL nova.

## Webhook

```http
POST /seu/endpoint
X-Zapi-Event: message.received
X-Zapi-Event-Id: 7c0e…        ← use para deduplicar
X-Zapi-Delivery-Id: 1b9f…
X-Zapi-Device-Id: 4a1d…
X-Zapi-Attempt: 1
X-Zapi-Signature: sha256=<HMAC-SHA256(secret, corpo bruto)>   ← só se o webhook tiver secret

{
  "id": "7c0e…", "seq": 42, "deviceId": "4a1d…",
  "type": "message.received", "timestamp": "2026-11-11T12:00:00.000Z",
  "data": {
    "id": "3EB0…", "chat": "5511988887777@s.whatsapp.net", "sender": "5511988887777@s.whatsapp.net",
    "fromMe": false, "isGroup": false, "pushName": "Maria", "timestamp": "2026-11-11T11:59:59.000Z",
    "type": "image", "text": "legenda",
    "media": { "status": "stored", "mimetype": "image/jpeg", "size": 48213, "url": "http://…" }
  }
}
```

Verificando a assinatura (Node):

```js
const expected = 'sha256=' + crypto.createHmac('sha256', SECRET).update(rawBody).digest('hex')
const ok = crypto.timingSafeEqual(Buffer.from(expected), Buffer.from(req.headers['x-zapi-signature']))
```

| Evento | Quando |
|--------|--------|
| `connection.update` | Mudança de estado: `connecting`, `qr`, `connected`, `reconnecting`, `disconnected`, `logged_out` |
| `qr.updated` | Novo QR code disponível |
| `message.received` / `message.sent` | Mensagem recebida / enviada (pela API — com `outboundId` — ou pelo celular) |
| `message.status` | `sent` → `delivered` → `read` → `played` |
| `message.failed` | Envio pela API esgotou as tentativas ou foi rejeitado |
| `message.edited` / `message.deleted` / `message.reaction` / `message.poll_vote` / `message.receipt` | Alterações em mensagens |
| `group.updated` / `group.participants` / `group.join_request` | Grupos |
| `call.update` / `presence.update` | Chamadas e presença |

Um webhook pode assinar todos os eventos (padrão) ou uma lista. Cada dispositivo pode ter vários webhooks.

**LID × número.** O WhatsApp vem trocando números por identificadores `@lid`. Quando o chat/remetente chega
como LID, `chatAlt`/`senderAlt` trazem o JID com o número (quando o WhatsApp o informa), e vice-versa.

## API em resumo

Tudo sob `/v1`, detalhado em `/docs`.

| Área | Rotas |
|------|-------|
| Dispositivos | `POST/GET /devices`, `GET/PATCH/DELETE /devices/{id}`, `POST …/connect`, `…/disconnect`, `…/restart`, `…/logout`, `…/rotate-key`, `GET …/qr`, `POST …/pairing-code`, `PATCH …/profile` |
| Mensagens | `POST …/messages` (text, image, video, audio, document, sticker, location, contact, poll, reaction, edit, delete), `GET …/messages`, `GET …/messages/{id}`, `GET …/messages/{id}/media`, `POST …/messages/read`, `GET/DELETE …/messages/outbound/{id}`, `POST …/messages/outbound/{id}/retry` |
| Webhooks | `GET/POST …/webhooks`, `PATCH/DELETE …/webhooks/{id}`, `POST …/webhooks/{id}/test`, `GET …/webhook-deliveries`, `POST …/webhook-deliveries/{id}/retry`, `POST …/webhook-deliveries/retry-failed` |
| Eventos | `GET …/events?after=` |
| Contatos | `POST …/contacts/check`, `GET …/contacts/{jid}/picture`, `…/status`, `…/business`, `POST …/contacts/{jid}/block`, `POST …/presence`, `POST …/calls/reject` |
| Grupos | `GET/POST …/groups`, `GET/PATCH …/groups/{jid}`, `POST …/participants`, `GET …/invite-code`, `POST …/invite-code/revoke`, `POST …/groups/join`, `POST …/leave` |
| Sistema | `GET /health`, `GET /ready`, `GET /metrics` (Prometheus), `GET /docs` |

## Operação

- **Métricas** (Prometheus): a API em `:3000/metrics`; worker e dispatcher em `:9100/metrics` dentro da rede do
  compose. Destaques: `zapi_worker_sessions{state}`, `zapi_webhook_deliveries_pending`,
  `zapi_webhook_deliveries_total{outcome}`, `zapi_messages_failed_total`, `zapi_session_reconnects_total{code}`.
- **Backup**: o volume `postgres-data` contém tudo o que importa (inclusive as sessões do WhatsApp — trate como segredo).
  `minio-data` guarda as mídias. O Redis pode ser recriado do zero sem perda.
- **Produção**: coloque a API atrás de um proxy com TLS, não exponha Postgres/Redis, e aponte `S3_PUBLIC_ENDPOINT`
  para o endereço pelo qual os consumidores alcançam o MinIO.
- **Retenção**: eventos e entregas concluídos são apagados após `EVENT_RETENTION_DAYS` (30). O histórico de
  mensagens é mantido para sempre, a menos que `MESSAGE_RETENTION_DAYS` seja definido.

## Desenvolvimento

```bash
npm install
docker compose up -d postgres redis minio      # só a infraestrutura
npm run build && npm run migrate
npm run dev:api & npm run dev:worker & npm run dev:dispatcher
npm test
```

Para rodar fora do Docker, exponha as portas do Postgres/Redis ou ajuste `DATABASE_URL`, `REDIS_URL` e `S3_*`.
