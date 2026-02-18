# tradingDeskAPI

Hosted Express + EJS API for public/state endpoints.

## Endpoints

- `GET /bj/active`: Read latest active table state.
- `POST /bj/active`: Accept state updates from local scraper agent.
  - Optional auth: set `BJSPY_AGENT_KEY` and send header `x-agent-key`.
- `POST /bj/refresh-request`: Frontend/manual refresh request (creates a pending command).
- `GET /bj/refresh-status`: Current command status for UI polling.
- `GET /bj/refresh-command`: Agent command pull endpoint (returns pending command).
- `POST /bj/refresh-command/ack`: Agent command acknowledgment/status updates.
- `GET /bj/health`: Health + latest sync metadata.

## Run

```bash
npm install
npm run dev
```

Default port is `3001` (set `PORT` to override).

## Local Scraper Push

In `tradingDeskBE`, set:

- `BJSPY_REMOTE_PUSH_URL=http://localhost:3001/bj/active`
- `BJSPY_REMOTE_PUSH_KEY=<same-as-BJSPY_AGENT_KEY-if-set>`
- `BJSPY_REMOTE_COMMAND_PULL_URL=http://localhost:3001/bj/refresh-command`
- `BJSPY_REMOTE_COMMAND_ACK_URL=http://localhost:3001/bj/refresh-command/ack`

Then local live scraper pushes state updates to this API.
