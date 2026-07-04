# Aggregator.Admin - Exchange Simulator Management

This admin interface allows you to manage multiple WebSocket exchange simulators.

## Features

1. **Server Management** - Start/stop individual exchange servers
2. **Real-time Logs** - View server status and logs
3. **Frequency Configuration** - Adjust how often messages are sent from each server

## Configuration

The exchange servers are configured in `appsettings.json` with the following structure:

```json
{
  "ExchangeServers": {
    "Binance": {
      "Port": 8080,
      "Enabled": true,
      "Frequency": 1000
    },
    "Coinbase": {
      "Port": 8081,
      "Enabled": false,
      "Frequency": 1500
    },
    "Kraken": {
      "Port": 8082,
      "Enabled": true,
      "Frequency": 2000
    }
  }
}
```

## Usage

1. Run the Aggregator.Admin application
2. Navigate to the "Exchange Servers" page in the admin interface
3. Use the controls to start/stop servers and adjust their settings

## Server Endpoints

Each exchange simulator will be available at:
- `ws://localhost:8080` (Binance)
- `ws://localhost:8081` (Coinbase)
- `ws://localhost:8082` (Kraken)

## WebSocket Messages

Messages are sent in the following format:

```json
{
  "symbol": "BTCUSDT",
  "price": 50000.00,
  "volume": 0.5000
}
```

The price and volume are randomly generated values.