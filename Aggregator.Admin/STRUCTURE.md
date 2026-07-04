# Project Structure

## Aggregator.Admin

### Controllers
- `ExchangeAdminController.cs` - Admin interface for managing exchange servers
- `HomeController.cs` - Default home page

### ExchangeSimulator
- `ExchangeServer.cs` - Individual exchange server implementation
- `ExchangeServerManager.cs` - Manager for all exchange servers

### Views
- `Views/Home/Index.cshtml` - Home page with link to admin
- `Views/Home/Privacy.cshtml` - Privacy page
- `Views/ExchangeAdmin/Index.cshtml` - Main server list page
- `Views/ExchangeAdmin/Logs.cshtml` - Server logs page
- `Views/ExchangeAdmin/Settings.cshtml` - Server settings page

### wwwroot
- CSS and JS files for the UI
- Bootstrap framework

### Program.cs
- Main application entry point
- Configuration loading
- Server startup and shutdown

## Configuration

The `appsettings.json` file contains the configuration for exchange servers:

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

## Functionality

1. **Multiple Exchange Servers** - Each server runs on its own port
2. **Start/Stop Control** - Individual server control
3. **Frequency Settings** - Configure how often messages are sent
4. **Real-time Monitoring** - View server status and logs
5. **WebSocket Interface** - Simulated exchange data via WebSocket