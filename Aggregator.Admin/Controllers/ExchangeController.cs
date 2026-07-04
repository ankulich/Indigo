using Microsoft.AspNetCore.Mvc;
using Aggregator.Admin.ExchangeSimulator;

namespace Aggregator.Admin.Controllers
{
    /// <summary>
    /// Контроллер для управления серверами бирж.
    /// </summary>
    public class ExchangeController : Controller
    {
        private readonly ExchangeServerManager _serverManager;

        public ExchangeController(ExchangeServerManager serverManager)
        {
            _serverManager = serverManager ?? throw new ArgumentNullException(nameof(serverManager));
        }

        public IActionResult Index()
        {
            var servers = _serverManager.GetAllServers();
            return View(servers);
        }

        public IActionResult Logs(string name)
        {
            if (string.IsNullOrEmpty(name))
                return BadRequest("Server name is required");

            var server = _serverManager.GetServer(name);
            if (server == null)
                return NotFound();

            return View(server);
        }

        public IActionResult Settings(string name)
        {
            if (string.IsNullOrEmpty(name))
                return BadRequest("Server name is required");

            var server = _serverManager.GetServer(name);
            if (server == null)
                return NotFound();

            return View(server);
        }

        [HttpPost]
        public async Task<IActionResult> UpdateFrequency(string name, int frequency)
        {
            if (string.IsNullOrEmpty(name))
                return BadRequest("Server name is required");

            var server = _serverManager.GetServer(name);
            if (server == null)
                return NotFound();

            await server.UpdateFrequency(frequency);

            // Если сервер запущен, перезапускаем его для применения новых настроек
            if (server.Enabled)
            {
                await _serverManager.StopServerAsync(name);
                await _serverManager.StartServerAsync(name);
            }

            return RedirectToAction("Settings", new { name = name });
        }

        [HttpPost]
        public async Task<IActionResult> Start(string name)
        {
            if (string.IsNullOrEmpty(name))
                return BadRequest("Server name is required");

            await _serverManager.StartServerAsync(name);
            return RedirectToAction("Index");
        }

        [HttpPost]
        public async Task<IActionResult> Stop(string name)
        {
            if (string.IsNullOrEmpty(name))
                return BadRequest("Server name is required");

            await _serverManager.StopServerAsync(name);
            return RedirectToAction("Index");
        }

        [HttpPost]
        public async Task<IActionResult> Disconnect(string name)
        {
            if (string.IsNullOrEmpty(name))
                return BadRequest("Server name is required");

            var server = _serverManager.GetServer(name);
            if (server == null)
                return NotFound();

            server.DisconnectClient();

            return RedirectToAction("Index");
        }

        [HttpPost]
        public async Task<IActionResult> Resend(string name, int count)
        {
            if (string.IsNullOrEmpty(name))
                return BadRequest("Server name is required");

            var server = _serverManager.GetServer(name);
            if (server == null)
                return NotFound();

            server.ResendLastMessage(count);

            return RedirectToAction("Index");
        }
    }
}
