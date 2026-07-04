using Microsoft.AspNetCore.Mvc;
using Aggregator.Admin.Services;
using Aggregator.Admin.Models;

namespace Aggregator.Admin.Controllers
{
    /// <summary>
    /// Контроллер для работы с тиками.
    /// </summary>
    [Route("[controller]")]
    public class TicksController : Controller
    {
        private readonly TickService _tickService;

        public TicksController(TickService tickService)
        {
            _tickService = tickService ?? throw new ArgumentNullException(nameof(tickService));
        }

        [HttpGet]
        public async Task<IActionResult> Index(int page = 1, int pageSize = 50)
        {
            if (page < 1) page = 1;
            if (pageSize < 1 || pageSize > 100) pageSize = 50;

            var ticks = await _tickService.GetTicksAsync(pageSize);
            ViewBag.CurrentPage = page;
            ViewBag.PageSize = pageSize;

            return View(ticks);
        }

        [HttpGet("GetTicks")]
        public async Task<IActionResult> GetTicks(int page = 1, int pageSize = 50)
        {
            if (page < 1) page = 1;
            if (pageSize < 1 || pageSize > 100) pageSize = 50;

            var ticks = await _tickService.GetTicksAsync(pageSize);
            return Json(ticks);
        }
    }
}