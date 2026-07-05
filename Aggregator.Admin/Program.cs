using Aggregator.Admin.ExchangeSimulator;
using Aggregator.Admin.Services;
using Aggregator.Core.Services;
using Aggregator.Api.Services;

var builder = WebApplication.CreateBuilder(args);

// Add services to the container.
builder.Services.AddControllersWithViews();

// Register services
builder.Services.AddTransient<ITickRepository, PostgresTickRepository>();
builder.Services.AddSingleton<TickService>();

// Register ExchangeServerManager
builder.Services.AddSingleton<ExchangeServerManager>();
var host = builder.Build();

// Start exchange servers
var manager = host.Services.GetRequiredService<ExchangeServerManager>();
await manager.StartAllAsync();

// Configure the HTTP request pipeline.
if (!host.Environment.IsDevelopment())
{
    host.UseExceptionHandler("/Home/Error");
    // The default HSTS value is 30 days. You may want to change this for production scenarios, see https://aka.ms/aspnetcore-hsts.
    host.UseHsts();
}

host.UseHttpsRedirection();
host.UseStaticFiles();

host.UseRouting();
host.UseWebSockets();

host.UseAuthorization();

host.MapControllerRoute(
    name: "default",
    pattern: "{controller=Home}/{action=Index}/{id?}");

// Запускаем приложение
host.Run();

