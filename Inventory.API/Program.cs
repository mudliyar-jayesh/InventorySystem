using StackExchange.Redis;
using Microsoft.AspNetCore.SignalR;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddSignalR();
builder.Services.AddCors(options => options.AddDefaultPolicy(p => p.AllowAnyHeader().AllowAnyMethod().AllowCredentials().SetIsOriginAllowed(_ => true)));

var redis = ConnectionMultiplexer.Connect("localhost");
builder.Services.AddSingleton<IConnectionMultiplexer>(redis);

builder.Services.AddHostedService<RedisSubscriberService>();

var app = builder.Build();

app.UseCors();

app.MapHub<StockHub>("/hubs/stock");

app.MapGet("/stock/{sku}", async (IConnectionMultiplexer redis, string sku) =>
{
    var db = redis.GetDatabase();

    var stockCount = await db.StringGetAsync($"stock:{sku}");

    if (stockCount.IsNull)
    {
        return Results.Ok(new { Sku = sku, Count = 0, Source = "Cache" });
    }
    return Results.Ok(new { Sku = sku, Count = (int)stockCount, Source = "Cache" });
});

app.Run();


public class RedisSubscriberService : BackgroundService
{
    private readonly IConnectionMultiplexer _redis;
    private readonly IHubContext<StockHub> _hubContext;

    public RedisSubscriberService(IConnectionMultiplexer redis, IHubContext<StockHub> hubContext)
    {
        _redis = redis;
        _hubContext = hubContext;
    }


    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var subscriber = _redis.GetSubscriber();

        await subscriber.SubscribeAsync("events", async (channel, message) =>
        {
            await _hubContext.Clients.All.SendAsync("ReceiveStockUpdate", message.ToString());
        });
    }
}
