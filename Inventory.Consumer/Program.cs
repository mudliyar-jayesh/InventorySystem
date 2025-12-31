using System.Text;
using System.Text.Json;
using Npgsql;
using Dapper;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using StackExchange.Redis;


const string DbConnectionString = "Host=localhost;Port=5432;Username=admin;Password=betterChimp_314;Database=TallyData";


using (var dbConnection = new NpgsqlConnection(DbConnectionString))
{
    dbConnection.Open();
    dbConnection.Execute(@"
            CREATE TABLE IF NOT EXISTS InventoryLedger (
            Id SERIAL PRIMARY KEY,
            Sku VARCHAR(100),
            QtyChange INT,
            Source VARCHAR(50)
            );
        ");
    Console.WriteLine("[System] Database Connected & Table Verified");
}

var redis = ConnectionMultiplexer.Connect("localhost");
var db = redis.GetDatabase();
Console.WriteLine("Connected to Redis");


var factory = new ConnectionFactory { HostName = "localhost", Port = 5673, UserName = "guest", Password = "guest" };
using var connection = await factory.CreateConnectionAsync();
using var channel = await connection.CreateChannelAsync();

await channel.QueueDeclareAsync(queue: "inventory_updates", durable: true, exclusive: false, autoDelete: false);

//await channel.BasicQosAsync(prefetchSize: 0, prefetchCount: 1, global: false);

Console.WriteLine("---Consumer Listening---");

var consumer = new AsyncEventingBasicConsumer(channel);
consumer.ReceivedAsync += async (model, ea) =>
{
    var body = ea.Body.ToArray();
    var message = Encoding.UTF8.GetString(body);

    try
    {
        var intEvent = JsonSerializer.Deserialize<InventoryEvent>(message);

        using (var dbConn = new NpgsqlConnection(DbConnectionString))
        {
            var sql = "INSERT INTO InventoryLedger (Sku, QtyChange, Source) VALUES (@Sku, @QtyChange, @Source)";
            await dbConn.ExecuteAsync(sql, intEvent);
        }

        var newStock = await db.StringIncrementAsync($"stock:{intEvent.Sku}", intEvent.QtyChange);

        await db.PublishAsync("events", JsonSerializer.Serialize(new
        {
            sku = intEvent.Sku,
            count = (int)newStock,
            source = "RealTime"
        }));

        Console.WriteLine($"[Processed] Item: {intEvent.Sku} | Qty: {intEvent.QtyChange} | Source :{intEvent.Source} ");

        await channel.BasicAckAsync(deliveryTag: ea.DeliveryTag, multiple: false);
    }
    catch (Exception error)
    {
        Console.WriteLine($"[Error]: {error.Message}");
    }
};

await channel.BasicConsumeAsync(queue: "inventory_updates", autoAck: false, consumer: consumer);


Console.ReadLine();

public class InventoryEvent
{
    public string Sku { get; set; }
    public int QtyChange { get; set; }
    public string Source { get; set; }
}
