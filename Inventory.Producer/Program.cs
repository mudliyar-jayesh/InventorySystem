
using System.Text;
using System.Text.Json;
using RabbitMQ.Client;

var factory = new ConnectionFactory { HostName = "localhost", Port = 5673, UserName = "guest", Password = "guest" };
using var connection = await factory.CreateConnectionAsync();
using var channel = await connection.CreateChannelAsync();


await channel.QueueDeclareAsync(queue: "inventory_updates", durable: true, exclusive: false, autoDelete: false);

Console.WriteLine("---Producer Started---");
while (true)
{
    Console.Write("> ");
    var input = Console.ReadLine();
    if (input == "exit") break;

    var intEvent = new InventoryEvent { Sku = input, QtyChange = new Random().Next(1, 100), Source = "Tally" };
    var body = Encoding.UTF8.GetBytes(JsonSerializer.Serialize(intEvent));

    await channel.BasicPublishAsync(exchange: "", routingKey: "inventory_updates", body: body);
    Console.WriteLine($"[Sent] {input} sold event to Queue");
}


public class InventoryEvent
{
    public string Sku { get; set; }
    public int QtyChange { get; set; }
    public string Source { get; set; }
}
