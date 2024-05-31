using System;
using System.Text;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using InfluxDB.Client;
using InfluxDB.Client.Api.Domain;
using InfluxDB.Client.Writes;
using Ajeevi.IoT.RabbitMqListenerStateless;

  class Program
{
    static void Main(string[] args)
    {
        // InfluxDB credentials
        string influxUrl = "http://122.176.158.205:8086";
        string token = "HI-lJuWEidQmPPuXOL1Qy9bZYwoA_fDDaVII1D8J2cBglrV-3QEmTeCAXinyyectbpOJCff0muWUOU8ur7Z9Lw==";
        string org = "Ajeevi";
        string bucket = "iotdata8";

        // RabbitMQ setup
        var factory = new ConnectionFactory { HostName = "localhost" };
        using (var connection = factory.CreateConnection())
        using (var channel = connection.CreateModel())
        {
            channel.QueueDeclare(queue: "ajeevi.stateless",
                                 durable: true,
                                 exclusive: false,
                                 autoDelete: false,
                                 arguments: null);

            var consumer = new EventingBasicConsumer(channel);
            consumer.Received += async (model, ea) =>
            {
                try
                {
                    var body = ea.Body.ToArray();
                    var message = Encoding.UTF8.GetString(body);
                    Console.WriteLine($"Received message from RabbitMQ: {message}");

                    var point = new DataParser().ParseDataPacketForInflux(message);
                    if (point != null)
                    {
                        using (var influxDBClient = InfluxDBClientFactory.Create(influxUrl, token.ToCharArray()))
                        {
                            var writeApiAsync = influxDBClient.GetWriteApiAsync();
                            await writeApiAsync.WritePointAsync(point, bucket, org);
                            Console.WriteLine("Data sent to InfluxDB.");
                        }
                    }
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"Error processing message: {ex.Message}");
                }
            };

            channel.BasicConsume(queue: "ajeevi.stateless",
                                 autoAck: true,
                                 consumer: consumer);

            Console.WriteLine("Listening for messages...");
            Console.ReadLine();
        }
    }
}

