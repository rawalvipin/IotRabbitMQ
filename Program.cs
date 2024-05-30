using System.Text;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using InfluxDB.Client;
using InfluxDB.Client.Api.Domain;
using InfluxDB.Client.Writes;

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

                    var point = CreatePointFromMessage(message);
                    if (point != null)
                    {
                        // Create a new HttpClient instance with timeout
                        using (var httpClient = new HttpClient { Timeout = TimeSpan.FromSeconds(30) })
                        {
                            // Create InfluxDBClientOptions with custom HttpClient
                            var options = new InfluxDBClientOptions.Builder()
                                .Url(influxUrl)
                                .AuthenticateToken(token.ToCharArray())
                               // .HttpClient(httpClient) // Set the custom HttpClient
                                .Build();

                            // Create InfluxDB client with the configured options
                            using (var influxDBClient = InfluxDBClientFactory.Create(options))
                            {
                                var writeApiAsync = influxDBClient.GetWriteApiAsync();

                                await writeApiAsync.WritePointAsync(point, bucket, org);
                                Console.WriteLine("Data sent to InfluxDB.");
                            }
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

    private static PointData CreatePointFromMessage(string message)
    {
        try
        {
            // Remove any leading or trailing whitespace and newline characters
            message = message.Trim();

            // Check if the message starts with "*@" and ends with "##"
            if (!message.StartsWith(">*") || !message.EndsWith("#"))
            {
                Console.WriteLine("Error: Invalid message format.");
                return null;
            }

            // Split the message by commas
            var fields = message.Split(',');

            // Ensure that we have the expected number of fields
            if (fields.Length != 12)
            {
                Console.WriteLine("Error: One or more required fields are missing.");
                return null;
            }

            // Extract values from the fields array
            var Header = fields[0];
            var Device_Type = fields[1];
            var Packet_Type = fields[2];
            var Data_Type = fields[3];
            var timestamp = fields[4];
            var date = fields[5];
            var Sesnor_State = fields[6];
            var temperature = Convert.ToDouble(fields[7]);
            var GPS_Validation = fields[8];
            var Latitude = Convert.ToDouble(fields[9]);
            var Longitude = Convert.ToDouble(fields[10]);
            var End_Byte =fields[11];


            //double temperatureValue = ConvertToDouble(temperature, "temperature");
            //double GPSValidationValue = ConvertToDouble(GPSValidation, "GPSValidation");
            //double LatitudeValue = ConvertToDouble(Latitude, "Latitude");
            //double LongitudeValue = ConvertToDouble(Longitude, "Longitude");
            //double GroundSpeedValue = ConvertToDouble(GroundSpeed, "GroundSpeed");
            //double GPS_ElevationValue = ConvertToDouble(GPS_Elevation, "GPS_Elevation");
            //double BatteryVoltageValue = ConvertToDouble(BatteryVoltage, "BatteryVoltage");

            // Create the point data
            return PointData.Measurement("iotdata")
                .Tag("Header", Header)
                .Tag("Device_Type", Device_Type)
                .Tag("Packet_Type", Packet_Type)
                .Tag("Data_Type", Data_Type)
                .Tag("timestamp", timestamp)
                .Tag("date", date)
                .Tag("Sesnor_State", Sesnor_State)
                .Field("temperature", temperature)
                .Field("GPS_Validation", GPS_Validation)
                .Field("Latitude", Latitude)
                .Field("Longitude", Longitude)
                .Field("End_Byte", End_Byte)
                .Timestamp(DateTime.UtcNow, WritePrecision.Ns);
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Error processing message: {ex.Message}");
            return null;
        }
    }

    //private static double ConvertToDouble(string value, string fieldName)
    //{
    //    try
    //    {
    //        return double.Parse(value);
    //    }
    //    catch (FormatException)
    //    {
    //        Console.WriteLine($"Error: {fieldName} field is not in the correct format: {value}");
    //        throw;
    //    }
    //}
}
