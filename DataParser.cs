using System;
using InfluxDB.Client.Api.Domain;
using InfluxDB.Client.Writes;

namespace Ajeevi.IoT.RabbitMqListenerStateless
{
    public class DataParser
    {
        public PointData ParseDataPacketForInflux(string field)
        {
            string[] fields = field.Split(',');
            if (fields.Length != 12 && fields.Length != 16)
            {
                Console.WriteLine("Invalid field format.");
                return null;
            }

            try
            {
                if (fields.Length == 12 && fields[0].Equals(">*") && fields[1].Equals("1005"))
                {
                    return ParseTemperatureSensorDataForInflux(fields);
                }
                else if (fields.Length == 16 && fields[0].Equals(">*") && fields[1].Equals("1005"))
                {
                    return ParseGPSv140DataForInflux(fields);
                }
                else
                {
                    Console.WriteLine("Unknown fields type or invalid length.");
                    return null;
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error parsing data: {ex}");
                return null;
            }
        }

        private PointData ParseTemperatureSensorDataForInflux(string[] fields)
        {
            try
            {
                if (fields.Length != 12)
                {
                    throw new ArgumentException("Invalid number of fields for temperature sensor data");
                }
                //EX: > *,1005,2,1,152041,07072022,101,38.87,1,28.321245,22.878765,#\r\n
                var header = fields[0];
                var deviceType = fields[1];
                var packetType = fields[2];
                var dataType = fields[3];
                var timestamp = fields[4];
                var date = fields[5];
                var sensorState = fields[6];
                var temperature = Convert.ToDouble(fields[7]);
                var gpsValidation = fields[8];
                var latitude = Convert.ToDouble(fields[9]);
                var longitude = Convert.ToDouble(fields[10]);
                var endByte = fields[11];

                // Create the point data
                return PointData.Measurement("iotdata")
                    .Tag("Header", header)
                    .Tag("Device_Type", deviceType)
                    .Tag("Packet_Type", packetType)
                    .Tag("Data_Type", dataType)
                    .Tag("timestamp", timestamp)
                    .Tag("date", date)
                    .Tag("Sensor_State", sensorState)
                    .Field("temperature", temperature)
                    .Field("GPS_Validation", gpsValidation)
                    .Field("Latitude", latitude)
                    .Field("Longitude", longitude)
                    .Field("End_Byte", endByte)
                    .Timestamp(DateTime.UtcNow, WritePrecision.Ns);
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error parsing temperature sensor data: {ex}");
                return null;
            }
        }

        private PointData ParseGPSv140DataForInflux(string[] fields)
        {
            try
            {
                if (fields.Length != 16)
                {
                    throw new ArgumentException("Invalid number of fields for GPS data.");
                }
           // EX: > *,1005,860906047220709,1,152041,07072022,101,38.87,1,28.321245,22.878765,0.214,221.5,3.7,0,#\r\n
                var header = fields[0];
                var deviceType = fields[1];
                var imie = fields[2];
                var dataType = fields[3];
                var timestamp = fields[4];
                var date = fields[5];
                var sensorState = fields[6];
                var temperature = Convert.ToDouble(fields[7]);
                var gpsValidation = fields[8];
                var latitude = Convert.ToDouble(fields[9]);
                var longitude = Convert.ToDouble(fields[10]);
                var groundSpeed = Convert.ToDouble(fields[11]);
                var gpsElevation = Convert.ToDouble(fields[12]);
                var batteryVoltage = Convert.ToDouble(fields[13]);
                var reserveByte2 = fields[14];
                var endByte = fields[15];

                // Create the point data
                return PointData.Measurement("iotdata")
                    .Tag("Header", header)
                    .Tag("Device_Type", deviceType)
                    .Tag("imie", imie)
                    .Tag("Data_Type", dataType)
                    .Tag("timestamp", timestamp)
                    .Tag("date", date)
                    .Field("Sensor_State", sensorState)
                    .Field("temperature", temperature)
                    .Field("gpsValidation", gpsValidation)
                    .Field("Latitude", latitude)
                    .Field("Longitude", longitude)
                    .Field("groundSpeed", groundSpeed)
                    .Field("gpsElevation", gpsElevation)
                    .Field("batteryVoltage", batteryVoltage)
                    .Field("reserveByte2", reserveByte2)
                    .Field("endByte", endByte)
                    .Timestamp(DateTime.UtcNow, WritePrecision.Ns);
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error parsing GPS data: {ex}");
                return null;
            }
        }
    }
}

