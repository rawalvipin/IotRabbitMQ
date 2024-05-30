using System;
using System.Text;

namespace Ajeevi.IoT.RabbitMqListenerStateless
{
    public class DataParser
    {
        public string ParseDataPacket(string packet)
        {
            string returnMessage = string.Empty;
            string[] packets = packet.Split(",");
            if (packets[0].Equals("*@") && packets[1].Equals("1001"))
            {
                returnMessage = ParsetemperaturesensorData(packet);
            }
            else if (packets[0].Equals(">*") && packets[1].Equals("1005"))
            {
                returnMessage = ParseGPSv140Data(packet);
            }
            return returnMessage;
        }

        private string ParsetemperaturesensorData(string packet)
        {
            string parsedString = string.Empty;
            StringBuilder sbResponse = new StringBuilder();
            string[] packets;
            try
            {
                packets = packet.Split(",");
                sbResponse.Append("header:" + packets[0] + ",");
                sbResponse.Append("devicetype:" + packets[1] + ",");
                sbResponse.Append("imie:" + packets[2] + ",");
                sbResponse.Append("datatype:" + packets[3] + ",");
                sbResponse.Append("datasource:" + packets[4] + ",");
                sbResponse.Append("packetserialnumber:" + packets[5] + ",");
                sbResponse.Append("time:" + packets[6] + ",");
                sbResponse.Append("date:" + packets[7] + ",");
                sbResponse.Append("batteryvoltage:" + packets[8] + ",");
                sbResponse.Append("signalstrength:" + packets[9] + ",");
                sbResponse.Append("chargingindication:" + packets[10] + ",");
                sbResponse.Append("temperatorealert:" + packets[11] + ",");
                sbResponse.Append("temperature:" + packets[12] + ",");
                sbResponse.Append("humidity:" + packets[13] + ",");
                sbResponse.Append("endbyte:" + packets[14] + ",");
                parsedString = sbResponse.ToString();
            }
            catch (Exception ex)
            {
                Console.WriteLine("Error Parsing {0} ", ex.ToString());
                parsedString = "Invalid Data";
            }

            return parsedString;
        }

        private string ParseGPSv140Data(string packet)
        {
            string parsedString = string.Empty;
            StringBuilder sbResponse = new StringBuilder();
            string[] packets;
            try
            {
                packets = packet.Split(",");
                sbResponse.Append("header:" + packets[0] + ",");
                sbResponse.Append("devicetype:" + packets[1] + ",");
                sbResponse.Append("imie:" + packets[2] + ",");
                sbResponse.Append("datatype:" + packets[3] + ",");
                sbResponse.Append("time:" + packets[4] + ",");
                sbResponse.Append("date:" + packets[5] + ",");
                sbResponse.Append("sensorstate:" + packets[6] + ",");
                sbResponse.Append("temperature:" + packets[7] + ",");
                sbResponse.Append("gpsvalidation:" + packets[8] + ",");
                sbResponse.Append("latitude:" + packets[9] + ",");
                sbResponse.Append("longitude:" + packets[10] + ",");
                sbResponse.Append("groundspeed:" + packets[11] + ",");
                sbResponse.Append("gpselevation:" + packets[12] + ",");
                sbResponse.Append("batteryvoltage:" + packets[13] + ",");
                sbResponse.Append("reservebyte2:" + packets[14] + ",");
                sbResponse.Append("endbyte:" + packets[15] + ",");
                parsedString = sbResponse.ToString();
            }
            catch (Exception ex)
            {
                Console.WriteLine("Error Parsing {0} ", ex.ToString());
                parsedString = "Invalid Data";
            }

            return parsedString;
        }

        public string ParseDataPacketForSQL(string packet)
        {
            string returnMessage = string.Empty;
            string[] packets = packet.Split(",");
            if (packets[0].Equals("*@") && packets[1].Equals("1001"))
            {
                returnMessage = ParsetemperaturesensorDataSQL(packet);
            }
            else if (packets[0].Equals(">*") && packets[1].Equals("1005"))
            {
                returnMessage = ParseGPSv140DataSQL(packet);
            }
            return returnMessage;
        }

        private string ParsetemperaturesensorDataSQL(string packet)
        {
            StringBuilder sbResponse = new StringBuilder();
            string[] packets;
            try
            {
                packets = packet.Split(",");
                sbResponse.Append("insert into `ajeeviiot`.`iotdata`(`temperature`,`imie`,`devicetype`,");
                sbResponse.Append("`ischarging`,`batteryvoltage`,`humidity`,`signalstrength`,`lastreportingtime`,");
                sbResponse.Append("`receiveddatapacket`,`header`,`datatype`,`datapacketsrnum`,`datasource`,`time`,`date`,`temperaturealert`)");
                sbResponse.Append("VALUES(");
                sbResponse.Append("'"); sbResponse.Append(packets[12]); sbResponse.Append("'"); sbResponse.Append(",");//temperature
                sbResponse.Append("'"); sbResponse.Append(packets[2]); sbResponse.Append("'"); sbResponse.Append(",");//imie
                sbResponse.Append("'"); sbResponse.Append(packets[1]); sbResponse.Append("'"); sbResponse.Append(",");//devicetype
                sbResponse.Append("'"); sbResponse.Append(packets[10]); sbResponse.Append("'"); sbResponse.Append(",");//chargingindication
                sbResponse.Append("'"); sbResponse.Append(packets[8]); sbResponse.Append("'"); sbResponse.Append(",");//battery voltage
                sbResponse.Append("'"); sbResponse.Append(packets[13]); sbResponse.Append("'"); sbResponse.Append(",");//humidity
                sbResponse.Append("'"); sbResponse.Append(packets[9]); sbResponse.Append("'"); sbResponse.Append(",");//signalstrength
                sbResponse.Append("'"); sbResponse.Append(DateTime.Now.ToString("yyyy-MM-dd H:mm:ss")); sbResponse.Append("'"); sbResponse.Append(",");//lastreportingtime
                sbResponse.Append("'"); sbResponse.Append(packet); sbResponse.Append("'"); sbResponse.Append(",");//receiveddatapacket
                sbResponse.Append("'"); sbResponse.Append(packets[0]); sbResponse.Append("'"); sbResponse.Append(",");//header
                sbResponse.Append("'"); sbResponse.Append(packets[3]); sbResponse.Append("'"); sbResponse.Append(",");//datatype
                sbResponse.Append("'"); sbResponse.Append(packets[5]); sbResponse.Append("'"); sbResponse.Append(",");//datapacketsrnum
                sbResponse.Append("'"); sbResponse.Append(packets[4]); sbResponse.Append("'"); sbResponse.Append(",");//datasource
                sbResponse.Append("'"); sbResponse.Append(packets[6]); sbResponse.Append("'"); sbResponse.Append(",");//time
                sbResponse.Append("'"); sbResponse.Append(packets[7]); sbResponse.Append("'"); sbResponse.Append(",");//date
                sbResponse.Append("'"); sbResponse.Append(packets[11]); sbResponse.Append("'");//temperaturealert
                sbResponse.Append(")");
                return sbResponse.ToString();
            }
            catch (Exception ex)
            {
                Console.WriteLine("Error Parsing {0} ", ex.ToString());
                return "Invalid Data";
            }
        }

        private string ParseGPSv140DataSQL(string packet)
        {
            StringBuilder sbResponse = new StringBuilder();
            string[] packets;
            try
            {
                packets = packet.Split(",");
                sbResponse.Append("header:" + packets[0] + ",");
                sbResponse.Append("devicetype:" + packets[1] + ",");
                sbResponse.Append("imie:" + packets[2] + ",");
                sbResponse.Append("datatype:" + packets[3] + ",");
                sbResponse.Append("time:" + packets[4] + ",");
                sbResponse.Append("date:" + packets[5] + ",");
                sbResponse.Append("sensorstate:" + packets[6] + ",");
                sbResponse.Append("temperature:" + packets[7] + ",");
                sbResponse.Append("gpsvalidation:" + packets[8] + ",");
                sbResponse.Append("latitude:" + packets[9] + ",");
                sbResponse.Append("longitude:" + packets[10] + ",");
                sbResponse.Append("groundspeed:" + packets[11] + ",");
                sbResponse.Append("gpselevation:" + packets[12] + ",");
                sbResponse.Append("batteryvoltage:" + packets[13] + ",");
                sbResponse.Append("reservebyte2:" + packets[14] + ",");
                sbResponse.Append("endbyte:" + packets[15] + ",");
                return sbResponse.ToString();
            }
            catch (Exception ex)
            {
                Console.WriteLine("Error Parsing {0} ", ex.ToString());
                return "Invalid Data";
            }
        }
    }
}
