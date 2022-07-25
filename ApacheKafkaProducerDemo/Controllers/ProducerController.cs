using Confluent.Kafka;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using System.Diagnostics;
using System.Text.Json;

namespace ApacheKafkaProducerDemo.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class ProducerController : ControllerBase
    {
        private readonly string bootstrapServers = "subtle-oyster-5370-us1-kafka.upstash.io:9092";
        private readonly string topic = "test";

        [HttpPost]
        public async Task<IActionResult> Post([FromBody] OrderRequest orderRequest)
        {
            string message = JsonSerializer.Serialize(orderRequest);
            return Ok(await SendOrderRequest(topic, message));
        }

        private async Task<bool> SendOrderRequest  (string topic, string message)
        {
            //ProducerConfig config = new ProducerConfig
            //{
            //    BootstrapServers = bootstrapServers,
            //    ClientId = "Client ID"//Dns.GetHostName()
            //};

            var config = new ClientConfig
            {
                BootstrapServers = "subtle-oyster-5370-us1-kafka.upstash.io:9092",
                SaslMechanism = SaslMechanism.ScramSha256,
                SaslUsername = "c3VidGxlLW95c3Rlci01MzcwJOJSZO5AJaoowjHoDFZ2dLnPsmYdxdpL0tKB-H8",
                SaslPassword = "S-tTWUfAAQfC71ngk5UQy6hDj6wmrdiAqZDgoWb3IZR1s-8IaRTS6LgxS_xEDZ3CR4gcrw==",
                SecurityProtocol = SecurityProtocol.SaslSsl,
            };

            try
            {
                using (var producer = new ProducerBuilder <Null, string>(config).Build())
                {
                    var result = await producer.ProduceAsync
                    (topic, new Message<Null, string>
                    {
                        Value = message
                    });

                    Debug.WriteLine($"Delivery Timestamp: { result.Timestamp.UtcDateTime} ");
                    return await Task.FromResult(true);
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error occured: {ex.Message}");
            }

            return await Task.FromResult(false);
        }
    }
}
