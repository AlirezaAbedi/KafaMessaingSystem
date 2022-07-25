using Confluent.Kafka;
using System.Diagnostics;
using System.Text.Json;

namespace ApacheKafkaConsumerDemo
{
    public class ApacheKafkaConsumerService : IHostedService
    {
        private readonly string topic = "test";
        private readonly string groupId = "$GROUP_NAME";
        private readonly string bootstrapServers = "subtle-oyster-5370-us1-kafka.upstash.io:9092";

        public Task StartAsync(CancellationToken cancellationToken)
        {
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
                using (var consumerBuilder = new ConsumerBuilder<Ignore, string>(config).Build())
                {
                    consumerBuilder.Subscribe(topic);
                    var cancelToken = new CancellationTokenSource();

                    try
                    {
                        while (true)
                        {
                            var consumer = consumerBuilder.Consume(cancelToken.Token);
                            var orderRequest = JsonSerializer.Deserialize<OrderProcessingRequest>(consumer.Message.Value);
                            Debug.WriteLine($"Processing Order Id: { orderRequest.OrderId} ");
                        }
                    }
                    catch (OperationCanceledException)
                    {
                        consumerBuilder.Close();
                    }
                }
            }
            catch (Exception ex)
            {
                System.Diagnostics.Debug.WriteLine(ex.Message);
            }

            return Task.CompletedTask;
        }

        public Task StopAsync(CancellationToken cancellationToken)
        {
            return Task.CompletedTask;
        }
    }
}
