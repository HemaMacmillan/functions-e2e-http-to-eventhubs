using Azure.Messaging.EventHubs;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;
using System;
using System.Text;
using System.Text.Json;
using Microsoft.Azure.Cosmos;
using System.Threading.Tasks;

namespace MyFunctions
{
    public class EventHubConsumer
    {
        private readonly ILogger _logger;

        public EventHubConsumer(ILoggerFactory loggerFactory)
        {
            _logger = loggerFactory.CreateLogger<EventHubConsumer>();
        }

        [Function("EventHubConsumer")]
        public async Task Run(
            [EventHubTrigger("%EventHubName%",
                             Connection = "EventHubConnection")]
            EventData[] events)
        {
            // Create Cosmos client once per batch
            var cosmosClient = new CosmosClient(
                Environment.GetEnvironmentVariable("CosmosDBConnection"));

            var container = cosmosClient.GetContainer("ordersdb", "orders");

            foreach (var evt in events)
            {
                string body = evt.EventBody.ToString();
                _logger.LogInformation($"Received event: {body}");

                try
                {
                    // Deserialize incoming JSON into Order object
                    var order = JsonSerializer.Deserialize<Order>(body);

                    // Build Cosmos DB document
                    var doc = new
                    {
                        id = Guid.NewGuid().ToString(),
                        orderId = order.orderId,
                        customerId = order.customerId,
                        amount = order.amount,
                        timestamp = DateTime.UtcNow
                    };

                    // Write to Cosmos DB (sync wrapper because Run is void)
                     await container.CreateItemAsync(doc, new PartitionKey(order.orderId));
                    
                    _logger.LogInformation("Written to Cosmos DB");
                }
                catch (Exception ex)
                {
                    _logger.LogError($"Cosmos write failed: {ex.Message}");
                }
            }
        }

        public class Order
        {
            public string orderId { get; set; }
            public string customerId { get; set; }
            public decimal amount { get; set; }
        }
    }
}
