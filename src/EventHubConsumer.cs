using Azure.Messaging.EventHubs;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;

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
        public void Run(
            [EventHubTrigger("%EventHubName%",
                             Connection = "EventHubConnection")] 
            EventData[] events)
        {
            foreach (var evt in events)
            {
                string body = evt.EventBody.ToString();
                _logger.LogInformation($"Received event: {body}");
            }
        }
    }
}
