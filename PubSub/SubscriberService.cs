using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.Extensions.Logging;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;
using Newtonsoft.Json;
using PubSub.DTO;
using PubSub.StorageTableEntity;
using System;
using System.Threading.Tasks;

namespace PubSub
{
    public class SubscriberService
    {
        public SubscriberService()
        {

        }

        [FunctionName(nameof(Subscribe))]
        public async Task<IActionResult> Subscribe(
            [HttpTrigger(AuthorizationLevel.Anonymous, "POST", Route = "subscribe/{queue}")]HttpRequest req
            , string queue
            , ILogger logger
            )
        {
            try
            {
                var user = JsonConvert.DeserializeObject<Subscriber>(await req.ReadAsStringAsync());
                if (user == null || string.IsNullOrWhiteSpace(user.Username))
                    return new BadRequestResult();

                await AddSubscriberAsync(queue, user.Username);
                return new OkResult();
            }
            catch(JsonReaderException e)
            {
                logger.LogError(e.ToString());
                return new BadRequestObjectResult(e.Message);
            }
            catch (Exception e)
            {
                logger.LogError(e.ToString());
                return new ObjectResult(new { error = e.ToString() });
            }
        }

        [FunctionName(nameof(NotifySubscribers))]
        public async Task NotifySubscribers(
            [QueueTrigger("newproduct")]Product input,
            ILogger logger
            )
        {
            try
            {
                var table = await GetCloudTableReferenceAsync("QueueMessageSubscribers");
                var query = new TableQuery<QueueSubscriptionMessage>().Where(
                        TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, "newProduct")
                    );
                var subscribers = await table.ExecuteQuerySegmentedAsync(query, new TableContinuationToken());

                //TODO: implement notification service.
                subscribers.Results.ForEach(x => logger.LogInformation($"Sending notification to {x.RowKey}"));//Row key stores the username.

            }
            catch (Exception e)
            {
                logger.LogError(e.ToString());
            }
        }

        

        private async Task AddSubscriberAsync(string queue, string username)
        {
            var subscriptionMessage = new QueueSubscriptionMessage()
            {
                // Because a queue can have multiple subscribers.
                PartitionKey = queue,

                // Because every subcriber is unique.
                RowKey = username,
            };

            var table = await GetCloudTableReferenceAsync("QueueMessageSubscribers");

            TableOperation insert = TableOperation.Insert(subscriptionMessage);
            var insertionResult = await table.ExecuteAsync(insert);
        }
        
          
        private async Task<CloudTable> GetCloudTableReferenceAsync(string tableName, string connectionString = "")
        {
            CloudStorageAccount storageAccount = 
                string.IsNullOrWhiteSpace(connectionString) 
                ? CloudStorageAccount.DevelopmentStorageAccount : CloudStorageAccount.Parse(connectionString);

            CloudTableClient tableClient = storageAccount.CreateCloudTableClient();

            CloudTable table = tableClient.GetTableReference(tableName);
            await table.CreateIfNotExistsAsync();
            return table;
        }

    }
}
