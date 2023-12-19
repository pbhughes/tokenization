using System;
using System.Collections.Generic;
using System.IO;
using Azure.Storage.Blobs;
using System.Threading.Tasks;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Host;
using Microsoft.Extensions.Logging;
using System.Text;
using Azure.Messaging.EventHubs.Producer;
using Azure.Messaging.EventHubs;

namespace tokenization_func
{
    public class Function1
    {
        [FunctionName("Function1")]
        public static async Task Run([BlobTrigger("blocks/{name}", Connection = "AZURESTORAGE")]Stream myBlob, string name, ILogger log)
        {
            log.LogInformation($"C# Blob trigger function Processed blob\n Name:{name} \n Size: {myBlob.Length} Bytes");

            using (var blobStreamReader = new StreamReader(myBlob))
            {
                string line = string.Empty;
               
                while ((line = blobStreamReader.ReadLine()) != null)
                {
                    line = $"{line} tokenized  tokenized {DateTime.Now.TimeOfDay.ToString()}";

                    await WriteSubFileAsync(line, log);
                }
            }
        }

        private static async Task WriteSubFileAsync(string line, ILogger log)
        {


            EventHubProducerClient producerClient = new EventHubProducerClient(
          "Endpoint=sb://customer-list.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=1lkDNH2JI74AwezhU5ukrJSc6ktH9ZV7m+AEhIUnEZU=",
          "search-requests");

            using EventDataBatch eventBatch = await producerClient.CreateBatchAsync();

            if (!eventBatch.TryAdd(new EventData(Encoding.UTF8.GetBytes(line))))
            {
                // if it is too large for the batch
                throw new Exception($"Event {line} is too large for the batch and cannot be sent.");
            }

            log.LogInformation($"staged 1 event to search requests");

            try
            {
                // Use the producer client to send the batch of events to the event hub
                await producerClient.SendAsync(eventBatch);
                log.LogInformation($"published event");

            }
            finally
            {
                await producerClient.DisposeAsync();
            }

        }
    }
}
