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
        [FunctionName("Tokenization")]
        public static async Task Run([BlobTrigger("blocks/{name}", Connection = "AZURESTORAGE")]Stream myBlob, string name, ILogger log)
        {
            log.LogInformation($"C# Blob trigger function Processed blob\n Name:{name} \n Size: {myBlob.Length} Bytes");
            var lines = new List<string>();
            using (var blobStreamReader = new StreamReader(myBlob))
            {
                string line = string.Empty;
               
                while ((line = blobStreamReader.ReadLine()) != null)
                {
                    line = $"{line} tokenized  tokenized {DateTime.Now.TimeOfDay.ToString()}";
                    lines.Add(line);
                    if (lines.Count == 100)
                    {
                        await WriteSubFileAsync(lines, log);
                        lines.Clear();
                    }
                        
                   
                }
            }
        }

        private static async Task WriteSubFileAsync(List<string> lines, ILogger log)
        {


            EventHubProducerClient producerClient = new EventHubProducerClient(
          "Endpoint=sb://customer-list.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=1lkDNH2JI74AwezhU5ukrJSc6ktH9ZV7m+AEhIUnEZU=",
          "search-requests");

            using EventDataBatch eventBatch = await producerClient.CreateBatchAsync();

            for(int i = 0;i < lines.Count; i++)
            {
                if (!eventBatch.TryAdd(new EventData(Encoding.UTF8.GetBytes(lines[i]))))
                {
                    // if it is too large for the batch
                    throw new Exception($"Event {lines[i]} is too large for the batch and cannot be sent.");
                }
            }
            

            log.LogInformation($"staged 1 event to search requests");

            try
            {
                // Use the producer client to send the batch of events to the event hub
                await producerClient.SendAsync(eventBatch);
                log.LogInformation($"published {lines.Count} events");

            }
            finally
            {
                await producerClient.DisposeAsync();
            }

        }
    }
}
