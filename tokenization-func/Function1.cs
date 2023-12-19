using System;
using System.Collections.Generic;
using System.IO;
using Azure.Storage.Blobs;
using System.Threading.Tasks;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Host;
using Microsoft.Extensions.Logging;

namespace tokenization_func
{
    public class Function1
    {
        [FunctionName("Function1")]
        public static async Task Run([BlobTrigger("blocks/{name}", Connection = "AZURE-STORAGE")]Stream myBlob, string name, ILogger log)
        {
            log.LogInformation($"C# Blob trigger function Processed blob\n Name:{name} \n Size: {myBlob.Length} Bytes");

            using (var blobStreamReader = new StreamReader(myBlob))
            {
                int recordSize = 10;
                string line = string.Empty;
                List<string> lines = new List<string>(10);
                while ((line = blobStreamReader.ReadLine()) != null)
                {
                    line = $"{line} tokenized  tokenized {DateTime.Now.TimeOfDay.ToString()}";
                    lines.Add(line);
                    if (lines.Count == recordSize)
                    {
                        await WriteSubFileAsync(lines, log, name);
                        lines.Clear();
                    }
                }
            }
        }

        private static async Task WriteSubFileAsync(List<string> lines, ILogger log, string blobName)
        {
            // TODO: Replace <storage-account-name> with your actual storage account name
            var bc = new BlobServiceClient("DefaultEndpointsProtocol=https;AccountName=fileprocessing32;AccountKey=/cW0JvbEXxSldiwPsmmXwf3sCpIteLct83ohZITICu11na5EfHz0/vrzE5yLz17DQr37z1IAKtFM+AStSPNy/g==;EndpointSuffix=core.windows.net");

            //Create a unique name for the container
            string containerName = "blocks";

            // Create the container and return a container client object
            BlobContainerClient cc = bc.GetBlobContainerClient(containerName);
            foreach (string line in lines)
            {
                log.LogInformation($"{line}");
            }

           
            string blobContent = string.Join(Environment.NewLine, lines.ToArray());
            var blobClient = cc.GetBlobClient(blobName);

            using (var stream = new MemoryStream(System.Text.Encoding.UTF8.GetBytes(blobContent)))
            {
                await blobClient.UploadAsync(stream, true);
            }

            Console.WriteLine($"String array uploaded to blob: {blobName} in container: {containerName}");

        }
    }
}
