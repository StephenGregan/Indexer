using System;
using System.Net;
using System.Threading.Tasks;
using Microsoft.Azure.Search;
using Microsoft.Azure.Search.Models;
using Microsoft.Extensions.Configuration;
using Microsoft.Rest.Azure;

namespace BlobIndexer
{
    public sealed class Program
    {
        public static async void Main(string[] args)
        {
            IConfigurationBuilder builder = new ConfigurationBuilder().AddJsonFile("appsettings.json");
            IConfigurationRoot configuration = builder.Build();

            if (configuration["SearchServiceName"] == "Put your search service name here")
            {
                Console.Error.WriteLine("Specify SearchServiceAdminApiKey in appsettings.json");
                Environment.Exit(-1);
            }
            if (configuration["SearchServiceAdminApiKey"] == "Put your primary or secondary key here")
            {
                Console.WriteLine("Specify SearchServiceAdminApiKey in appsettings.json");
                Environment.Exit(-1);
            }
            if (configuration["AzureBlobStorageConnectionString"] == "Put your Azure Blob Storage connection string here")
            {
                Console.WriteLine("Specify AzureBlobStorageConnectionString in appsettings.json");
                Environment.Exit(-1);
            }
            SearchServiceClient searchClient = new SearchServiceClient(
                searchServiceName: configuration["SearchServiceName"],
                credentials: new SearchCredentials(configuration["SearchServiceAdimApiKey"]));

            Console.WriteLine("Creating index...");
            Index index = new Index(
                name: "index",
                fields: FieldBuilder.BuildForType<Contacts>());

            bool exists = await searchClient.Indexes.ExistsAsync(index.Name);
            if (exists)
            {
                await searchClient.Indexes.DeleteAsync(index.Name);
            }
            await searchClient.Indexes.CreateAsync(index);

            Console.WriteLine("Creating data source...");

            DataSource dataSource = DataSource.AzureBlobStorage(
                name: "datasource",
                storageConnectionString: configuration["AzureBlobStorageConnectionString"],
                containerName: "contacts",
                deletionDetectionPolicy: new SoftDeleteColumnDeletionDetectionPolicy(
                    softDeleteColumnName: "isDeleted",
                    softDeleteMarkerValue: "true"));
            //dataSource.DataChangeDetectionPolicy = new SqlIntegratedChangeTrackingPolicy();
            await searchClient.DataSources.CreateOrUpdateAsync(dataSource);

            Console.WriteLine("Creating Azure Blob Storage indexer...");
            Indexer indexer = new Indexer(
                name: "indexer",
                dataSourceName: dataSource.Name,
                targetIndexName: index.Name,
                schedule: new IndexingSchedule(TimeSpan.FromDays(1)));

            exists = await searchClient.Indexers.ExistsAsync(indexer.Name);

            if (exists)
            {
                await searchClient.Indexers.ResetAsync(indexer.Name);
            }
            await searchClient.Indexers.CreateOrUpdateAsync(indexer);

            Console.WriteLine("Running Azure Blob Storage indexer...");

            try
            {
                await searchClient.Indexers.RunAsync(indexer.Name);
            }
            catch (CloudException e) when (e.Response.StatusCode == (HttpStatusCode)429)
            {
                Console.WriteLine("Failed to run indexer: {0}", e.Response.Content);
            }
            Console.WriteLine("Press any key to continue...");
            Console.ReadKey();
            Environment.Exit(0);
        }
    }
}
