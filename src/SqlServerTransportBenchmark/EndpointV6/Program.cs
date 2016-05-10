using System;
using System.Threading.Tasks;
using NServiceBus;

namespace EndpointV6
{
    internal class Program
    {
        private static void Main()
        {
            MainAsync().GetAwaiter().GetResult();
        }

        private static async Task MainAsync()
        {
            var config = new EndpointConfiguration("Endpoint");
            config.UsePersistence<InMemoryPersistence>();
            config.UseTransport<SqlServerTransport>()
                .ConnectionString(
                    @"Data Source=.\SQLEXPRESS;Initial Catalog=SqlServerTransportBenchmark;Integrated Security=True;");
            config.SendFailedMessagesTo("errors");
            config.Conventions().DefiningMessagesAs(t => t.Namespace != null && t.Name.EndsWith("Message"));
            config.LimitMessageProcessingConcurrencyTo(16);
            config.EnableInstallers();

            MyMessageHandler.Stopwatch.Start();
            var endpoint = await Endpoint.Start(config);

            try
            {
                Console.WriteLine("Endpoint started.");
                Console.ReadLine();
            }
            finally
            {
                await endpoint.Stop();
            }
        }
    }
}