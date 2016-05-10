using System;
using NServiceBus;

namespace EndpointV5
{
    internal class Program
    {
        private static void Main(string[] args)
        {
            var config = new BusConfiguration();
            config.UsePersistence<InMemoryPersistence>();
            config.UseTransport<SqlServerTransport>()
                .ConnectionString(
                    @"Data Source=.\SQLEXPRESS;Initial Catalog=SqlServerTransportBenchmark;Integrated Security=True;");
            config.EndpointName("Endpoint");
            config.Conventions().DefiningMessagesAs(t => t.Namespace != null && t.Name.EndsWith("Message"));
            config.EnableInstallers();

            MyMessageHandler.Stopwatch.Start();
            using (var bus = Bus.Create(config).Start())
            {
                Console.WriteLine("Bus started.");
                Console.ReadLine();
            }
        }
    }
}