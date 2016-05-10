using Messages;
using NServiceBus;

namespace Sender
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
            config.Conventions().DefiningMessagesAs(t => t.Namespace != null && t.Name.EndsWith("Message"));
            config.EndpointName("Endpoint");

            using (var bus = Bus.CreateSendOnly(config))
            {
                const int n = 10000;
                const int size = 1;

                for (var i = 0; i < n; i++)
                {
                    var msg = new MyMessage {Blob = new int[size]};

                    bus.Send("Endpoint", msg);
                }
            }
        }
    }
}