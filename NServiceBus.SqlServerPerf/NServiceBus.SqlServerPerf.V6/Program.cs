using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Messages;
using NServiceBus.Logging;

namespace NServiceBus.SqlServerPerf.V6
{
    class Program
    {
        static void Main(string[] args)
        {
            string connectionString = @"FILL IN HERE"; ;
            int numberOfMessages = 10000;
            int messageSize = 1024;
            int concurrency = 1;

            for (int i = 0; i < 9; i++)
            {
                int originalMessageSize = messageSize;
                for (int j = 0; j < 9; j++)
                {
                    SingleSendRun(connectionString, messageSize, numberOfMessages, concurrency);

                    SingleReceiveRun(connectionString, messageSize, numberOfMessages, concurrency);

                    messageSize *= 4;
                }

                messageSize = originalMessageSize;
                concurrency *= 2;
            }

            Console.WriteLine("done.");
            Console.ReadLine();
        }

        private static void SingleSendRun(string connectionString, int messageSize, int numberOfMessages, int concurrency)
        {

            DefaultFactory defaultFactory = LogManager.Use<DefaultFactory>();
            defaultFactory.Level(LogLevel.Error);

            var configuration = new EndpointConfiguration("NServiceBus.SqlServerPerf.Source");
            configuration.EnableInstallers();
            configuration.UseSerialization<JsonSerializer>();
            configuration.UseTransport<SqlServerTransport>().ConnectionString(connectionString);

            configuration.SendOnly();
            configuration.UsePersistence<InMemoryPersistence>();

            var bus = Endpoint.Start(configuration).GetAwaiter().GetResult();

            var destination = "NServiceBus.SqlServerPerf.Destination";
            QueueHelper.CreateQueue(connectionString, destination);

            using (ProduceChocolateBar.Initialize(messageSize))
            {
                var stopWatch = Stopwatch.StartNew();

                var semaphoreSlim = new SemaphoreSlim(concurrency);
                var sendOperations = Enumerable.Range(0, numberOfMessages).Select(async i =>
                {
                    try
                    {
                        await semaphoreSlim.WaitAsync().ConfigureAwait(false);
                        await bus.Send(destination, new ProduceChocolateBar(true) { LotNumber = i, MaxLotNumber = numberOfMessages }).ConfigureAwait(false);
                    }
                    finally
                    {
                        semaphoreSlim.Release();
                    }
                });

                Task.WaitAll(sendOperations.ToArray());

                stopWatch.Stop();

                bus.Stop().GetAwaiter().GetResult();
                semaphoreSlim.Dispose();

                Console.WriteLine($"Send: NumberOfMessages {numberOfMessages}, MessageSize {messageSize}, Concurrency { concurrency}, TimeInMs { stopWatch.ElapsedMilliseconds }");
            }
        }

        private static void SingleReceiveRun(string connectionString, int messageSize, int numberOfMessages, int concurrency)
        {

            DefaultFactory defaultFactory = LogManager.Use<DefaultFactory>();
            defaultFactory.Level(LogLevel.Error);

            var configuration = new EndpointConfiguration("NServiceBus.SqlServerPerf.Destination");
            configuration.SendFailedMessagesTo("error");
            configuration.EnableInstallers();
            configuration.UseSerialization<JsonSerializer>();
            configuration.UseTransport<SqlServerTransport>().ConnectionString(connectionString);

            configuration.UsePersistence<InMemoryPersistence>();

            configuration.LimitMessageProcessingConcurrencyTo(concurrency);
            Syncher.SyncEvent = new CountdownEvent(numberOfMessages);

            var stopWatch = Stopwatch.StartNew();

            var bus = Endpoint.Start(configuration).GetAwaiter().GetResult();

            Syncher.SyncEvent.Wait();

            stopWatch.Stop();

            bus.Stop().GetAwaiter().GetResult();
            Syncher.SyncEvent.Dispose();

            Console.WriteLine($"Receive: NumberOfMessages {numberOfMessages}, MessageSize {messageSize}, Concurrency { concurrency}, TimeInMs { stopWatch.ElapsedMilliseconds }");
        }
    }

    public class Handler : IHandleMessages<ProduceChocolateBar>
    {
        public async Task Handle(ProduceChocolateBar message, IMessageHandlerContext context)
        {
            Syncher.SyncEvent.Signal();
        }
    }
}
