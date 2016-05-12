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
            string connectionString = @"Data Source =.\SQLEXPRESS; Integrated Security = True; Database = PerformanceTests; Min Pool Size = 100; Max Pool Size = 1000;";
            int numberOfMessages = 5000;
            int messageSize = 1024;
            int concurrency = 1;

            for (int i = 0; i < 11; i++)
            {
                int originalMessageSize = messageSize;
                //for (int j = 0; j < 9; j++)
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

                //ParallelFor(numberOfMessages, concurrency, bus, destination);

                WaitAllSemaphore(numberOfMessages, concurrency, bus, destination);

                stopWatch.Stop();

                bus.Stop().GetAwaiter().GetResult();

                var avg = (double)numberOfMessages / stopWatch.ElapsedMilliseconds * 1000;
                Console.WriteLine($"TX: Concurrency {concurrency,6:N0}, Time {stopWatch.Elapsed.TotalSeconds,4:N1} s, Throughput: {avg,8:N1} msg/s");
            }
        }

        private static void WaitAllSemaphore(int numberOfMessages, int concurrency, IEndpointInstance bus, string destination)
        {
            var semaphoreSlim = new SemaphoreSlim(concurrency);
            var sendOperations = Enumerable.Range(0, numberOfMessages).Select(async i =>
            {
                try
                {
                    await semaphoreSlim.WaitAsync().ConfigureAwait(false);
                    await bus.Send(destination, new ProduceChocolateBar(true) /*{ LotNumber = i, MaxLotNumber = numberOfMessages }*/).ConfigureAwait(false);
                }
                finally
                {
                    semaphoreSlim.Release();
                }
            });

            Task.WaitAll(sendOperations.ToArray());
            semaphoreSlim.Dispose();
        }

        private static void ParallelFor(int numberOfMessages, int concurrency, IEndpointInstance bus, string destination)
        {
            Parallel.For(0, numberOfMessages,
                new ParallelOptions { MaxDegreeOfParallelism = concurrency },
                i =>
                {
                    bus.Send(destination, new ProduceChocolateBar(true) /*{ LotNumber = i, MaxLotNumber = numberOfMessages }*/)
                        .ConfigureAwait(false)
                        .GetAwaiter().GetResult();
                });
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

            var avg = (double)numberOfMessages / stopWatch.ElapsedMilliseconds * 1000;
            Console.WriteLine($"RX: Concurrency {concurrency,6:N0}, Time {stopWatch.Elapsed.TotalSeconds,4:N1} s, Throughput: {avg,8:N1} msg/s");
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
