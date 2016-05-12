﻿using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Messages;
using NServiceBus.Config;
using NServiceBus.Config.ConfigurationSource;
using NServiceBus.Logging;

namespace NServiceBus.SqlServerPerf
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

                    //  messageSize *= 4;
                }

                messageSize = originalMessageSize;
                concurrency *= 2;
            }

            Console.WriteLine("done.");
            Console.ReadLine();
        }

        private static void SingleSendRun(string connectionString, int messageSize, int numberOfMessages,
            int concurrencyForSends)
        {

            DefaultFactory defaultFactory = LogManager.Use<DefaultFactory>();
            defaultFactory.Level(LogLevel.Error);

            var configuration = new BusConfiguration();
            configuration.EndpointName("NServiceBus.SqlServerPerf.Source");
            configuration.EnableInstallers();
            configuration.UseSerialization<JsonSerializer>();
            configuration.UseTransport<SqlServerTransport>().ConnectionString(connectionString);

            configuration.UsePersistence<InMemoryPersistence>();

            var bus = Bus.CreateSendOnly(configuration);

            var destination = "NServiceBus.SqlServerPerf.Destination";
            QueueHelper.CreateQueue(connectionString, destination);

            using (ProduceChocolateBar.Initialize(messageSize))
            {
                var stopWatch = Stopwatch.StartNew();

                Parallel.For(0, numberOfMessages, new ParallelOptions { MaxDegreeOfParallelism = concurrencyForSends },
                    i =>
                    {
                        bus.Send(destination,
                            new ProduceChocolateBar(true) /*{ LotNumber = i, MaxLotNumber = numberOfMessages }*/);
                    });

                stopWatch.Stop();

                bus.Dispose();

                var avg = (double)numberOfMessages / stopWatch.ElapsedMilliseconds * 1000;
                Console.WriteLine($"TX: Concurrency {concurrencyForSends,6:N0}, Time {stopWatch.Elapsed.TotalSeconds,4:N1} s, Throughput: {avg,8:N1} msg/s");
            }
        }

        private static void SingleReceiveRun(string connectionString, int messageSize, int numberOfMessages, int concurrencyForReceives)
        {

            DefaultFactory defaultFactory = LogManager.Use<DefaultFactory>();
            defaultFactory.Level(LogLevel.Error);

            var configuration = new BusConfiguration();
            configuration.EndpointName("NServiceBus.SqlServerPerf.Destination");
            configuration.EnableInstallers();
            configuration.UseSerialization<JsonSerializer>();
            configuration.UseTransport<SqlServerTransport>().ConnectionString(connectionString);

            configuration.UsePersistence<InMemoryPersistence>();

            ProvideTransportConfiguration.Concurrency = concurrencyForReceives;
            Syncher.SyncEvent = new CountdownEvent(numberOfMessages);

            var stopWatch = Stopwatch.StartNew();

            var bus = Bus.Create(configuration).Start();

            Syncher.SyncEvent.Wait();

            stopWatch.Stop();

            bus.Dispose();
            Syncher.SyncEvent.Dispose();

            var avg = (double)numberOfMessages / stopWatch.ElapsedMilliseconds * 1000;
            Console.WriteLine($"RX: Concurrency {concurrencyForReceives,6:N0}, Time {stopWatch.Elapsed.TotalSeconds,4:N1} s, Throughput: {avg,8:N1} msg/s");
        }
    }

    public class Handler : IHandleMessages<ProduceChocolateBar>
    {
        public void Handle(ProduceChocolateBar message)
        {
            Syncher.SyncEvent.Signal();
        }
    }

    public class ProvideTransportConfiguration : IProvideConfiguration<TransportConfig>
    {
        public static int Concurrency;

        public TransportConfig GetConfiguration()
        {
            return new TransportConfig { MaximumConcurrencyLevel = Concurrency };
        }
    }

    public class ErrorConfigurationProvider : IProvideConfiguration<MessageForwardingInCaseOfFaultConfig>
    {
        public MessageForwardingInCaseOfFaultConfig GetConfiguration()
        {
            return new MessageForwardingInCaseOfFaultConfig { ErrorQueue = "error" };
        }
    }
}
