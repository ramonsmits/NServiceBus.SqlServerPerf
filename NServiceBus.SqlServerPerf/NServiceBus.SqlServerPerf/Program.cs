using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using NServiceBus.Config;
using NServiceBus.Config.ConfigurationSource;
using NServiceBus.Logging;

namespace NServiceBus.SqlServerPerf
{
    class Program
    {
        static void Main(string[] args)
        {
            string connectionString = @"FILL IN HERE";
            int numberOfMessages = 5000;
            int messageSize = 1024;
            int concurrency = 1;

            for (int i = 0; i < 9; i++)
            {
                for (int j = 0; j < 9; j++)
                {
                    SingleSendRun(connectionString, messageSize, numberOfMessages, concurrency);

                    SingleReceiveRun(connectionString, messageSize, numberOfMessages, concurrency);

                    messageSize *= 4;
                }

                concurrency *= 2;
            }

            Console.ReadLine();
        }

        private static void SingleSendRun(string connectionString, int messageSize, int numberOfMessages, int concurrencyForSends)
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
            CreateQueue(connectionString, destination);

            ProduceChocolateBar.Initialize(messageSize);

            var stopWatch = Stopwatch.StartNew();

            Parallel.For(0, numberOfMessages, new ParallelOptions {MaxDegreeOfParallelism = concurrencyForSends},
                i => { bus.Send(destination, new ProduceChocolateBar(true) {LotNumber = i, MaxLotNumber = numberOfMessages}); });

            stopWatch.Stop();

            bus.Dispose();

            Console.WriteLine($"NumberOfMessages {numberOfMessages}, MessageSize {messageSize}, SendConcurrency { concurrencyForSends}, SendTimeInMs { stopWatch.ElapsedMilliseconds }");
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

            Console.WriteLine($"NumberOfMessages {numberOfMessages}, MessageSize {messageSize}, ReceiveConcurrency { concurrencyForReceives}, ReceiveTimeInMs { stopWatch.ElapsedMilliseconds }");
        }

        static void CreateQueue(string connectionString, string queueName)
        {
            var ddl = @"IF NOT  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[{0}].[{1}]') AND type in (N'U'))
                  BEGIN
                    EXEC sp_getapplock @Resource = '{0}_{1}_lock', @LockMode = 'Exclusive'

                    IF NOT  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[{0}].[{1}]') AND type in (N'U'))
                    BEGIN
                        CREATE TABLE [{0}].[{1}](
	                        [Id] [uniqueidentifier] NOT NULL,
	                        [CorrelationId] [varchar](255) NULL,
	                        [ReplyToAddress] [varchar](255) NULL,
	                        [Recoverable] [bit] NOT NULL,
	                        [Expires] [datetime] NULL,
	                        [Headers] [varchar](max) NOT NULL,
	                        [Body] [varbinary](max) NULL,
	                        [RowVersion] [bigint] IDENTITY(1,1) NOT NULL
                        ) ON [PRIMARY];

                        CREATE CLUSTERED INDEX [Index_RowVersion] ON [{0}].[{1}]
                        (
	                        [RowVersion] ASC
                        )WITH (PAD_INDEX  = OFF, STATISTICS_NORECOMPUTE  = OFF, SORT_IN_TEMPDB = OFF, IGNORE_DUP_KEY = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS  = ON, ALLOW_PAGE_LOCKS  = ON) ON [PRIMARY]

                        CREATE NONCLUSTERED INDEX [Index_Expires] ON [{0}].[{1}]
                        (
	                        [Expires] ASC
                        )
                        INCLUDE
                        (
                            [Id],
                            [RowVersion]
                        )
                        WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON)
                    END

                    EXEC sp_releaseapplock @Resource = '{0}_{1}_lock'

                    TRUNCATE TABLE [{0}].[{1}]
                  END";

            var commandText = string.Format(ddl, "dbo", queueName);

            using (var connection = new SqlConnection(connectionString))
            {
                connection.Open();

                using (var transaction = connection.BeginTransaction())
                {
                    using (var command = new SqlCommand(commandText, connection, transaction))
                    {
                        command.ExecuteNonQuery();
                    }

                    transaction.Commit();
                }
            }
        }
    }

    public class ProduceChocolateBar : ICommand
    {
        private static string chunk;

        public int LotNumber { get; set; }

        public int MaxLotNumber { get; set; }

        public string Chunk { get; set; }

        public ProduceChocolateBar()
        {
        }

        public ProduceChocolateBar(bool initialize)
        {
            Chunk = chunk;
        }

        public static void Initialize(int messageSizeInBytes)
        {
            if (string.IsNullOrEmpty(chunk))
            {
                var b = new byte[messageSizeInBytes];
                new Random().NextBytes(b);

                chunk = Encoding.UTF8.GetString(b);
            }
        }
    }

    public class Handler : IHandleMessages<ProduceChocolateBar>
    {
        public void Handle(ProduceChocolateBar message)
        {
            Syncher.SyncEvent.Signal();
        }
    }

    public static class Syncher
    {
        public static CountdownEvent SyncEvent;
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
