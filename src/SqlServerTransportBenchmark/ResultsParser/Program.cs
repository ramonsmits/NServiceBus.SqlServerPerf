using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ResultsParser
{
    class Program
    {
        static string SqlV5ResultsFileName = "sql-v5-results.txt";

        static string SqlV5SendResultsParsedFileName = "sql-v5-send-results-parsed.csv";
        static string SqlV5ReceiveResultsParsedFileName = "sql-v5-receive-results-parsed.csv";

        static string SqlV6ResultsFileName = "sql-v6-results.txt";

        static string SqlV6SendResultsParsedFileName = "sql-v6-send-results-parsed.csv";
        static string SqlV6ReceiveResultsParsedFileName = "sql-v6-receive-results-parsed.csv";

        static string SqlV5AzureResultsFileName = "sql-v5-results-azure.txt";

        static string SqlV5SendAzureResultsParsedFileName = "sql-v5-azure-send-results-parsed.csv";
        static string SqlV5ReceiveAzureResultsParsedFileName = "sql-v5-azure-receive-results-parsed.csv";

        static string SqlV6AzureResultsFileName = "sql-v6-results-azure.txt";

        static string SqlV6SendAzureResultsParsedFileName = "sql-v6-azure-send-results-parsed.csv";
        static string SqlV6ReceiveAzureResultsParsedFileName = "sql-v6-azure-receive-results-parsed.csv";

        static void Main(string[] args)
        {
            ParseResults("Send:", SqlV5ResultsFileName, SqlV5SendResultsParsedFileName);
            ParseResults("Receive:", SqlV5ResultsFileName, SqlV5ReceiveResultsParsedFileName);

            ParseResults("Send:", SqlV6ResultsFileName, SqlV6SendResultsParsedFileName);
            ParseResults("Receive:", SqlV6ResultsFileName, SqlV6ReceiveResultsParsedFileName);

            ParseResults("Send:", SqlV5AzureResultsFileName, SqlV5SendAzureResultsParsedFileName);
            ParseResults("Receive:", SqlV5AzureResultsFileName, SqlV5ReceiveAzureResultsParsedFileName);

            ParseResults("Send:", SqlV6AzureResultsFileName, SqlV6SendAzureResultsParsedFileName);
            ParseResults("Receive:", SqlV6AzureResultsFileName, SqlV6ReceiveAzureResultsParsedFileName);
        }

        static void ParseResults(string prefix, string inputFile, string outputFile)
        {
            var sendResults = File.ReadAllLines(inputFile)
                .Where(l => l.StartsWith(prefix))
                .Select(l => l.Replace(prefix, String.Empty))
                .Select(l => l.Split(',').Select(i => int.Parse(i.Split(' ').Last())).ToArray())
                .Select(p => new MeasurementPoint
                {
                    MessageSizeInBytes = p[1],
                    ConcurrencyLimit = p[2],
                    TimeInMilliseconds = p[3]
                })
                .ToArray();

            var headerRow = sendResults
                .Select(r => r.MessageSizeInBytes)
                .Distinct()
                .OrderBy(_ => _)
                .Select(i => i.ToString())
                .Aggregate(@"Concurrency\MessageSize", (c, i) => c + "|" + i);


            var dataRows = sendResults
                .GroupBy(r => r.ConcurrencyLimit)
                .OrderBy(g => g.Key)
                .Select(g => g
                    .OrderBy(i => i.MessageSizeInBytes)
                    .Select(i => i.TimeInMilliseconds.ToString())
                    .Aggregate(g.Key.ToString(), (p, i) => p + "|" + i)
                );

            var allRows = new[] {headerRow}.ToList();
            allRows.AddRange(dataRows);

            File.WriteAllLines(outputFile, allRows);
        }


        class MeasurementPoint
        {
            public int MessageSizeInBytes { get; set; }

            public int ConcurrencyLimit { get; set; }

            public int TimeInMilliseconds { get; set; }
        }
    }
}
