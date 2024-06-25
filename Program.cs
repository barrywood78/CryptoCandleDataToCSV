using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Coinbase.AdvancedTrade;
using Coinbase.AdvancedTrade.Enums;
using Coinbase.AdvancedTrade.Models;
using CsvHelper;
using CsvHelper.Configuration;

// Configuration
//var productIds = new[] { "BTC-USDC" };
//var granularities = new[] { Granularity.ONE_DAY };
var productIds = new[] { "BTC-USDC", "ETH-USDC" };
var granularities = new[] { Granularity.ONE_DAY, Granularity.ONE_HOUR, Granularity.FIFTEEN_MINUTE, Granularity.FIVE_MINUTE };
DateTimeOffset start = new(2019, 6, 1, 0, 0, 0, TimeSpan.Zero);
DateTimeOffset end = new(2024, 6, 1, 0, 0, 0, TimeSpan.Zero);
string folderPath = Environment.GetFolderPath(Environment.SpecialFolder.Desktop);

var coinbaseClient = new CoinbasePublicClient();
var spinner = new[] { "/", "-", "\\", "|" };
int spinnerIndex = 0;
const int BatchSize = 1000; // Adjust the batch size as needed
const int MaxRetryAttempts = 3; // Maximum number of retry attempts
const int RetryDelayMilliseconds = 2000; // Delay between retries in milliseconds

foreach (var productId in productIds)
{
    foreach (var granularity in granularities)
    {
        string fileName = $"candle_data_{productId}_{granularity}_{DateTime.Now:yyyyMMdd_HHmmss}.csv";
        string filePath = Path.Combine(folderPath, fileName);

        // Create the file and write the header once
        using (var writer = new StreamWriter(filePath))
        using (var csv = new CsvWriter(writer, CultureInfo.InvariantCulture))
        {
            csv.WriteHeader<Candle>();
            csv.NextRecord();
        }

        List<Candle> allCandles = new();

        DateTimeOffset currentChunkStart = start;

        while (currentChunkStart < end)
        {
            DateTimeOffset chunkEnd = GetChunkEnd(currentChunkStart, granularity);
            if (chunkEnd > end)
            {
                chunkEnd = end;
            }

            long startUnix = currentChunkStart.ToUnixTimeSeconds();
            long endUnix = chunkEnd.ToUnixTimeSeconds();

            bool success = false;
            int attempts = 0;

            while (!success && attempts < MaxRetryAttempts)
            {
                attempts++;

                try
                {
                    var candles = await coinbaseClient.Public.GetPublicProductCandlesAsync(productId, startUnix, endUnix, granularity);

                    if (candles == null)
                    {
                        throw new Exception("Failed to fetch candlestick data. API returned null.");
                    }

                    foreach (var candle in candles)
                    {
                        allCandles.Add(new Candle
                        {
                            ProductId = productId,
                            Granularity = granularity.ToString(),
                            StartUnix = long.Parse(candle.Start),
                            StartDate = DateTimeOffset.FromUnixTimeSeconds(long.Parse(candle.Start)).DateTime,
                            Low = decimal.Parse(candle.Low),
                            High = decimal.Parse(candle.High),
                            Open = decimal.Parse(candle.Open),
                            Close = decimal.Parse(candle.Close),
                            Volume = decimal.Parse(candle.Volume)
                        });

                        ShowSpinner($"Processing {productId} {granularity} data...", ref spinnerIndex, spinner);
                    }

                    if (allCandles.Count >= BatchSize)
                    {
                        // Write the collected data to the CSV file in chunks
                        WriteBatchToCsv(allCandles, filePath);
                        allCandles.Clear(); // Clear the list after writing
                    }

                    success = true;
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"\nError fetching data for {granularity} (Attempt {attempts}/{MaxRetryAttempts}): {ex.Message}");
                    if (attempts < MaxRetryAttempts)
                    {
                        await Task.Delay(RetryDelayMilliseconds);
                    }
                    else
                    {
                        Console.WriteLine("\nMax retry attempts reached. Exiting...");
                        return; // Exit the program
                    }
                }
            }

            currentChunkStart = chunkEnd;
            await Task.Delay(250); // Adding delay to avoid rate limits
        }

        // Write any remaining data to the CSV file
        if (allCandles.Count > 0)
        {
            WriteBatchToCsv(allCandles, filePath);
        }

        // Ensure file is closed before removing duplicates
        GC.Collect();
        GC.WaitForPendingFinalizers();

        // Remove duplicates from the CSV file
        RemoveDuplicatesFromCsv(filePath);

        Console.WriteLine($"\nData exported to CSV file successfully for {productId} {granularity}!");
    }
}

static DateTimeOffset GetChunkEnd(DateTimeOffset start, Granularity granularity)
{
    return granularity switch
    {
        Granularity.ONE_MINUTE => start.AddDays(1),
        Granularity.FIVE_MINUTE => start.AddDays(1),
        Granularity.FIFTEEN_MINUTE => start.AddDays(1),
        Granularity.ONE_HOUR => start.AddDays(1),
        Granularity.SIX_HOUR => start.AddDays(5),
        Granularity.ONE_DAY => start.AddMonths(1),
        _ => start.AddDays(1),
    };
}

static void WriteBatchToCsv(List<Candle> candleDataList, string filePath)
{
    // Ensure duplicates are removed before writing
    RemoveDuplicates(candleDataList);
    SortByStartDate(candleDataList);

    var config = new CsvConfiguration(CultureInfo.InvariantCulture)
    {
        HasHeaderRecord = false,
    };

    using var writer = new StreamWriter(filePath, append: true);
    using var csv = new CsvWriter(writer, config);
    csv.WriteRecords(candleDataList);
}

static void RemoveDuplicates(List<Candle> candleDataList)
{
    var uniqueCandles = candleDataList
        .GroupBy(candle => candle.StartUnix)
        .Select(group => group.First())
        .ToList();

    candleDataList.Clear();
    candleDataList.AddRange(uniqueCandles);
}

static void SortByStartDate(List<Candle> candleDataList)
{
    candleDataList.Sort((x, y) => x.StartDate.CompareTo(y.StartDate));
}

static void RemoveDuplicatesFromCsv(string filePath)
{
    var tempFilePath = Path.GetTempFileName();
    var config = new CsvConfiguration(CultureInfo.InvariantCulture)
    {
        HasHeaderRecord = true,
    };

    using (var reader = new StreamReader(filePath))
    using (var csvReader = new CsvReader(reader, config))
    {
        var records = csvReader.GetRecords<Candle>().ToList();
        RemoveDuplicates(records);
        SortByStartDate(records);

        using (var writer = new StreamWriter(tempFilePath))
        using (var csvWriter = new CsvWriter(writer, config))
        {
            csvWriter.WriteHeader<Candle>();
            csvWriter.NextRecord();
            csvWriter.WriteRecords(records);
        }
    }

    File.Delete(filePath);
    File.Move(tempFilePath, filePath);
}

static void ShowSpinner(string message, ref int spinnerIndex, string[] spinner)
{
    spinnerIndex = (spinnerIndex + 1) % spinner.Length;
    Console.SetCursorPosition(0, Console.CursorTop);
    Console.Write($"{spinner[spinnerIndex]} {message}");
}

public class Candle
{
    public string? ProductId { get; set; }
    public string? Granularity { get; set; }
    public long StartUnix { get; set; }
    public DateTime StartDate { get; set; }
    public decimal Low { get; set; }
    public decimal High { get; set; }
    public decimal Open { get; set; }
    public decimal Close { get; set; }
    public decimal Volume { get; set; }
}
