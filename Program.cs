using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Coinbase.AdvancedTrade;
using Coinbase.AdvancedTrade.Enums;
using Coinbase.AdvancedTrade.Models;
using CsvHelper;
using CsvHelper.Configuration;
using SwiftLogger;
using SwiftLogger.Configs;
using SwiftLogger.Enums;
using Microsoft.Extensions.Configuration;

class Program
{
    private static SemaphoreSlim? RateLimiter;
    private static TimeSpan RateLimitInterval;
    private static ThreadSafeLogger? logger;

    static async Task Main(string[] args)
    {
        // Load configuration
        var config = new ConfigurationBuilder()
            .SetBasePath(Directory.GetCurrentDirectory())
            .AddJsonFile("appsettings.json", optional: false, reloadOnChange: true)
            .Build();

        // Read configuration values
        var productIds = config.GetSection("ProductIds").Get<string[]>() ?? Array.Empty<string>();
        var granularities = config.GetSection("Granularities").Get<string[]>()
            ?.Select(g => Enum.Parse<Granularity>(g))
            .ToArray() ?? Array.Empty<Granularity>();
        var start = config.GetValue<DateTimeOffset>("StartDate");
        var end = config.GetValue<DateTimeOffset>("EndDate");
        string folderPath = config.GetValue<string>("OutputFolder") ?? string.Empty;
        int batchSize = config.GetValue<int>("BatchSize");
        int maxRetryAttempts = config.GetValue<int>("MaxRetryAttempts");
        int retryDelayMilliseconds = config.GetValue<int>("RetryDelayMilliseconds");
        int requestsPerSecond = config.GetValue<int>("RateLimiting:RequestsPerSecond");
        int intervalSeconds = config.GetValue<int>("RateLimiting:IntervalSeconds");

        int maxConcurrentTasks = config.GetValue<int>("MaxConcurrentTasks", 1); // Default to 1 if not specified

        RateLimiter = new SemaphoreSlim(requestsPerSecond, requestsPerSecond);
        RateLimitInterval = TimeSpan.FromSeconds(intervalSeconds);

        var coinbaseClient = new CoinbasePublicClient();

        DateTime startTime = DateTime.Now;
        string timestamp = startTime.ToString("yyyyMMdd_HHmmss");

        var consoleConfig = new ConsoleLoggerConfig()
            .SetColorForLogLevel(LogLevel.Error, ConsoleColor.Red)
            .SetColorForLogLevel(LogLevel.Warning, ConsoleColor.Yellow)
            .SetMinimumLogLevel(LogLevel.Information);

        var fileConfig = new FileLoggerConfig()
            .SetFilePath($"CryptoCandleDataToCSV-{timestamp}.txt")
            .EnableSeparationByDate();

        var swiftLogger = new LoggerConfigBuilder()
            .LogTo.Console(consoleConfig)
            .LogTo.File(fileConfig)
            .Build();

        logger = new ThreadSafeLogger(swiftLogger);

        try
        {
            logger.Log(LogLevel.Information, $"CryptoCandleDataToCSV started at: {startTime}");

            await ProcessGranularities(granularities, productIds, start, end, folderPath, batchSize, maxRetryAttempts, retryDelayMilliseconds, coinbaseClient, maxConcurrentTasks);

            logger.Log(LogLevel.Information, $"Application completed at: {DateTime.Now}");
        }
        finally
        {
            await logger.FlushAndStop();
        }
    }

    static async Task ProcessGranularities(Granularity[] granularities, string[] productIds, DateTimeOffset start, DateTimeOffset end, string folderPath, int batchSize, int maxRetryAttempts, int retryDelayMilliseconds, CoinbasePublicClient coinbaseClient, int maxConcurrentTasks)
    {
        foreach (var granularity in granularities)
        {
            await ProcessGranularityAllProducts(granularity, productIds, start, end, folderPath, batchSize, maxRetryAttempts, retryDelayMilliseconds, coinbaseClient, maxConcurrentTasks);
        }
    }

    static async Task ProcessGranularityAllProducts(Granularity granularity, string[] productIds, DateTimeOffset start, DateTimeOffset end, string folderPath, int batchSize, int maxRetryAttempts, int retryDelayMilliseconds, CoinbasePublicClient coinbaseClient, int maxConcurrentTasks)
    {
        var semaphore = new SemaphoreSlim(maxConcurrentTasks, maxConcurrentTasks);
        var tasks = new List<Task>();

        foreach (var productId in productIds)
        {
            await semaphore.WaitAsync();
            tasks.Add(Task.Run(async () =>
            {
                try
                {
                    await ProcessProductGranularity(productId, granularity, start, end, folderPath, batchSize, maxRetryAttempts, retryDelayMilliseconds, coinbaseClient);
                }
                finally
                {
                    semaphore.Release();
                }
            }));
        }

        await Task.WhenAll(tasks);
    }

    static async Task ProcessProductGranularity(string productId, Granularity granularity, DateTimeOffset start, DateTimeOffset end, string folderPath, int batchSize, int maxRetryAttempts, int retryDelayMilliseconds, CoinbasePublicClient coinbaseClient)
    {
        string fileName = $"candle_data_{productId}_{granularity}.csv";
        string filePath = Path.Combine(folderPath, fileName);

        // Create the file and write the header
        using (var writer = new StreamWriter(filePath))
        using (var csv = new CsvWriter(writer, CultureInfo.InvariantCulture))
        {
            csv.WriteHeader<Candle>();
            csv.NextRecord();
        }

        var candleBuffer = new List<Candle>(batchSize);

        DateTimeOffset currentChunkStart = start;
        int chunkCount = 0;
        int totalChunks = (int)Math.Ceiling((end - start).TotalDays / GetChunkDays(granularity));
        var progress = new Progress<ProgressReport>(report =>
            logger?.Log(LogLevel.Information, $"{productId} {granularity} Progress: {report.PercentComplete:F2}% - ETA: {report.EstimatedTimeRemaining:hh\\:mm\\:ss}")
        );

        var progressTracker = new ProgressTracker(totalChunks);

        while (currentChunkStart < end)
        {
            chunkCount++;
            DateTimeOffset chunkEnd = GetChunkEnd(currentChunkStart, granularity);
            if (chunkEnd > end)
            {
                chunkEnd = end;
            }

            long startUnix = currentChunkStart.ToUnixTimeSeconds();
            long endUnix = chunkEnd.ToUnixTimeSeconds();

            bool success = false;
            int attempts = 0;

            while (!success && attempts < maxRetryAttempts)
            {
                attempts++;

                try
                {
                    logger?.Log(LogLevel.Information, $"Processing chunk {chunkCount}/{totalChunks} for {productId} {granularity} ({currentChunkStart:yyyy-MM-dd} to {chunkEnd:yyyy-MM-dd})");

                    await RateLimiter?.WaitAsync()!;
                    try
                    {
                        var candles = await coinbaseClient.Public.GetPublicProductCandlesAsync(productId, startUnix, endUnix, granularity);

                        if (candles == null)
                        {
                            throw new Exception("Failed to fetch candlestick data. API returned null.");
                        }

                        foreach (var candle in candles)
                        {
                            candleBuffer.Add(new Candle
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

                            if (candleBuffer.Count >= batchSize)
                            {
                                await WriteBatchToCsvAsync(candleBuffer, filePath);
                                candleBuffer.Clear();
                            }
                        }

                        success = true;
                    }
                    finally
                    {
                        _ = Task.Delay(RateLimitInterval).ContinueWith(_ => RateLimiter.Release());
                    }
                }
                catch (Exception ex)
                {
                    logger?.Log(LogLevel.Error, $"Error fetching data for {productId} {granularity} (Attempt {attempts}/{maxRetryAttempts}): {ex.Message}");
                    if (attempts < maxRetryAttempts)
                    {
                        await Task.Delay(retryDelayMilliseconds);
                    }
                    else
                    {
                        logger?.Log(LogLevel.Critical, $"Max retry attempts reached for chunk. Moving to next chunk.");
                        break;
                    }
                }
            }

            currentChunkStart = chunkEnd;
            progressTracker.IncrementProgress();
            ((IProgress<ProgressReport>)progress).Report(progressTracker.GetProgressReport());
        }

        // Write any remaining data to the CSV file
        if (candleBuffer.Count > 0)
        {
            await WriteBatchToCsvAsync(candleBuffer, filePath);
        }

        await RemoveDuplicatesFromCsvAsync(filePath);

        logger?.Log(LogLevel.Information, $"Data exported to CSV file successfully for {productId} {granularity}!");
    }

    static async Task WriteBatchToCsvAsync(List<Candle> candleDataList, string filePath)
    {
        var config = new CsvConfiguration(CultureInfo.InvariantCulture)
        {
            HasHeaderRecord = false,
        };

        using (var writer = new StreamWriter(filePath, append: true))
        using (var csv = new CsvWriter(writer, config))
        {
            await csv.WriteRecordsAsync(candleDataList);
        }
    }

    static async Task RemoveDuplicatesFromCsvAsync(string filePath)
    {
        var tempFilePath = Path.GetTempFileName();
        var config = new CsvConfiguration(CultureInfo.InvariantCulture)
        {
            HasHeaderRecord = true,
        };

        var uniqueRecords = new HashSet<Candle>(new CandleComparer());

        using (var reader = new StreamReader(filePath))
        using (var csv = new CsvReader(reader, config))
        {
            await foreach (var record in csv.GetRecordsAsync<Candle>())
            {
                uniqueRecords.Add(record);
            }
        }

        var sortedRecords = uniqueRecords.OrderBy(c => c.StartDate).ToList();

        using (var writer = new StreamWriter(tempFilePath))
        using (var csv = new CsvWriter(writer, config))
        {
            await csv.WriteRecordsAsync(sortedRecords);
        }

        File.Delete(filePath);
        File.Move(tempFilePath, filePath);
    }

    static DateTimeOffset GetChunkEnd(DateTimeOffset start, Granularity granularity) => granularity switch
    {
        Granularity.ONE_MINUTE => start.AddDays(1),
        Granularity.FIVE_MINUTE => start.AddDays(1),
        Granularity.FIFTEEN_MINUTE => start.AddDays(1),
        Granularity.ONE_HOUR => start.AddDays(1),
        Granularity.SIX_HOUR => start.AddDays(5),
        Granularity.ONE_DAY => start.AddMonths(1),
        _ => start.AddDays(1),
    };

    static double GetChunkDays(Granularity granularity) => granularity switch
    {
        Granularity.ONE_MINUTE => 1,
        Granularity.FIVE_MINUTE => 1,
        Granularity.FIFTEEN_MINUTE => 1,
        Granularity.ONE_HOUR => 1,
        Granularity.SIX_HOUR => 5,
        Granularity.ONE_DAY => 30,
        _ => 1,
    };
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

public class CandleComparer : IEqualityComparer<Candle>
{
    public bool Equals(Candle? x, Candle? y)
    {
        if (ReferenceEquals(x, y)) return true;
        if (x is null || y is null) return false;
        return x.ProductId == y.ProductId &&
               x.Granularity == y.Granularity &&
               x.StartUnix == y.StartUnix;
    }

    public int GetHashCode(Candle obj)
    {
        return HashCode.Combine(obj.ProductId, obj.Granularity, obj.StartUnix);
    }
}

public class ProgressTracker
{
    private readonly int _totalChunks;
    private int _completedChunks;
    private readonly DateTime _startTime;

    public ProgressTracker(int totalChunks)
    {
        _totalChunks = totalChunks;
        _completedChunks = 0;
        _startTime = DateTime.Now;
    }

    public void IncrementProgress()
    {
        Interlocked.Increment(ref _completedChunks);
    }

    public ProgressReport GetProgressReport()
    {
        var progress = (double)_completedChunks / _totalChunks;
        var elapsedTime = DateTime.Now - _startTime;
        var estimatedTotalTime = TimeSpan.FromTicks((long)(elapsedTime.Ticks / progress));
        var estimatedTimeRemaining = estimatedTotalTime - elapsedTime;

        return new ProgressReport
        {
            PercentComplete = progress * 100,
            EstimatedTimeRemaining = estimatedTimeRemaining
        };
    }
}

public class ProgressReport
{
    public double PercentComplete { get; set; }
    public TimeSpan EstimatedTimeRemaining { get; set; }
}