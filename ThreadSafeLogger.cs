using SwiftLogger.Enums;
using System.Collections.Concurrent;

public class ThreadSafeLogger
{
    private readonly SwiftLogger.SwiftLogger _logger;
    private readonly BlockingCollection<LogMessage> _logQueue;
    private readonly Task _logTask;
    private readonly CancellationTokenSource _cts;

    public ThreadSafeLogger(SwiftLogger.SwiftLogger logger)
    {
        _logger = logger;
        _logQueue = new BlockingCollection<LogMessage>();
        _cts = new CancellationTokenSource();
        _logTask = Task.Run(ProcessLogQueue);
    }

    public void Log(LogLevel level, string message)
    {
        if (!_cts.IsCancellationRequested)
        {
            _logQueue.Add(new LogMessage { Level = level, Message = message ?? string.Empty });
        }
    }

    private async Task ProcessLogQueue()
    {
        try
        {
            while (!_cts.Token.IsCancellationRequested || _logQueue.Count > 0)
            {
                if (_logQueue.TryTake(out var logMessage, 100, _cts.Token))
                {
                    await _logger.Log(logMessage.Level, logMessage.Message ?? string.Empty);
                }
            }
        }
        catch (OperationCanceledException)
        {
            // Expected exception when cancellation is requested, we can safely ignore it
        }
    }

    public async Task FlushAndStop()
    {
        _cts.Cancel();

        // Wait for the logging task to complete, with a timeout
        await Task.WhenAny(_logTask, Task.Delay(5000));

        // Process any remaining messages
        while (_logQueue.TryTake(out var logMessage))
        {
            await _logger.Log(logMessage.Level, logMessage.Message ?? string.Empty);
        }
    }

    private class LogMessage
    {
        public LogLevel Level { get; set; }
        public string Message { get; set; } = string.Empty;
    }
}