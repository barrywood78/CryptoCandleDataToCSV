using System.Collections.Concurrent;
using SwiftLogger;
using SwiftLogger.Enums;

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
        _logQueue.Add(new LogMessage { Level = level, Message = message });
    }

    private async Task ProcessLogQueue()
    {
        while (!_cts.IsCancellationRequested)
        {
            if (_logQueue.TryTake(out var logMessage, Timeout.Infinite, _cts.Token))
            {
                await _logger.Log(logMessage.Level, logMessage.Message);
            }
        }
    }

    public async Task FlushAndStop()
    {
        _cts.Cancel();
        while (_logQueue.TryTake(out var logMessage))
        {
            await _logger.Log(logMessage.Level, logMessage.Message);
        }
        await _logTask;
    }

    private class LogMessage
    {
        public LogLevel Level { get; set; }
        public string Message { get; set; }
    }
}