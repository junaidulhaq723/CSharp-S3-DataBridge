using S3ParquetWorkerService.Services;

namespace S3ParquetWorkerService
{
    public class Worker : BackgroundService
    {
        private readonly ILogger<Worker> _logger;
        private readonly DeviceHeartbeatProcessor _processor;
        private DateTime _lastS3Upload = DateTime.UtcNow;

        public Worker(ILogger<Worker> logger, DeviceHeartbeatProcessor processor)
        {
            _logger = logger;
            _processor = processor;
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                try
                {
                    // Process SQS messages continuously
                    await _processor.ProcessSQSMessages(stoppingToken);

                    // Check if an hour has passed since last S3 upload
                    if (DateTime.UtcNow - _lastS3Upload >= TimeSpan.FromHours(1))
                    {
                        await _processor.ProcessFileDbToS3();
                        _lastS3Upload = DateTime.UtcNow;
                    }
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Error in worker process");
                }
            }
        }
    }
}
