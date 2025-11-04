using Amazon.SQS;
using Amazon.SQS.Model;
using Amazon.S3;
using Amazon.S3.Model;
using System.Text.Json;
using Parquet;
using Parquet.Data;
using Microsoft.Extensions.Logging;
using LiteDB;
using JsonSerializer = System.Text.Json.JsonSerializer;
using Parquet.Schema;
using S3ParquetWorkerService.Models;

namespace S3ParquetWorkerService.Services;

public class DeviceHeartbeatProcessor
{
    private readonly ILogger<DeviceHeartbeatProcessor> _logger;
    private readonly IAmazonSQS _sqsClient;
    private readonly IAmazonS3 _s3Client;
    private readonly string _queueUrl;
    private readonly string _s3Bucket;
    private readonly string _dbPath;
    private readonly SemaphoreSlim _processLock = new(1, 1);

    public DeviceHeartbeatProcessor(
        ILogger<DeviceHeartbeatProcessor> logger,
        IAmazonSQS sqsClient,
        IAmazonS3 s3Client,
        IConfiguration configuration)
    {
        _logger = logger;
        _sqsClient = sqsClient;
        _s3Client = s3Client;
        _queueUrl = configuration["AWS:SQSQueueUrl"] ?? throw new ArgumentNullException("SQS Queue URL not configured");
        _s3Bucket = configuration["AWS:S3Bucket"] ?? throw new ArgumentNullException("S3 Bucket not configured");
        _dbPath = configuration["FileDB:Path"] ?? "data/heartbeats.db";
        
        // Ensure directory exists
        Directory.CreateDirectory(Path.GetDirectoryName(_dbPath) ?? "data");

        // Initialize database
        using var db = new LiteDatabase(_dbPath);
        var collection = db.GetCollection<DeviceHeartbeatModel>("heartbeats");
        collection.EnsureIndex(x => x.ClientTime);
    }

    public async Task ProcessSQSMessages(CancellationToken cancellationToken)
    {
        try
        {
            var request = new ReceiveMessageRequest
            {
                QueueUrl = _queueUrl,
                MaxNumberOfMessages = 10,
                WaitTimeSeconds = 20
            };

            var response = await _sqsClient.ReceiveMessageAsync(request, cancellationToken);
            if (response.Messages != null && response.Messages.Count > 0)
            {
                foreach (var message in response.Messages)
                {
                    try
                    {
                        var heartbeat = JsonSerializer.Deserialize<DeviceHeartbeatModel>(message.Body);
                        if (heartbeat != null)
                        {
                            await AppendToFileDb(heartbeat);
                            await _sqsClient.DeleteMessageAsync(_queueUrl, message.ReceiptHandle, cancellationToken);
                        }
                    }
                    catch (Exception ex)
                    {
                        _logger.LogError(ex, "Error processing message: {MessageId}", message.MessageId);
                    }
                }
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error receiving SQS messages");
        }
    }

    public async Task ProcessFileDbToS3()
    {
        if (!await _processLock.WaitAsync(0))
        {
            _logger.LogInformation("Another S3 upload process is already running");
            return;
        }

        try
        {
            using var db = new LiteDatabase(_dbPath);
            var collection = db.GetCollection<DeviceHeartbeatModel>("heartbeats");
            
            // Group records by hour
            var now = DateTime.UtcNow;
            var records = collection.Find(x => x.ClientTime < now)
                .GroupBy(x => new
                {
                    Year = x.ClientTime.Year,
                    Month = x.ClientTime.Month,
                    Day = x.ClientTime.Day,
                    Hour = x.ClientTime.Hour
                })
                .ToList();
            var s3Key = $"{now.Year}/{now.Month:D2}/{now.Day:D2}/{now.Hour:D2}/heartbeats_{now:yyyyMMddHHmmss}.parquet";
            foreach (var group in records)
            {
                var groupRecords = group.ToList();
                if (groupRecords.Any())
                {
                    // Create S3 key with partitioning
                    
                    
                    await CreateAndUploadParquetFile(groupRecords, s3Key);

                    // Delete processed records
                    var ids = groupRecords.Select(r => r.Id).ToArray();
                    collection.DeleteMany(x => ids.Contains(x.Id));
                }
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error processing file DB to S3");
        }
        finally
        {
            _processLock.Release();
        }
    }

    private async Task AppendToFileDb(DeviceHeartbeatModel heartbeat)
    {
        using var db = new LiteDatabase(_dbPath);
        var collection = db.GetCollection<DeviceHeartbeatModel>("heartbeats");
        collection.Insert(heartbeat);
        await Task.CompletedTask; // LiteDB operations are synchronous
    }

    private async Task CreateAndUploadParquetFile(List<DeviceHeartbeatModel> records, string s3Key)
    {
        using var memStream = new MemoryStream();
        
        var deviceIds = new DataField<long>("device_id");
        var heartbeatTimes = new DataField<DateTime>("heartbeat_time");
        
        var schema = new ParquetSchema( deviceIds, heartbeatTimes );

        using (var parquetWriter = await ParquetWriter.CreateAsync(schema, memStream))
        {
            using var groupWriter = parquetWriter.CreateRowGroup();
            
            await groupWriter.WriteColumnAsync(
                new DataColumn(deviceIds, records.Select(r => r.EndpointId).ToArray()));
            
            await groupWriter.WriteColumnAsync(
                new DataColumn(heartbeatTimes, records.Select(r => r.ClientTime).ToArray()));
        }

        memStream.Position = 0;
        
        var putRequest = new PutObjectRequest
        {
            BucketName = _s3Bucket,
            Key = s3Key,
            InputStream = memStream
        };

        await _s3Client.PutObjectAsync(putRequest);
    }
}

