using Amazon.S3;
using Amazon.SQS;
using S3ParquetWorkerService;
using S3ParquetWorkerService.Services;

var builder = Host.CreateApplicationBuilder(args);
builder.Services.AddHostedService<Worker>();

// Add AWS services
builder.Services.AddAWSService<IAmazonSQS>();
builder.Services.AddAWSService<IAmazonS3>();

// Add processor
builder.Services.AddSingleton<DeviceHeartbeatProcessor>();

builder.Configuration.SetBasePath(AppDomain.CurrentDomain.BaseDirectory);
builder.Configuration.AddJsonFile("appsettings.json", optional: true, reloadOnChange: true); // <-- Add this line

var host = builder.Build();
host.Run();
