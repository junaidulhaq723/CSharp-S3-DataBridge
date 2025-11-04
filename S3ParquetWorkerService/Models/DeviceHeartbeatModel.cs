using LiteDB;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace S3ParquetWorkerService.Models
{
    public class DeviceHeartbeatModel
    {
        public ObjectId Id { get; set; } // LiteDB identifier
        public long EndpointId { get; set; }
        public DateTime ClientTime { get; set; }
    }
}
