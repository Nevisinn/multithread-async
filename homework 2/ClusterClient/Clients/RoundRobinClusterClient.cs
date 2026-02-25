using System;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using log4net;

namespace ClusterClient.Clients;

public class RoundRobinClusterClient : ClusterClientBase
{
    public RoundRobinClusterClient(string[] replicaAddresses) : base(replicaAddresses)
    {
    }

    protected override ILog Log => LogManager.GetLogger(typeof(RoundRobinClusterClient));

    public override async Task<string> ProcessRequestAsync(string query, TimeSpan timeout)
    {
        var badRequestCounter = 0;
        var sw = new Stopwatch();
        var orderedReplicaAddresses = ReplicaAddresses
            .OrderBy(r => ReplicaStats.Stats[r])
            .ToList();

        foreach (var address in orderedReplicaAddresses)
        {
            var webRequest = CreateRequest(address + "?query=" + query);

            Log.InfoFormat($"Processing {webRequest.RequestUri}");
                
            var task = ProcessRequestAsync(webRequest);
            var requestTimeout = timeout / (ReplicaAddresses.Length - badRequestCounter);
            var timeoutTask = Task.Delay(requestTimeout);
            
            sw.Restart();
            var completedTask = await Task.WhenAny(task, timeoutTask);
            sw.Stop();
            
            timeout -= sw.Elapsed;
            
            if (completedTask != timeoutTask && completedTask.Status == TaskStatus.RanToCompletion)
            {
                ReplicaStats.Update(address, sw.Elapsed.Milliseconds);
                return await (Task<string>)completedTask;
            }

            badRequestCounter++;
        }

        throw new TimeoutException();
    }
}