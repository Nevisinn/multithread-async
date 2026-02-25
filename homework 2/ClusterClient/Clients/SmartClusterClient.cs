using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using log4net;

namespace ClusterClient.Clients;

public class SmartClusterClient : ClusterClientBase
{
    public SmartClusterClient(string[] replicaAddresses) : base(replicaAddresses)
    {
    }

    protected override ILog Log => LogManager.GetLogger(typeof(SmartClusterClient));

    public override async Task<string> ProcessRequestAsync(string query, TimeSpan timeout)
    {
        var badRequestCounter = 0;
        var sw = new Stopwatch();
        var tasks = new List<Task<string>>();
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
            tasks.Add(task);
            
            sw.Restart();
            var anyTask = Task.WhenAny(tasks);
            var completedTask = await Task.WhenAny(anyTask, timeoutTask);
            sw.Stop();
            
            timeout -= sw.Elapsed;

            if (completedTask != timeoutTask)
            {
                var result = await anyTask;
                if (result.IsFaulted) 
                    tasks.Remove(result);

                if (result.Status == TaskStatus.RanToCompletion)
                {
                    ReplicaStats.Update(address, sw.Elapsed.Milliseconds);
                    return await result;
                }
            }

            badRequestCounter++;
        }

        throw new TimeoutException();
    }
}