using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Fclp.Internals.Extensions;
using log4net;

namespace ClusterClient.Clients;

public class ParallelClusterClient : ClusterClientBase
{
    public ParallelClusterClient(string[] replicaAddresses) : base(replicaAddresses)
    {
    }

    protected override ILog Log => LogManager.GetLogger(typeof(ParallelClusterClient));

    public override async Task<string> ProcessRequestAsync(string query, TimeSpan timeout)
    {
        var webRequests = ReplicaAddresses
            .Select(replicaAddress => CreateRequest(replicaAddress + "?query=" + query))
            .ToList();

        webRequests.ForEach(r => Log.InfoFormat($"Processing {r.RequestUri}"));
        
        var timeoutTask = Task.Delay(timeout);

        var tasks = webRequests
            .Select(ProcessRequestAsync)
            .Append(timeoutTask)
            .ToList();
        
        while (tasks.Count > 1)
        {   
            var completedTask = await Task.WhenAny(tasks);

            if (completedTask == timeoutTask)
                throw new TimeoutException();

            tasks.Remove(completedTask);

            if (completedTask.Status == TaskStatus.RanToCompletion)
                return await (Task<string>)completedTask;
        }

        throw new TimeoutException();
    }
}