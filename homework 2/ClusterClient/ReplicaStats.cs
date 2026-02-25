using System.Collections.Concurrent;
using Fclp.Internals.Extensions;

namespace ClusterClient;

public class ReplicaStats
{
    private const float Coefficient = 0.2f;
    public readonly ConcurrentDictionary<string, float> Stats = new();

    public ReplicaStats(string[] replicas)
    {
        replicas.ForEach(r => Stats.TryAdd(r, 0));
    }

    public void Update(string replica, int latency)
    {
        var oldValue = Stats[replica];
        var ema = Stats[replica] * (1 - Coefficient) + latency * Coefficient;
        Stats.TryUpdate(replica, ema, oldValue);
    }
}