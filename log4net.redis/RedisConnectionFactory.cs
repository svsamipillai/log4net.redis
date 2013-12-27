using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using BookSleeve;

namespace log4net.redis
{
    public class RedisConnectionFactory : IRedisConnectionFactory
    {
        public Task<RedisConnection> CreateRedisConnection()
        {
            IDictionary<string, string> settings = RedisConnectionProvider.Instance.ConnectionsSettings;
            var redisConnection = new RedisConnection(host: settings["host"], port: Convert.ToInt32(settings["port"]),
                                                      password: settings["password"]);
            var taskCreateConnection = new TaskCompletionSource<RedisConnection>();

            try
            {
                redisConnection.Open().ContinueWith((task) =>
                    {
                        if (!task.IsFaulted)
                        {
                            taskCreateConnection.SetResult(redisConnection);
                        }
                        else
                        {
                            task.Exception.Handle(x => true);
                            taskCreateConnection.SetException(task.Exception);
                        }
                    }, TaskScheduler.Default);
            }
            catch (Exception ex)
            {
                taskCreateConnection.SetException(ex);
            }

            return taskCreateConnection.Task;
        }
    }
}