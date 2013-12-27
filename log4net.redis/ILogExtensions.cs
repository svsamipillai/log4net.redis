namespace log4net.redis
{
    internal static class ILogExtensions
    {
        public static ILog ObserverKey(this ILog log, string key)
        {
            return new RedisLogger(key, log, new RedisConnectionFactory());
        }
    }
}