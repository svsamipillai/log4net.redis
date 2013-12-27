using System.Threading.Tasks;
using BookSleeve;

namespace log4net.redis
{
    public interface IRedisConnectionFactory
    {
        Task<RedisConnection> CreateRedisConnection();
    }
}