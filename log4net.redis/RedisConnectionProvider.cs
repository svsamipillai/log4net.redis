using System;
using System.Collections.Generic;
using System.Linq;
using System.Xml.Linq;

namespace log4net.redis
{
    internal class RedisConnectionProvider
    {
        #region static

        private static readonly RedisConnectionProvider instance = new RedisConnectionProvider();

        public static RedisConnectionProvider Instance
        {
            get { return instance; }
        }

        #endregion

        private readonly Lazy<IDictionary<string, string>> _connectionSettingsLazy;

        private RedisConnectionProvider()
        {
            _connectionSettingsLazy = new Lazy<IDictionary<string, string>>(GetConnetionSettings,
                                                                           isThreadSafe: true);
        }

        public IDictionary<string, string> ConnectionsSettings
        {
            get { return _connectionSettingsLazy.Value.ToDictionary(x => x.Key, x => x.Value); }
        }

        private IDictionary<string, string> GetConnetionSettings()
        {
            XDocument document = XDocument.Load("log4net.redis.xml");
            IEnumerable<XAttribute> settings = document.Descendants("connection").First().Attributes();
            return settings.ToDictionary(
                attribute => attribute.Name.LocalName,
                attribute => attribute.Value);
        }
    }
}