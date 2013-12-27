using System;
using System.Globalization;
using System.Reactive.Linq;
using System.Reactive.Subjects;
using System.Threading;
using System.Threading.Tasks;
using BookSleeve;
using log4net.Core;

namespace log4net.redis
{
    internal class RedisLogger : IRedisLog, IDisposable
    {
        private readonly string key;
        private readonly ILog log;
        private readonly ReplaySubject<Tuple<string, string>> messagesSubject;
        private readonly BehaviorSubject<bool> retry;
        private volatile RedisConnection redisConnection;
        private RedisConnectionFactory redisConnectionFactory;
        private IDisposable subscription;

        internal RedisLogger(string key, ILog log, IRedisConnectionFactory redisConnectionFactory)
        {
            this.key = string.Format(CultureInfo.InvariantCulture, "{0}:{1}", log.Logger.Name, key);
            this.log = log;
            messagesSubject = new ReplaySubject<Tuple<string, string>>(100, TimeSpan.FromSeconds(5));
            retry = new BehaviorSubject<bool>(false);

            var redisOnConnectionAction = new Action<Task<RedisConnection>>(task =>
                {
                    if (task.IsCompleted && !task.IsFaulted)
                    {
                        Interlocked.CompareExchange(ref redisConnection, task.Result, null);
                        subscription =
                            messagesSubject.TakeUntil(retry.Skip(1))
                                           .Subscribe(
                                               (item) => redisConnection.Publish(item.Item1, item.Item2)
                                                                        .ContinueWith(
                                                                            taskWithException => taskWithException.Exception.Handle(
                                                                                ex => true),
                                                                            TaskContinuationOptions.OnlyOnFaulted));
                    }
                });

            var redisOnErrorAction = new Action<ErrorEventArgs>(ex =>
                {
                    if (ex.IsFatal)
                    {
                        retry.OnNext(true);
                        Interlocked.Exchange(ref redisConnection, null);
                    }
                });

            Action subscribeAction = () =>
                {
                    Task<RedisConnection> connectionTask = redisConnectionFactory.CreateRedisConnection();
                    connectionTask.ContinueWith(taskConnection =>
                        {
                            if (!taskConnection.IsFaulted)
                            {
                                taskConnection.ContinueWith(redisOnConnectionAction);
                                taskConnection.Result.Error += (_, err) => redisOnErrorAction(err);
                            }
                            else
                            {
                                taskConnection.Exception.Handle(_ => true);
                                retry.OnNext(true);
                            }
                        });
                };

            retry.Subscribe(val =>
                {
                    if (val)
                        Observable.Timer(TimeSpan.FromSeconds(10)).Subscribe(_ => subscribeAction());
                    else
                        subscribeAction();
                });
        }

        public void Dispose()
        {
            if (redisConnection != null)
            {
                redisConnection.Close(true);
                redisConnection.Dispose();
                redisConnection = null;
            }
        }

        public ILog ObserverKey(string key, ILog log, IRedisConnectionFactory redisConnectionFactory)
        {
            return new RedisLogger(key, log, redisConnectionFactory);
        }

        public void Debug(object message, Exception exception)
        {
            log.Debug(message, exception);
        }

        public void Debug(object message)
        {
            Publish(key, message.ToString());
            log.Debug(message);
        }

        public void DebugFormat(IFormatProvider provider, string format, params object[] args)
        {
            Publish(key, string.Format(provider, format, args));
            log.DebugFormat(provider, format, args);
        }

        public void DebugFormat(string format, object arg0, object arg1, object arg2)
        {
            Publish(key, string.Format(format, arg0, arg1, arg2));
            log.DebugFormat(format, arg0, arg1, arg2);
        }

        public void DebugFormat(string format, object arg0, object arg1)
        {
            Publish(key, string.Format(format, arg0, arg1));
            log.DebugFormat(format, arg0, arg1);
        }

        public void DebugFormat(string format, object arg0)
        {
            Publish(key, string.Format(format, arg0));
            log.DebugFormat(format, arg0);
        }

        public void DebugFormat(string format, params object[] args)
        {
            Publish(key, string.Format(format, args));
            log.DebugFormat(format, args);
        }

        public void Error(object message, Exception exception)
        {
            log.Error(message, exception);
        }

        public void Error(object message)
        {
            log.Error(message);
        }

        public void ErrorFormat(IFormatProvider provider, string format, params object[] args)
        {
            log.ErrorFormat(provider, format, args);
        }

        public void ErrorFormat(string format, object arg0, object arg1, object arg2)
        {
            log.ErrorFormat(format, arg0, arg1, arg2);
        }

        public void ErrorFormat(string format, object arg0, object arg1)
        {
            log.ErrorFormat(format, arg0, arg1);
        }

        public void ErrorFormat(string format, object arg0)
        {
            log.ErrorFormat(format, arg0);
        }

        public void ErrorFormat(string format, params object[] args)
        {
            log.ErrorFormat(format, args);
        }

        public void Fatal(object message, Exception exception)
        {
            log.Fatal(message, exception);
        }

        public void Fatal(object message)
        {
            log.Fatal(message);
        }

        public void FatalFormat(IFormatProvider provider, string format, params object[] args)
        {
            log.FatalFormat(provider, format, args);
        }

        public void FatalFormat(string format, object arg0, object arg1, object arg2)
        {
            log.FatalFormat(format, arg0, arg1, arg2);
        }

        public void FatalFormat(string format, object arg0, object arg1)
        {
            log.FatalFormat(format, arg0, arg1);
        }

        public void FatalFormat(string format, object arg0)
        {
            log.FatalFormat(format, arg0);
        }

        public void FatalFormat(string format, params object[] args)
        {
            log.FatalFormat(format, args);
        }

        public void Info(object message, Exception exception)
        {
            log.Info(message, exception);
        }

        public void Info(object message)
        {
            log.Info(message);
        }

        public void InfoFormat(IFormatProvider provider, string format, params object[] args)
        {
            log.InfoFormat(provider, format, args);
        }

        public void InfoFormat(string format, object arg0, object arg1, object arg2)
        {
            log.InfoFormat(format, arg0, arg1, arg2);
        }

        public void InfoFormat(string format, object arg0, object arg1)
        {
            log.InfoFormat(format, arg0, arg1);
        }

        public void InfoFormat(string format, object arg0)
        {
            log.InfoFormat(format, arg0);
        }

        public void InfoFormat(string format, params object[] args)
        {
            log.InfoFormat(format, args);
        }

        public bool IsDebugEnabled
        {
            get { return log.IsDebugEnabled; }
        }

        public bool IsErrorEnabled
        {
            get { return log.IsErrorEnabled; }
        }

        public bool IsFatalEnabled
        {
            get { return log.IsFatalEnabled; }
        }

        public bool IsInfoEnabled
        {
            get { return log.IsInfoEnabled; }
        }

        public bool IsWarnEnabled
        {
            get { return log.IsWarnEnabled; }
        }

        public void Warn(object message, Exception exception)
        {
            log.Warn(message, exception);
        }

        public void Warn(object message)
        {
            log.Warn(message);
        }

        public void WarnFormat(IFormatProvider provider, string format, params object[] args)
        {
            log.WarnFormat(provider, format, args);
        }

        public void WarnFormat(string format, object arg0, object arg1, object arg2)
        {
            log.DebugFormat(format, arg0, arg1, arg2);
        }

        public void WarnFormat(string format, object arg0, object arg1)
        {
            log.WarnFormat(format, arg0, arg1);
        }

        public void WarnFormat(string format, object arg0)
        {
            log.WarnFormat(format, arg0);
        }

        public void WarnFormat(string format, params object[] args)
        {
            log.WarnFormat(format, args);
        }

        public ILogger Logger
        {
            get { return log.Logger; }
        }

        private void Publish(string key, string message)
        {
            messagesSubject.OnNext(new Tuple<string, string>(key, message));
        }
    }
}