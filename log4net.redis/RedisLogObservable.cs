using System;
using System.Reactive.Concurrency;
using System.Reactive.Disposables;
using System.Reactive.Linq;
using System.Text;
using BookSleeve;

namespace log4net.redis
{
    public static class RedisLogObservable
    {
        private static IObservable<string> CreateObservable(string key, IScheduler scheduler)
        {
            IObservable<string> observable = Observable.Defer(() => Observable.Create<string>(observer =>
                {
                    var connectionFactory = new RedisConnectionFactory();

                    return connectionFactory.CreateRedisConnection().ContinueWith(task =>
                        {
                            if (task.IsFaulted)
                            {
                                observer.OnError(task.Exception);
                                return Disposable.Empty;
                            }

                            return scheduler.Schedule(() =>
                                {
                                    RedisSubscriberConnection connection =
                                        task.Result.GetOpenSubscriberChannel();
                                    task.Result.Error += (_, err) => observer.OnError(err.Exception);

                                    connection.Subscribe(key, (header, content) =>
                                        {
                                            if (header == key)
                                                observer.OnNext(Encoding.UTF8.GetString(content));
                                        });
                                });
                        });
                }));

            return observable;
        }

        public static IObservable<string> CreateObservableWithRetry(string key, IScheduler scheduler)
        {
            return Observable.Create<string>(observer =>
                {
                    return scheduler.Schedule(self =>
                        {
                            IObservable<string> observable = CreateObservable(key, scheduler);
                            IDisposable disposable = observable.Subscribe(
                                message => { observer.OnNext(message); },
                                error => { scheduler.Schedule(TimeSpan.FromSeconds(10), self); });
                        });
                });
        }
    }
}