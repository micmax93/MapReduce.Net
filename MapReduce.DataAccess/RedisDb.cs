using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Runtime.Serialization;
using System.Runtime.Serialization.Formatters.Binary;
using System.Text;
using System.Xml.Serialization;
using MapReduce.Api;
using StackExchange.Redis;
using StackExchange.Redis.KeyspaceIsolation;

namespace MapReduce.DataAccess
{
    public partial class RedisDb
    {
        public delegate void CompletionTrigger(string id);
        protected IDatabase db;
        protected IServer server;
        protected string node;
        protected ISubscriber subscriber;
        public RedisDb(string host, string nodeName)
        {
            var conn = ConnectionMultiplexer.Connect(host + ",allowAdmin=true");
            db = conn.GetDatabase();
            server = conn.GetServer(host);
            node = nodeName;
            subscriber = conn.GetSubscriber();
        }

        private object HashGet(RedisKey key, RedisValue field)
        {
            var val = db.HashGet(key, field);
            if (!val.HasValue) return null;
            return val;
        }
        private void HashSet(RedisKey key, RedisValue field, object value)
        {
            if (value == null) db.HashDelete(key, field);
            else db.HashSet(key, field, (RedisValue)value);
        }

        public void Subscribe(string channel, Action<string, string> action)
        {
            subscriber.Subscribe(channel, (ch, val) => action(ch, val));
        }

        public void Unsubscribe(string channel)
        {
            subscriber.Unsubscribe(channel);
        }

        public void Publish(string channel, string message)
        {
            subscriber.Publish(channel, message);
        }
    }
}
