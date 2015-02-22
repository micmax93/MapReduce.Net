using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using MapReduce.Api;
using MapReduce.Core;
using MapReduce.DataAccess;

namespace MapReduce.Nodes
{
    public class MapNode: Node
    {
        public MapNode(string nodeName) : base(nodeName)
        {
        }

        public void MapDone(string id)
        {
            db.CloseMap(id);
            db.CreateReduceTasks(id);
            db.Publish("new_reduce", id);
        }

        public override void OnStart()
        {
            db.Subscribe("new_map", (ch,val) => Signal());
        }

        public override void OnStop()
        {
            db.Unsubscribe("new_map");
        }

        public override bool TryExecuteTask()
        {
            bool taskAvailable = db.AssignMapTask();
            if(!taskAvailable) return false;
            Stream input = db.GetDataStream();
            IMapReduce mapReduce = db.LoadAssembly();
            Mapper mapper = new Mapper(mapReduce);
            mapper.ReadAndMap(input, db.KeyValueDataCollector);
            db.FinishMapTask(MapDone);
            return true;
        }
    }
}
