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
    public class ReduceNode : Node
    {
        public ReduceNode(string nodeName) : base(nodeName)
        {
        }

        public void ReduceDone(string id)
        {
            db.CloseReduce(id);
            db.JobStatus = "saving";
            string path = db.JobOutFile;
            FileStream fs = new FileStream(path, FileMode.Create);
            var data = db.GetOutData(id);
            new Reducer(db.LoadAssembly()).Write(fs, data);
            db.JobId = "done";
        }

        public override void OnStart()
        {
            db.Subscribe("new_reduce", (ch, val) => Signal());
        }

        public override void OnStop()
        {
            db.Unsubscribe("new_reduce");
        }

        public override bool TryExecuteTask()
        {
            bool taskAvailable = db.AssignReduceTask();
            if (!taskAvailable) return false;
            var task = db.GetReduceTask();
            IMapReduce mapReduce = db.LoadAssembly();
            Reducer reducer = new Reducer(mapReduce);
            var result = reducer.Reduce(task.Key, task.Value);
            db.FinishReduceTask(result, ReduceDone);
            return true;
        }
    }
}
