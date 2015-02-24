using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text;
using MapReduce.Api;

namespace MapReduce.DataAccess
{
    public partial class RedisDb
    {
        public string JobId
        {
            get { return HashGet("node_" + node, "id"); }
            set { HashSet("node_" + node, "id", value); }
        }

        public byte[] CurrentTask
        {
            get { return HashGetBytes("node_" + node, "task"); }
            set { HashSet("node_" + node, "task", value); }
        }

        public string JobAssembly
        {
            get
            {
                string id = JobId;
                if (id == null) return null;
                return HashGet("job_" + id, "assembly");
            }
            set
            {
                string id = JobId;
                if (id == null) return;
                HashSet("job_" + id, "assembly", value);
            }
        }

        public string JobStatus
        {
            get
            {
                string id = JobId;
                if (id == null) return null;
                return HashGet("job_" + id, "status");
            }
            set
            {
                string id = JobId;
                if (id == null) return;
                HashSet("job_" + id, "status", value);
            }
        }

        public IMapReduce LoadAssembly(string typeName = null)
        {
            string assemblyFile = JobAssembly;
            Assembly dll = Assembly.Load(File.ReadAllBytes(assemblyFile));
            if (typeName == null)
            {
                typeName = Path.GetFileNameWithoutExtension(assemblyFile);
            }
            Type type = dll.GetType(typeName);
            var method = type.GetConstructor(new Type[] { });
            return (IMapReduce)method.Invoke(new object[] { });
        }

        public string JobOutFile
        {
            get
            {
                string id = JobId;
                if (id == null) return null;
                return HashGet("job_" + id, "outfile");
            }
            set
            {
                string id = JobId;
                if (id == null) return;
                HashSet("job_" + id, "outfile", value);
            }
        }

        public string CreateJob(string user, string jobName, string assemblyFile, string outPath)
        {
            string id = user + "_" + jobName;
            if(GetJobStatus(id)!=null) throw new DuplicateNameException();
            HashSet("job_" + id, "status", "new");
            HashSet("job_" + id, "assembly", assemblyFile);
            HashSet("job_" + id, "outfile", outPath);
            return id;
        }

        public void RemoveJob(string id)
        {
            db.KeyDelete("job_" + id);
        }

        public IEnumerable<string> ListJobs(string user)
        {
            var jobs = server.Keys(pattern: "job_" + user + "_*").Select(k => (string)k);
            return jobs.Select(j => j.Replace("job_", ""));
        }

        public string GetJobStatus(string id)
        {
            return HashGet("job_" + id, "status");
        }
    }
}
