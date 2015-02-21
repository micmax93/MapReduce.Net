using System;
using System.Collections.Generic;
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
            get { return (string)HashGet("node_" + node, "id"); }
            set { HashSet("node_" + node, "id", value); }
        }
        public byte[] CurrentTask
        {
            get { return (byte[])HashGet("node_" + node, "task"); }
            set { HashSet("node_" + node, "task", value); }
        }
        public string JobAssembly
        {
            get
            {
                string id = JobId;
                if (id == null) return null;
                return (string)HashGet("job_" + id, "assembly");
            }
            set
            {
                string id = JobId;
                if (id == null) return;
                HashSet("job_" + id, "assembly", value);
            }
        }

        public IMapReduce LoadAssembly(string typeName = null)
        {
            string assemblyFile = JobAssembly;
            Assembly dll = Assembly.LoadFile(assemblyFile);
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
                return (string)HashGet("job_" + id, "outfile");
            }
            set
            {
                string id = JobId;
                if (id == null) return;
                HashSet("job_" + id, "outfile", value);
            }
        }

        public void CreateJob(string id, string assemblyFile, string outPath)
        {
            JobId = id;
            JobAssembly = assemblyFile;
            JobOutFile = outPath;
        }
    }
}
