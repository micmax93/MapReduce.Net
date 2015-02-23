using System;
using System.Collections.Generic;
using System.Configuration;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using MapReduce.DataAccess;

namespace MapReduce.Nodes
{
    public class ClientNode
    {
        RedisDb db;
        public ClientNode(string nodeName="client")
        {
            var host = ConfigurationManager.ConnectionStrings["redisDb"].ConnectionString;
            db = new RedisDb(host, nodeName);
        }
        public string SpawnNewJob(string user, string jobName, string assemblyFile, string inputPath, string outputFile)
        {
            
            if (Directory.Exists(inputPath))
            {
                return SpawnNewJob(user, jobName, assemblyFile, Directory.GetFiles(inputPath), outputFile);
            }
            else
            {
                return SpawnNewJob(user, jobName, assemblyFile, new[] {inputPath}, outputFile);
            }
        }
        public string SpawnNewJob(string user, string jobName, string assemblyFile, IEnumerable<string> inputFiles, string outputFile)
        {
            string id = db.CreateJob(user, jobName, assemblyFile, outputFile);
            db.CreateMapTasks(id, inputFiles);
            return id;
        }

        public IEnumerable<string> ListJobs(string user)
        {
            return db.ListJobs(user);
        }

        public string GetJobStatus(string id)
        {
            return db.GetJobStatus(id);
        }
        public IEnumerable<KeyValuePair<string, string>> GetJobsInfo(string user)
        {
            return db.ListJobs(user).Select(j => new KeyValuePair<string, string>(j, db.GetJobStatus(j)));
        }

        public void RemoveJob(string id)
        {
            db.RemoveJob(id);
        }
    }
}
