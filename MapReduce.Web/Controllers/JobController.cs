using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Web.Http;
using MapReduce.Nodes;
using MapReduce.Web.Models;

namespace MapReduce.Web.Controllers
{
    public class JobController : ApiController
    {
        private ClientNode client = new ClientNode();

        public string UserName
        {
            get { return User.Identity.Name.Split('/', '\\').Last(); }
        }

        // GET api/job
        public IEnumerable<JobStatus> Get()
        {
            var jobs = client.GetJobsInfo(UserName);
            return jobs.Select(j => new JobStatus() {JobId = j.Key, Status = j.Value});
        }

        // GET api/job/5
        public JobStatus Get(string id)
        {
            string status = client.GetJobStatus(id);
            if (status == null) return new JobStatus() {JobId = null, Status = "Job not found."};
            return new JobStatus() {JobId = id, Status = status};
        }

        // POST api/job
        public JobStatus Post([FromBody]JobRequest req)
        {
            string id;
            try
            {
                req.Validate();
                id = client.SpawnNewJob(UserName, req.JobName, req.DllPath, req.InPath, req.OutFile);
            }
            catch (Exception ex)
            {
                return new JobStatus() {JobId = null, Status = ex.Message};
            }
            return Get(id);
        }

        // DELETE api/job/5
        public void Delete(string id)
        {
            client.RemoveJob(id);
        }
    }
}
