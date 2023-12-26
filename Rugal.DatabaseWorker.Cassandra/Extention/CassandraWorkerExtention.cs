using Cassandra;
using Rugal.DatabaseWorker.Cassandra.Service;
using Rugal.DatabaseWorker.Core.Service;
namespace Rugal.DatabaseWorker.Cassandra.Extention
{
    public static class CassandraWorkerExtention
    {
        public static CassandraWorker AsCassandraWorker(this DbWorker _, ISession Database)
        {
            var NewWorker = new CassandraWorker(Database);
            return NewWorker;
        }
        public static CassandraWorker AsCassandraWorker(this ISession Database)
        {
            var NewWorker = new CassandraWorker(Database);
            return NewWorker;
        }
    }
}