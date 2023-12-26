using MongoDB.Driver;
using Rugal.DatabaseWorker.Core.Service;
using Rugal.DatabaseWorker.Mongo.Service;

namespace Rugal.DatabaseWorker.Mongo.Extention
{
    public static class MongoWorkerExtention
    {
        public static MongoWorker AsMongoWorker(this DbWorker _, IMongoDatabase Database)
        {
            var NewWorker = new MongoWorker(Database);
            return NewWorker;
        }
        public static MongoWorker AsMongoWorker(this IMongoDatabase Database)
        {
            var NewWorker = new MongoWorker(Database);
            return NewWorker;
        }
    }
}
