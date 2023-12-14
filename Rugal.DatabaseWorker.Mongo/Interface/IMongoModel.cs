using MongoDB.Bson;

namespace Rugal.DatabaseWorker.Mongo.Interface
{
    public interface IMongoModel
    {
        public ObjectId _id { get; set; }
        public void GenerateIdForEmpty();
    }
}
