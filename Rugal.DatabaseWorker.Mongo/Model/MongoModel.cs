using MongoDB.Bson;
using Rugal.DatabaseWorker.Mongo.Interface;

namespace Rugal.DatabaseWorker.Mongo.Model
{
    public class MongoModel : IMongoModel
    {
        public virtual ObjectId _id { set; get; } = ObjectId.GenerateNewId();
        public virtual void GenerateIdForEmpty()
        {
            if (_id != ObjectId.Empty)
                return;

            _id = ObjectId.GenerateNewId();
        }
    }
}
