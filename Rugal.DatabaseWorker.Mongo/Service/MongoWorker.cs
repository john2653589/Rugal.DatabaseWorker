using MongoDB.Driver;
using Rugal.DatabaseWorker.Core.Service;
using Rugal.DatabaseWorker.Mongo.Interface;
using System.Linq.Expressions;

namespace Rugal.DatabaseWorker.Mongo.Service
{
    public class MongoWorker : DbWorker
    {
        public IMongoDatabase Database { get; private set; }
        public MongoWorker(IMongoDatabase _Mongo)
        {
            Database = _Mongo;
        }

        public MongoCollectWorker<TModel> Add<TModel>(string CollectName, TModel Model)
            where TModel : class, IMongoModel
        {
            var NewWorker = NewCollectWorker<TModel>(CollectName);
            NewWorker.Add(Model);
            return NewWorker;
        }
        public MongoCollectWorker<TModel> AddRange<TModel>(string CollectName, IEnumerable<TModel> Models)
            where TModel : class, IMongoModel
        {
            var NewWorker = NewCollectWorker<TModel>(CollectName);
            NewWorker.AddRange(Models);
            return NewWorker;
        }

        public MongoCollectWorker<TModel> Update<TModel>(string CollectName, TModel Model)
            where TModel : class, IMongoModel
        {
            var NewWorker = NewCollectWorker<TModel>(CollectName);
            NewWorker.Update(Model);
            return NewWorker;
        }
        public MongoCollectWorker<TModel> UpdateRange<TModel>(string CollectName, IEnumerable<TModel> Models)
            where TModel : class, IMongoModel
        {
            var NewWorker = NewCollectWorker<TModel>(CollectName);
            NewWorker.UpdateRange(Models);
            return NewWorker;
        }

        public MongoCollectWorker<TModel> Update<TModel, TValue>(string CollectName, TModel Model, Expression<Func<TModel, TValue>> UpdateExp)
            where TModel : class, IMongoModel
        {
            var NewWorker = NewCollectWorker<TModel>(CollectName);
            NewWorker.Update(Model, UpdateExp);
            return NewWorker;
        }
        public MongoCollectWorker<TModel> UpdateRange<TModel, TValue>(string CollectName, IEnumerable<TModel> Models, Expression<Func<TModel, TValue>> UpdateExp)
            where TModel : class, IMongoModel
        {
            var NewWorker = NewCollectWorker<TModel>(CollectName);
            NewWorker.UpdateRange(Models, UpdateExp);
            return NewWorker;
        }

        public MongoCollectWorker<TModel> Remove<TModel>(string CollectName, TModel Model)
            where TModel : class, IMongoModel
        {
            var NewWorker = NewCollectWorker<TModel>(CollectName);
            NewWorker.Remove(Model);
            return NewWorker;
        }
        public MongoCollectWorker<TModel> RemoveRange<TModel>(string CollectName, IEnumerable<TModel> Models)
            where TModel : class, IMongoModel
        {
            var NewWorker = NewCollectWorker<TModel>(CollectName);
            NewWorker.RemoveRange(Models);
            return NewWorker;
        }
        public MongoCollectWorker<TModel> SaveChanges<TModel>(string CollectName)
            where TModel : class, IMongoModel
        {
            var NewWorker = NewCollectWorker<TModel>(CollectName);
            NewWorker.SaveChanges();
            return NewWorker;
        }

        public static MongoWorker AsMongoWorker(IMongoDatabase Database)
        {
            var NewWorker = new MongoWorker(Database);
            return NewWorker;
        }
        public MongoCollectWorker<TModel> AsCollectWorker<TModel>(string CollectName)
            where TModel : class, IMongoModel
        {
            var NewWorker = NewCollectWorker<TModel>(CollectName);
            return NewWorker;
        }
        private MongoCollectWorker<TModel> NewCollectWorker<TModel>(string CollectName)
            where TModel : class, IMongoModel
        {
            var NewWorker = new MongoCollectWorker<TModel>(Database, CollectName);
            return NewWorker;
        }
    }
    public class MongoCollectWorker<TModel> : MongoWorker
        where TModel : class, IMongoModel
    {
        public string CollectName { get; private set; }
        public IMongoCollection<TModel> Collect => GetCollection();
        private List<WriteModel<TModel>> Commands { get; set; }
        public MongoCollectWorker(IMongoDatabase _Database, string _CollectName) : base(_Database)
        {
            CollectName = _CollectName;
            Commands = [];
        }
        public MongoCollectWorker<TModel> Add(TModel Model)
        {
            BaseAdd(Model);
            return this;
        }
        public MongoCollectWorker<TModel> AddRange(IEnumerable<TModel> Models)
        {
            foreach (var Item in Models)
                BaseAdd(Item);
            return this;
        }
        public MongoCollectWorker<TModel> Update(TModel Model)
        {
            BaseReplace(Model);
            return this;
        }
        public MongoCollectWorker<TModel> UpdateRange(IEnumerable<TModel> Models)
        {
            foreach (var Item in Models)
                BaseReplace(Item);
            return this;
        }
        public MongoCollectWorker<TModel> Update<TValue>(TModel Model, Expression<Func<TModel, TValue>> UpdateFunc)
        {
            BaseUpdate(Model, UpdateFunc);
            return this;
        }
        public MongoCollectWorker<TModel> UpdateRange<TValue>(IEnumerable<TModel> Models, Expression<Func<TModel, TValue>> UpdateFunc)
        {
            foreach (var Item in Models)
                BaseUpdate(Item, UpdateFunc);
            return this;
        }
        public MongoCollectWorker<TModel> Remove(TModel Model)
        {
            BaseRemove(Model);
            return this;
        }
        public MongoCollectWorker<TModel> RemoveRange(IEnumerable<TModel> Models)
        {
            foreach (var Item in Models)
                BaseRemove(Item);
            return this;
        }
        public MongoCollectWorker<TModel> SaveChanges()
        {
            using var Session = Database.Client.StartSession();
            try
            {
                Collect.BulkWrite(Session, Commands);
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.ToString());
                throw new Exception(ex.ToString());
            }
            return this;
        }

        private void BaseAdd(TModel Model)
        {
            var InsertCommand = CreateInsertOne(Model);
            Commands.Add(InsertCommand);
        }
        private void BaseReplace(TModel Model)
        {
            var ReplaceCommand = CreateReplaceOne(Model);
            Commands.Add(ReplaceCommand);
        }
        private void BaseUpdate<TValue>(TModel Model, Expression<Func<TModel, TValue>> UpdateExp)
        {
            var UpdateFunc = UpdateExp.Compile();
            var UpdateCommand = CreateUpdateOne(Model, Update =>
            {
                var SetCommand = Update.Set(UpdateExp, UpdateFunc(Model));
                return SetCommand;
            });
            Commands.Add(UpdateCommand);
        }
        private void BaseRemove(TModel Model)
        {
            var DeleteCommand = CreateDeleteOne(Model);
            Commands.Add(DeleteCommand);
        }

        private static FilterDefinition<TModel> Build_IdFilter(TModel Model)
        {
            var Result = Build_Filter(Filter => Filter.Eq(Item => Item._id, Model._id));
            return Result;
        }
        private static FilterDefinition<TModel> Build_Filter(Func<FilterDefinitionBuilder<TModel>, FilterDefinition<TModel>> FilterFunc)
        {
            var Filter = Builders<TModel>.Filter;
            var Result = FilterFunc.Invoke(Filter);
            return Result;
        }
        private static UpdateDefinition<TModel> Build_Update(Func<UpdateDefinitionBuilder<TModel>, UpdateDefinition<TModel>> UpdateFunc)
        {
            var Update = Builders<TModel>.Update;
            var Result = UpdateFunc.Invoke(Update);
            return Result;
        }

        private static InsertOneModel<TModel> CreateInsertOne(TModel Model)
        {
            Model.GenerateIdForEmpty();
            var Result = new InsertOneModel<TModel>(Model);
            return Result;
        }
        private static ReplaceOneModel<TModel> CreateReplaceOne(TModel Model)
        {
            var Filter = Build_IdFilter(Model);
            var Result = new ReplaceOneModel<TModel>(Filter, Model)
            {
                IsUpsert = true,
            };
            return Result;
        }
        private static UpdateOneModel<TModel> CreateUpdateOne(TModel Model, Func<UpdateDefinitionBuilder<TModel>, UpdateDefinition<TModel>> UpdateFunc)
        {
            var Filter = Build_IdFilter(Model);
            var Update = Build_Update(UpdateFunc);
            var Result = new UpdateOneModel<TModel>(Filter, Update);
            return Result;
        }
        private static DeleteOneModel<TModel> CreateDeleteOne(TModel Model)
        {
            var Filter = Build_IdFilter(Model);
            var Result = new DeleteOneModel<TModel>(Filter);
            return Result;
        }

        private IMongoCollection<TModel> GetCollection()
        {
            var Result = Database.GetCollection<TModel>(CollectName);
            return Result;
        }
    }
}