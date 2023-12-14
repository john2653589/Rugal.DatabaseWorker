using Microsoft.Extensions.DependencyInjection;
using MongoDB.Driver;
using Rugal.DatabaseWorker.Core.Extention;
using Rugal.DatabaseWorker.Mongo.Service;

namespace Rugal.DatabaseWorker.Mongo.Extention
{
    public static class StartupExtention
    {
        public static IServiceCollection AddMongoWorker(this IServiceCollection Services)
        {
            Services.AddDatabaseWorker();
            Services.AddScoped(Provider =>
            {
                var Database = Provider.GetService<IMongoDatabase>();
                var Worker = new MongoWorker(Database);
                return Worker;
            });
            return Services;
        }
    }
}