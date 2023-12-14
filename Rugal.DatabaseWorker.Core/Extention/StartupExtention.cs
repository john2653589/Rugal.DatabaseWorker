using Microsoft.Extensions.DependencyInjection;
using Rugal.DatabaseWorker.Core.Service;

namespace Rugal.DatabaseWorker.Core.Extention
{
    public static class StartupExtention
    {
        public static IServiceCollection AddDatabaseWorker(this IServiceCollection Services)
        {
            Services.AddScoped<DbWorker>();
            return Services;
        }
    }
}
