using Microsoft.EntityFrameworkCore;
using System.Linq.Expressions;

namespace Rugal.DatabaseWorker.Core.Service
{
    public class DbWorker
    {
        public virtual ContextWorker<TContext, TModel> AsContextWorker<TContext, TModel>(
            TContext Context,
            Func<TContext, DbSet<TModel>> TableFunc)
            where TModel : class
            where TContext : DbContext
        {
            var NewWorker = new ContextWorker<TContext, TModel>(Context, TableFunc(Context));
            return NewWorker;
        }
        public virtual ContextWorker<TContext, TModel> Add<TContext, TModel>(
            TContext Context,
            Func<TContext, DbSet<TModel>> TableFunc, TModel Datas)
            where TContext : DbContext
            where TModel : class
        {
            var NewWorker = AsContextWorker(Context, TableFunc)
                .Add(Datas);
            return NewWorker;
        }
        public virtual ContextWorker<TContext, TModel> AddRange<TContext, TModel>(
            TContext Context,
            Func<TContext, DbSet<TModel>> TableFunc, IEnumerable<TModel> Datas)
            where TContext : DbContext
            where TModel : class
        {
            var NewWorker = AsContextWorker(Context, TableFunc)
                .AddRange(Datas);
            return NewWorker;
        }

        public virtual ContextWorker<TContext, TModel> Update<TContext, TModel>(TContext Context, Func<TContext, DbSet<TModel>> TableFunc,
           Expression<Func<TModel>> NewModelExp)
           where TContext : DbContext
           where TModel : class
        {
            var NewWorker = AsContextWorker(Context, TableFunc)
                .Update(NewModelExp);
            return NewWorker;
        }
        public virtual ContextWorker<TContext, TModel> UpdateRange<TContext, TModel>(TContext Context, Func<TContext, DbSet<TModel>> TableFunc,
            IEnumerable<TModel> Datas,
            Expression<Func<TModel, TModel>> NewModelExp)
            where TContext : DbContext
            where TModel : class
        {
            var NewWorker = AsContextWorker(Context, TableFunc)
                .UpdateRange(Datas, NewModelExp);
            return NewWorker;
        }

        public virtual ContextWorker<TContext, TModel> Remove<TContext, TModel>(
            TContext Context,
            Func<TContext, DbSet<TModel>> TableFunc, TModel Model)
            where TContext : DbContext
            where TModel : class
        {
            var NewWorker = AsContextWorker(Context, TableFunc)
                .Remove(Model);
            return NewWorker;
        }
        public virtual ContextWorker<TContext, TModel> RemoveRange<TContext, TModel>(
            TContext Context,
            Func<TContext, DbSet<TModel>> TableFunc, IEnumerable<TModel> Datas)
            where TContext : DbContext
            where TModel : class
        {
            var NewWorker = AsContextWorker(Context, TableFunc)
                .RemoveRange(Datas);
            return NewWorker;
        }

        public virtual ContextWorker<TContext, TModel> SaveChanges<TContext, TModel>(
            TContext Context,
            Func<TContext, DbSet<TModel>> TableFunc)
            where TContext : DbContext
            where TModel : class
        {
            var NewWorker = AsContextWorker(Context, TableFunc)
                .SaveChanges();
            return NewWorker;
        }
    }
}