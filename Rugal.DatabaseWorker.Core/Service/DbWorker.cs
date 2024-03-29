﻿using Microsoft.EntityFrameworkCore;

namespace Rugal.DatabaseWorker.Core.Service
{
    public class DbWorker
    {
        public virtual ContextWorker<TContext, TModel> AsContextWorker<TContext, TModel>(TContext Context, Func<TContext, DbSet<TModel>> TableFunc)
            where TModel : class
            where TContext : DbContext
        {
            var NewWorker = new ContextWorker<TContext, TModel>(Context, TableFunc(Context));
            return NewWorker;
        }
        public virtual ContextWorker<TContext, TModel> Add<TContext, TModel>(TContext Context, Func<TContext, DbSet<TModel>> TableFunc, TModel Model)
            where TContext : DbContext
            where TModel : class
        {
            var NewWorker = AsContextWorker(Context, TableFunc)
                .Add(Model);
            return NewWorker;
        }
        public virtual ContextWorker<TContext, TModel> Update<TContext, TModel>(TContext Context, Func<TContext, DbSet<TModel>> TableFunc, TModel Model)
            where TContext : DbContext
            where TModel : class
        {
            var NewWorker = AsContextWorker(Context, TableFunc)
                .Update(Model);
            return NewWorker;
        }
        public virtual ContextWorker<TContext, TModel> Remove<TContext, TModel>(TContext Context, Func<TContext, DbSet<TModel>> TableFunc, TModel Model)
            where TContext : DbContext
            where TModel : class
        {
            var NewWorker = AsContextWorker(Context, TableFunc)
                .Remove(Model);
            return NewWorker;
        }
        public virtual ContextWorker<TContext, TModel> SaveChanges<TContext, TModel>(TContext Context, Func<TContext, DbSet<TModel>> TableFunc)
            where TContext : DbContext
            where TModel : class
        {
            var NewWorker = AsContextWorker(Context, TableFunc)
                .SaveChanges();
            return NewWorker;
        }
    }
}