using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.ChangeTracking;

namespace Rugal.DatabaseWorker.Core.Service
{
    public class ContextWorker<TContext, TModel> : DbWorker
        where TContext : DbContext
        where TModel : class
    {
        public DbSet<TModel> Table { get; private set; }
        public TContext Context { get; private set; }
        public ContextWorker(TContext _Context, DbSet<TModel> _Table)
        {
            Context = _Context;
            Table = _Table;
        }
        public virtual ContextWorker<TContext, TModel> Add(TModel Model)
        {
            BaseAdd(Model);
            return this;
        }
        public virtual ContextWorker<TContext, TModel> AddRange(IEnumerable<TModel> Models)
        {
            foreach (var Item in Models)
                BaseAdd(Item);
            return this;
        }
        public virtual ContextWorker<TContext, TModel> Update(TModel Model)
        {
            BaseUpdate(Model);
            return this;
        }
        public virtual ContextWorker<TContext, TModel> UpdateRange(IEnumerable<TModel> Models)
        {
            foreach (var Item in Models)
                BaseUpdate(Item);
            return this;
        }
        public virtual ContextWorker<TContext, TModel> Remove(TModel Model)
        {
            BaseRemove(Model);
            return this;
        }
        public virtual ContextWorker<TContext, TModel> Remove(IEnumerable<TModel> Models)
        {
            foreach (var Item in Models)
                BaseRemove(Item);
            return this;
        }
        public virtual ContextWorker<TContext, TModel> SaveChanges()
        {
            Context.SaveChanges();
            return this;
        }

        #region Private Process
        protected virtual void BaseAdd(TModel Model)
        {
            var Entry = Context.Entry(Model);
            RCS_SetState(Entry, EntityState.Added);
        }
        protected virtual void BaseUpdate(TModel Model)
        {
            var Entry = Context.Entry(Model);
            RCS_SetState(Entry, EntityState.Modified);
        }
        protected virtual void BaseRemove(TModel Model)
        {
            var Entry = Context.Entry(Model);
            RCS_SetState(Entry, EntityState.Deleted);
        }
        private void RCS_SetState(EntityEntry Entry, EntityState State, object ParentModel = null, int Depth = 5)
        {
            if (Depth <= 0)
                return;

            var IsCanSet = true;

            if (State == EntityState.Modified)
            {
                if (Entry.State != EntityState.Detached)
                    IsCanSet = false;
            }

            if (State == EntityState.Deleted)
            {
                if (Entry.State == EntityState.Added)
                    State = EntityState.Detached;
            }

            foreach (var Datas in Entry.Collections)
            {
                foreach (var Data in Datas.CurrentValue)
                {
                    var DataEntry = Context.Entry(Data);
                    RCS_SetState(DataEntry, State, Entry.Entity, Depth - 1);
                }
            }

            foreach (var Model in Entry.References)
            {
                if (Model.CurrentValue is not null)
                {
                    var IsParent = Model.CurrentValue == ParentModel;
                    if (IsParent)
                        continue;
                    var DataEntry = Model.TargetEntry;
                    RCS_SetState(DataEntry, State, Entry.Entity, Depth - 1);
                }
            }

            if (IsCanSet)
                Entry.State = State;
        }
        #endregion
    }
}