using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.ChangeTracking;
using System.Linq.Expressions;

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
        public virtual ContextWorker<TContext, TTable> Add<TTable>(Func<TContext, DbSet<TTable>> TableFunc, TTable Model)
            where TTable : class
        {
            var NewWorker = AsContextWorker(Context, TableFunc)
                .Add(Model);
            return NewWorker;
        }
        public virtual ContextWorker<TContext, TModel> AddRange(IEnumerable<TModel> Models)
        {
            foreach (var Item in Models)
                BaseAdd(Item);
            return this;
        }
        public virtual ContextWorker<TContext, TTable> AddRange<TTable>(Func<TContext, DbSet<TTable>> TableFunc, IEnumerable<TTable> Models)
            where TTable : class
        {
            var NewWorker = AsContextWorker(Context, TableFunc)
                .AddRange(Models);
            return NewWorker;
        }
        public virtual ContextWorker<TContext, TModel> Update(Expression<Func<TModel>> NewModelExp)
        {
            BaseUpdate(NewModelExp);
            return this;
        }
        public virtual ContextWorker<TContext, TTable> Update<TTable>(Func<TContext, DbSet<TTable>> TableFunc,
            Expression<Func<TTable>> NewModelExp)
            where TTable : class
        {
            var NewWorker = AsContextWorker(Context, TableFunc)
                .Update(NewModelExp);
            return NewWorker;
        }
        public virtual ContextWorker<TContext, TModel> UpdateRange(IEnumerable<TModel> Datas,
            Expression<Func<TModel, TModel>> NewModelExp)
        {
            BaseUpdateRange(Datas, NewModelExp);
            return this;
        }
        public virtual ContextWorker<TContext, TTable> UpdateRange<TTable>(Func<TContext, DbSet<TTable>> TableFunc,
            IEnumerable<TTable> Datas,
            Expression<Func<TTable, TTable>> NewModelExp)
            where TTable : class
        {
            var NewWorker = AsContextWorker(Context, TableFunc)
                .UpdateRange(Datas, NewModelExp);
            return NewWorker;
        }
        public virtual ContextWorker<TContext, TModel> Remove(TModel Model)
        {
            BaseRemove(Model);
            return this;
        }
        public virtual ContextWorker<TContext, TTable> Remove<TTable>(Func<TContext, DbSet<TTable>> TableFunc, TTable Model)
            where TTable : class
        {
            var NewWorker = AsContextWorker(Context, TableFunc)
                .Remove(Model);
            return NewWorker;
        }
        public virtual ContextWorker<TContext, TModel> RemoveRange(IEnumerable<TModel> Models)
        {
            foreach (var Item in Models)
                BaseRemove(Item);
            return this;
        }
        public virtual ContextWorker<TContext, TTable> RemoveRange<TTable>(Func<TContext, DbSet<TTable>> TableFunc, IEnumerable<TTable> Models)
            where TTable : class
        {
            var NewWorker = AsContextWorker(Context, TableFunc)
                .RemoveRange(Models);
            return NewWorker;
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
        protected virtual void BaseUpdate(Expression<Func<TModel>> NewModelExp)
        {
            if (NewModelExp.Body is not MemberInitExpression MemberInitExp)
                throw new Exception("Update method only allow [Init] expression type");

            var NewModel = NewModelExp.Compile().Invoke();
            SetUpdateEntry(NewModel, MemberInitExp);
        }
        protected virtual void BaseUpdateRange(IEnumerable<TModel> Datas, Expression<Func<TModel, TModel>> NewModelExp)
        {
            CheckInitMemberExpression(NewModelExp.Body, out var MemberInitExp);

            var NewModelFunc = NewModelExp.Compile();
            foreach (var Data in Datas)
            {
                var NewModel = NewModelFunc.Invoke(Data);
                SetUpdateEntry(NewModel, MemberInitExp);
            }
        }
        private static void CheckInitMemberExpression(Expression Exp, out MemberInitExpression MemberInitExp)
        {
            if (Exp is not MemberInitExpression GetMemberInitExp)
                throw new Exception("Update method only allow [Init] expression type");

            MemberInitExp = GetMemberInitExp;
        }
        private void SetUpdateEntry(TModel NewModel, MemberInitExpression MemberInitExp)
        {
            var Entry = Context.Entry(NewModel);
            var UpdateProperty = MemberInitExp
                .Bindings
                .Select(Item => Item.Member.Name);

            foreach (var Property in Entry.Properties)
            {
                if (Property.Metadata.IsPrimaryKey())
                    continue;

                var PropertyName = Property.Metadata.Name;
                Property.IsModified = UpdateProperty.Contains(PropertyName);
            }

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