using Cassandra;
using Cassandra.Data.Linq;
using Cassandra.Mapping;
using System.Linq.Expressions;
using System.Reflection;

namespace Rugal.DatabaseWorker.Cassandra.Service
{
    public class CassandraWorker
    {
        public ISession Session { get; private set; }
        public CassandraWorker(ISession _Session)
        {
            Session = _Session;
        }
        public virtual CassandraTableWorker<TModel> WithTable<TModel>(string TableName)
        {
            var NewTableWorker = new CassandraTableWorker<TModel>(Session, TableName);
            return NewTableWorker;
        }
        public virtual CassandraModifyWorker<TModel> WithModify<TModel>(string TableName)
        {
            var NewModifyWorker = new CassandraModifyWorker<TModel>(Session, TableName);
            return NewModifyWorker;
        }
        public virtual CassandraQueryWorker<TModel> WithQuery<TModel>(string TableName)
        {
            var NewQueryWorker = new CassandraQueryWorker<TModel>(Session, TableName);
            return NewQueryWorker;
        }
    }
    public class CassandraTableWorker<TModel> : CassandraWorker
    {
        public string TableName { get; private set; }
        public MappingConfiguration TableConfig { get; private set; }
        public Table<TModel> Table { get; private set; }
        public CassandraTableWorker(ISession _Session, string _TableName) : base(_Session)
        {
            TableName = _TableName;
            TableConfig = new MappingConfiguration();
            NewTable();
        }

        #region With Method
        public CassandraTableWorker<TModel> WithTableName(string _TableName)
        {
            TableName = _TableName;
            NewTable();
            return this;
        }
        public CassandraTableWorker<TModel> WithTableConfig(MappingConfiguration _TableConfig)
        {
            TableConfig = _TableConfig;
            NewTable();
            return this;
        }
        public CassandraModifyWorker<TModel> WithModify()
        {
            return base.WithModify<TModel>(TableName);
        }
        public CassandraQueryWorker<TModel> WithQuery()
        {
            return base.WithQuery<TModel>(TableName);
        }
        #endregion

        #region Public Method
        public CassandraQueryWorker<TResult> Select<TResult>(Expression<Func<TModel, TResult>> SelectExp)
        {
            var NewQueryWorker = NewQueryWorker<TResult>();
            NewQueryWorker.Query = Table.Select(SelectExp);
            return NewQueryWorker;
        }
        public virtual TModel[] ToArray()
        {
            var NewQueryWorker = NewQueryWorker<TModel>();
            var Result = NewQueryWorker.ToArray();
            return Result;
        }
        public virtual List<TModel> ToList()
        {
            var NewQueryWorker = NewQueryWorker<TModel>();
            var Result = NewQueryWorker.ToList();
            return Result;
        }
        public virtual IEnumerable<TModel> AsEnumerable()
        {
            var NewQueryWorker = NewQueryWorker<TModel>();
            var Result = NewQueryWorker.AsEnumerable();
            return Result;
        }
        public void CheckTableGenrate()
        {
            Table.CreateIfNotExists();
        }
        #endregion

        #region Protected Process
        protected CassandraQueryWorker<TResult> NewQueryWorker<TResult>()
        {
            var NewWorker = new CassandraQueryWorker<TResult>(Session, TableName);
            return NewWorker;
        }
        protected CassandraTableWorker<TModel> NewTable()
        {
            Table = new Table<TModel>(Session, new MappingConfiguration(), TableName);
            return this;
        }
        protected CassandraTableWorker<TResult> NewTableWorker<TResult>()
        {
            var NewWorker = new CassandraTableWorker<TResult>(Session, TableName);
            return NewWorker;
        }
        #endregion
    }
    public class CassandraModifyWorker<TModel> : CassandraTableWorker<TModel>
    {
        private List<CqlCommand> ModifyCommands { get; set; }
        private string PkName { get; set; }
        public CassandraModifyWorker(ISession _Session, string _TableName) : base(_Session, _TableName)
        {
            ModifyCommands = [];
            TryGetPkName();
        }

        #region Public Method
        public CassandraModifyWorker<TModel> Add(TModel Model)
        {
            var InsertCommand = Table.Insert(Model);
            ModifyCommands.Add(InsertCommand);
            return this;
        }
        public CassandraModifyWorker<TModel> AddRange(IEnumerable<TModel> Models)
        {
            foreach (var Item in Models)
                Add(Item);
            return this;
        }
        public CassandraModifyWorker<TModel> Update<TValue>(TModel Model, Expression<Func<TModel, TValue>> PkExp)
        {
            var PkMember = PkExp.Body as MemberExpression; //取得陳述式成員
            var PkName = PkMember.Member.Name; //取得主Key名稱
            var PkFunc = PkExp.Compile(); //將陳述式轉換為Function

            //建立 Where 陳述式
            var WhereExp = CreateWherePkExpression(Model, PkName, PkFunc);

            //建立 建構子陳述式
            var SelectInitExp = CreateSelectInitExpression(Model, PkName);

            var UpdateCommand = Table
                .Where(WhereExp)
                .Select(SelectInitExp)
                .Update();

            ModifyCommands.Add(UpdateCommand);
            return this;
        }
        public CassandraModifyWorker<TModel> UpdateRange<TValue>(IEnumerable<TModel> Models, Expression<Func<TModel, TValue>> PkExp)
        {
            foreach (var Item in Models)
                Update(Item, PkExp);
            return this;
        }
        public CassandraModifyWorker<TModel> Update(TModel Model)
        {
            var PkValueFunc = DefaultPkValueFunc();

            //建立 Where 陳述式
            var WhereExp = CreateWherePkExpression(Model, PkName, PkValueFunc);

            //建立 建構子陳述式
            var SelectInitExp = CreateSelectInitExpression(Model, PkName);

            var UpdateCommand = Table
                .Where(WhereExp)
                .Select(SelectInitExp)
                .Update();

            ModifyCommands.Add(UpdateCommand);
            return this;
        }
        public CassandraModifyWorker<TModel> UpdateRange(IEnumerable<TModel> Models)
        {
            foreach (var Item in Models)
                Update(Item);
            return this;
        }
        public CassandraModifyWorker<TModel> Remove<TValue>(TModel Model, Expression<Func<TModel, TValue>> PkExp)
        {
            var PkMember = PkExp.Body as MemberExpression; //取得陳述式成員
            var PkName = PkMember.Member.Name; //取得主Key名稱
            var PkFunc = PkExp.Compile(); //將陳述式轉換為Function

            //建立 Where 陳述式
            var WhereExp = CreateWherePkExpression(Model, PkName, PkFunc);

            var DeleteCommand = Table
                .Where(WhereExp)
                .Delete();

            ModifyCommands.Add(DeleteCommand);
            return this;
        }
        public CassandraModifyWorker<TModel> RemoveRange<TValue>(IEnumerable<TModel> Models, Expression<Func<TModel, TValue>> PkExp)
        {
            foreach (var Item in Models)
                Remove(Item, PkExp);
            return this;
        }
        public CassandraModifyWorker<TModel> Remove(TModel Model)
        {
            var PkValueFunc = DefaultPkValueFunc();
            //建立 Where 陳述式
            var WhereExp = CreateWherePkExpression(Model, PkName, PkValueFunc);

            var DeleteCommand = Table
                .Where(WhereExp)
                .Delete();

            ModifyCommands.Add(DeleteCommand);
            return this;
        }
        public CassandraModifyWorker<TModel> RemoveRange(IEnumerable<TModel> Models)
        {
            foreach (var Item in Models)
                Remove(Item);
            return this;
        }
        public CassandraModifyWorker<TModel> SaveChanges()
        {
            if (ModifyCommands.Count == 0)
                return this;

            if (Table is null)
                NewTable();

            CheckTableGenrate();

            var Batch = Session.CreateBatch();
            foreach (var Command in ModifyCommands)
                Batch.Append(Command);

            Batch.Execute();
            ModifyCommands.Clear();
            return this;
        }
        #endregion

        #region Private Process
        private void TryGetPkName()
        {
            var AllAttrs = typeof(TModel)
                .GetCustomAttributes(true);

            var GetAttr = AllAttrs
                .FirstOrDefault(Item => Item.GetType().FullName == "Cassandra.Mapping.PrimaryKeyAttribute");

            if (GetAttr is null)
                return;

            var PrimaryKeyAttr = GetAttr as PrimaryKeyAttribute;
            PkName = PrimaryKeyAttr.ColumnNames[0];
        }
        private static MemberInitExpression CreateInitExpression(TModel Model, string SkipPkName = null)
        {
            //取得所有屬性
            var Properties = typeof(TModel)
                .GetProperties();

            //跳過主Key
            if (SkipPkName is not null)
                Properties = Properties
                    .Where(Item => Item.Name != SkipPkName)
                    .ToArray();

            //描述TModel的參數陳述式
            var ParamExp = Expression.Parameter(typeof(TModel));

            var BindsExp = Properties
                .Select(Item =>
                {
                    var PeopertyExp = Expression.Property(ParamExp, Item);
                    var Value = Item.GetValue(Model);
                    var ValueExp = Expression.Constant(Value, Item.PropertyType);
                    //設定陳述式 將Value設定到Property
                    var BindExp = Expression.Bind(Item, ValueExp);
                    return BindExp;
                });

            //建構 建構子陳述式
            var InitExp = Expression.MemberInit(Expression.New(typeof(TModel)), BindsExp);
            return InitExp;
        }
        private static Expression<Func<TModel, bool>> CreateWherePkExpression<TValue>(TModel Model, string PkNames, Func<TModel, TValue> PkValueFunc)
        {
            var ParamExp = Expression.Parameter(typeof(TModel));
            var EqualsExp = ConvertEqualsExpression(Model, PkNames, PkValueFunc);
            //建立針對 TModel 的參數陳述式
            var WhereExp = Expression.Lambda<Func<TModel, bool>>(EqualsExp, ParamExp);
            return WhereExp;
        }
        private static BinaryExpression ConvertEqualsExpression<TValue>(TModel Model, string PkName, Func<TModel, TValue> PkValueFunc)
        {
            var ParamExp = GetModelParamExpression();
            //建立屬性陳述式用來描述主Key名稱
            var PropertyExp = Expression.Property(ParamExp, PkName);
            //取得主Key值
            var PkValue = PkValueFunc(Model);
            //建立常數陳述式 對應到主Key
            var ValueExp = Expression.Constant(PkValue);
            //建立等於陳述式 用來比對 屬性跟值
            var EqualExp = Expression.Equal(PropertyExp, ValueExp);
            return EqualExp;
        }
        private static Expression<Func<TModel, TModel>> CreateSelectInitExpression(TModel Model, string SkipPkName = null)
        {
            var ParamExp = GetModelParamExpression();
            var InitExp = CreateInitExpression(Model, SkipPkName);
            var SelectInitExp = Expression.Lambda<Func<TModel, TModel>>(InitExp, ParamExp);
            return SelectInitExp;
        }
        private Func<TModel, object> DefaultPkValueFunc()
        {
            var PkInfo = GetModelProperties()
                .FirstOrDefault(Item => Item.Name == PkName) ??
                throw new Exception("Pk Column is not found");

            var Result = new Func<TModel, object>(Item => PkInfo.GetValue(Item));
            return Result;
        }
        private static PropertyInfo[] GetModelProperties()
            => typeof(TModel).GetProperties(BindingFlags.Instance | BindingFlags.Public);
        private static ParameterExpression GetModelParamExpression()
            => Expression.Parameter(typeof(TModel));
        #endregion
    }
    public class CassandraQueryWorker<TModel> : CassandraTableWorker<TModel>
    {
        internal CqlQuery<TModel> Query { get; set; }
        public CassandraQueryWorker(ISession _Session, string _TableName) : base(_Session, _TableName)
        {
        }
        #region Query Data
        public CassandraTableWorker<TModel> NewQuery()
        {
            Query = Table.Select(Item => Item);
            return this;
        }
        public CassandraTableWorker<TModel> Where(Expression<Func<TModel, bool>> WhereExp)
        {
            CheckQuery();
            Query = Query.Where(WhereExp);
            return this;
        }
        public TModel First(Expression<Func<TModel, bool>> WhereExp)
        {
            CheckQuery();
            var Result = Query.First(WhereExp);
            Query = null;
            return Result;
        }
        public TModel FirstOrDefault(Expression<Func<TModel, bool>> WhereExp)
        {
            CheckQuery();
            var Result = Query.FirstOrDefault(WhereExp);
            Query = null;
            return Result;
        }
        public bool Any(Expression<Func<TModel, bool>> WhereExp)
        {
            CheckQuery();
            var Result = Query.Any(WhereExp);
            Query = null;
            return Result;
        }
        public CassandraTableWorker<TModel> Take(int Count)
        {
            CheckQuery();
            Query = Query.Take(Count);
            return this;
        }
        public CassandraTableWorker<TModel> OrderBy<TValue>(Expression<Func<TModel, TValue>> OrderExp)
        {
            CheckQuery();
            Query = Query.OrderBy(OrderExp);
            return this;
        }
        public CassandraTableWorker<TModel> OrderByDescending<TValue>(Expression<Func<TModel, TValue>> OrderDescExp)
        {
            CheckQuery();
            Query = Query.OrderByDescending(OrderDescExp);
            return this;
        }
        public override TModel[] ToArray()
        {
            CheckQuery();
            var Result = Query.Execute().ToArray();
            Query = null;
            return Result;
        }
        public override List<TModel> ToList()
        {
            CheckQuery();
            var Result = Query.Execute().ToList();
            Query = null;
            return Result;
        }
        public override IEnumerable<TModel> AsEnumerable()
        {
            CheckQuery();
            var Result = Query.Execute().AsEnumerable();
            Query = null;
            return Result;
        }
        #endregion


        #region Private Process
        private void CheckQuery()
        {
            if (Query is null)
                NewQuery();

            CheckTableGenrate();
        }
        #endregion
    }
}