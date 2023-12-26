using Cassandra;
using Cassandra.Mapping;
using Cassandra.Mapping.Attributes;
using Rugal.DatabaseWorker.Cassandra.Service;

var Session = Cluster.Builder()
    .AddContactPoints("dtvl.com.tw")
    .WithPort(9042)
    .Build()
    .Connect("dtvl");

Session.UserDefinedTypes.Define(UdtMap.For<UDT_UserInfo>("udt_userinfo"));

var Worker = new CassandraWorker(Session);
var ModifyWorker = Worker
    .WithModify<User>("user")
    .Add(new User()
    {
        UserId = Guid.NewGuid(),
        UserInfo = new UDT_UserInfo()
        {
            Email = "test@",
            PhoneNumber = "0987654",
            UserName = "Rugal",
            UserNo = "123",
        }
    })
    .SaveChanges();

[PrimaryKey(nameof(Test))]
public class User
{
    [PartitionKey]
    public Guid UserId { get; set; }

    [SecondaryIndex]
    public Guid Test { get; set; }
    public UDT_UserInfo UserInfo { get; set; }
}

public class UDT_UserInfo
{
    public string UserName { get; set; }
    public string UserNo { get; set; }
    public string PhoneNumber { get; set; }
    public string Email { get; set; }
}

