using System.Configuration;
using System.Data;
using System.Data.SqlClient;
using NUnit.Framework;
using SqlMigrator.Services.Databases;
using SqlMigrator.Services.Databases.SqlServer;

namespace Integration
{
    [TestFixture]
    public class DatabaseFacts
    {
        private string database;
        private IDbConnection CreateConnection()
        {
            return new SqlConnection(ConfigurationManager.AppSettings["ConnectionString"]);
        }

        [Test]
        public async void create_database()
        {
            var sqlserver = new SqlServerCommander(CreateConnection);
            database = await sqlserver.Create();
            using (IDbConnection connection = CreateConnection())
            {
                connection.Open();
                using (IDbCommand cmd = connection.CreateCommand())
                {
                    cmd.CommandText = string.Format("select count(*) from sys.databases where name='{0}'", database);
                    object result = cmd.ExecuteScalar();
                    Assert.AreEqual(1, result);
                }
            }
        }
        [Test]
        public async void database_created_has_migration_log_table()
        {
            var sqlserver = new SqlServerCommander(CreateConnection);
            database = await sqlserver.Create();
            using (IDbConnection connection = CreateConnection())
            {
                connection.Open();
                using (IDbCommand cmd = connection.CreateCommand())
                {
                    cmd.CommandText = string.Format("select count(*) from {0}.sys.tables t inner join  {0}.sys.schemas s on s.schema_id=t.schema_id where t.name='log' and s.name='migrations'", database);
                    object result = cmd.ExecuteScalar();
                    Assert.AreEqual(1, result);
                }
            }
        }

        [TearDown]
        public void TearDown()
        {
            if (!string.IsNullOrEmpty(database))
            {
                using (IDbConnection connection = CreateConnection())
                {
                    connection.Open();
                    using (IDbCommand cmd = connection.CreateCommand())
                    {
                        cmd.CommandText = string.Format("drop database {0}", database);
                        cmd.ExecuteNonQuery();
                    }
                }
            }
        }
    }
}