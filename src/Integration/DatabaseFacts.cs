using System;
using System.Configuration;
using System.Data;
using System.Data.SqlClient;
using NUnit.Framework;
using SqlMigrator.Model;
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

        [Test]
        public async void current_version_returns_not_created_when_there_is_no_database()
        {
            var sqlserver = new SqlServerCommander(CreateConnection);
            var version = await sqlserver.CurrentVersion();
            Assert.AreEqual(DatabaseVersionType.NotCreated, version.Type);
        }
        [Test]
        public async void current_version_returns_missing_control_table_when_there_is_a_database_but_no_log_table()
        {
            database = "db_" + Guid.NewGuid().ToString("N");
            var sqlserver = new SqlServerCommander(CreateConnection, database);
            using (IDbConnection connection = CreateConnection())
            {
                connection.Open();
                using (IDbCommand cmd = connection.CreateCommand())
                {
                    cmd.CommandText = string.Format("create database {0}", database);
                    object result = cmd.ExecuteNonQuery();
                }
            }
            var version = await sqlserver.CurrentVersion();
            Assert.AreEqual(DatabaseVersionType.MissingMigrationHistoryTable, version.Type);
        }

        [Test]
        public async void execute_migration()
        {
            var sqlserver = new SqlServerCommander(CreateConnection);
            database=await sqlserver.Create();
            await sqlserver.ExecuteMigration(new Migration()
            {
                Number = "20150101",
                Sql = "create table test (id int primary key clustered)"
            });

            using (IDbConnection connection = CreateConnection())
            {
                connection.Open();
                using (IDbCommand cmd = connection.CreateCommand())
                {
                    cmd.CommandText = string.Format("select count(*) from {0}.sys.tables where name='test'", database);
                    object result = cmd.ExecuteScalar();
                    Assert.AreEqual(1, result);
                }
            }
        }
        [Test]
        public async void get_correct_version_number()
        {
            var sqlserver = new SqlServerCommander(CreateConnection);
            database = await sqlserver.Create();
            await sqlserver.ExecuteMigration(new Migration()
            {
                Number = "20150101",
                Sql = "create table test (id int primary key clustered)"
            });
            await sqlserver.ExecuteMigration(new Migration()
            {
                Number = "20150102",
                Sql = "create table test2 (id int primary key clustered)"
            });

            var version = await sqlserver.CurrentVersion();
            Assert.AreEqual(DatabaseVersionType.VersionNumber, version.Type);
            Assert.AreEqual("20150102",version.Number);
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
                        cmd.CommandText = string.Format("if exists (select * from sys.databases where name='{0}') drop database {0}", database);
                        cmd.ExecuteNonQuery();
                    }
                }
            }
        }

    }
}