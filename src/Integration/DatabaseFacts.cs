using System;
using System.Data;
using System.Data.SqlClient;
using Moq;
using NUnit.Framework;
using SqlMigrator;

namespace Integration
{
    [TestFixture]
    public class DatabaseFacts
    {
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
                        cmd.CommandText =
                            string.Format("if exists (select * from sys.databases where name='{0}') drop database {0}",
                                database);
                        cmd.ExecuteNonQuery();
                    }
                }
            }
        }

        private string database;

        private IDbConnection CreateConnection()
        {
            string connectionString = string.Format("Data Source={0};Integrated Security=SSPI;", GetDataSource());
            return new SqlConnection(connectionString);
        }

        private IDbConnection CreateDatabaseConnection()
        {
            string connectionString = string.Format("Data Source={0};Integrated Security=SSPI;database={1}",
                GetDataSource(), database);
            return new SqlConnection(connectionString);
        }

        private static Mock<ISupplyMigrations> SetupSource()
        {
            var source = new Mock<ISupplyMigrations>();
            source.Setup(s => s.LoadMigrations()).Returns(new[]
            {
                new Migration {Sql = "create table test (id int not null)", Number = "0"},
                new Migration {Sql = "create table test2 (id int not null)", Number = "1"}
            });
            return source;
        }

        private static string GetDataSource()
        {
            string dataSource = Environment.GetEnvironmentVariable("integration_test_server_database");
            if (string.IsNullOrEmpty(dataSource))
            {
                dataSource = ".";
            }
            return dataSource;
        }

        [Test]
        public void create_database()
        {
            var sqlserver = new SqlServerCommander(CreateConnection);
            database = sqlserver.Create();
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
        public void current_version_returns_missing_control_table_when_there_is_a_database_but_no_history_table()
        {
            database = "db_" + Guid.NewGuid().ToString("N");
            var sqlserver = new SqlServerCommander(CreateConnection, null, database);
            using (IDbConnection connection = CreateConnection())
            {
                connection.Open();
                using (IDbCommand cmd = connection.CreateCommand())
                {
                    cmd.CommandText = string.Format("create database {0}", database);
                    object result = cmd.ExecuteNonQuery();
                }
            }
            DatabaseVersion version = sqlserver.CurrentVersion();
            Assert.AreEqual(DatabaseVersionType.MissingMigrationHistoryTable, version.Type);
        }

        [Test]
        public void current_version_returns_not_created_when_there_is_no_database()
        {
            var sqlserver = new SqlServerCommander(CreateConnection);
            DatabaseVersion version = sqlserver.CurrentVersion();
            Assert.AreEqual(DatabaseVersionType.NotCreated, version.Type);
        }

        [Test]
        public void database_created_has_migration_history_table()
        {
            var sqlserver = new SqlServerCommander(CreateConnection);
            database = sqlserver.Create();
            using (IDbConnection connection = CreateConnection())
            {
                connection.Open();
                using (IDbCommand cmd = connection.CreateCommand())
                {
                    cmd.CommandText =
                        string.Format(
                            "select count(*) from {0}.sys.tables t inner join  {0}.sys.schemas s on s.schema_id=t.schema_id where t.name='history' and s.name='migrations'",
                            database);
                    object result = cmd.ExecuteScalar();
                    Assert.AreEqual(1, result);
                }
            }
        }

        [Test]
        public void execute_migration()
        {
            var sqlserver = new SqlServerCommander(CreateConnection);
            database = sqlserver.Create();
            sqlserver.ExecuteMigration(new Migration
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
        public void get_correct_version_number()
        {
            var sqlserver = new SqlServerCommander(CreateConnection);
            database = sqlserver.Create();
            sqlserver.ExecuteMigration(new Migration
            {
                Number = "20150101",
                Sql = "create table test (id int primary key clustered)"
            });
            sqlserver.ExecuteMigration(new Migration
            {
                Number = "20150102",
                Sql = "create table test2 (id int primary key clustered)"
            });

            DatabaseVersion version = sqlserver.CurrentVersion();
            Assert.AreEqual(DatabaseVersionType.VersionNumber, version.Type);
            Assert.AreEqual("20150102", version.Number);
        }

        [Test]
        public void migrate_with_full_connection_string()
        {
            database = "db_" + Guid.NewGuid().ToString("N");
            var migrator = new Migrator(SetupSource().Object, new NumberComparer(),
                new SqlServerCommander(CreateDatabaseConnection));
            migrator.Migrate();
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
    }
}