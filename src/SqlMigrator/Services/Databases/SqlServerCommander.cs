using System;
using System.Data;
using System.IO;
using System.Reflection;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using SqlMigrator.Logging;
using SqlMigrator.Model;

namespace SqlMigrator.Services.Databases
{
    public class SqlServerCommander : ICommandDatabases
    {
        private static readonly ILog Logger = LogProvider.For<SqlServerCommander>();
        private readonly Func<IDbConnection> _connectionFactory;
        private readonly string _databaseName;
        private static readonly Regex DatabaseFinder = new Regex(
            @"(database|initial catalog)\s*=\s*(?<database>[^;]+)", RegexOptions.IgnoreCase);
        private static readonly Regex SqlSplitter = new Regex(
            @"^\s*GO\s*$", RegexOptions.IgnoreCase | RegexOptions.Multiline);

        private const string CreateDatabaseTemplate = @"
            if not exists (select * from master.sys.databases where name='{0}') 
	            create database [{0}]
            GO
            use [{0}]
            GO
            if not exists (select * from sys.schemas where name='migrations')
	            EXEC('CREATE SCHEMA migrations ');
            GO
            if not exists (select * from sys.tables t inner join  sys.schemas s on s.schema_id=t.schema_id where t.name='log' and s.name='migrations' ) 
	            create table migrations.log (number nvarchar(200) primary key clustered,applied datetimeoffset not null )
            GO
        ";
        private const string ExecuteMigrationTemplate = @"
            use [{0}]
            GO
            {1}
            GO
            insert into migrations.log (number,applied) values ('{2}',getdate())

        ";


        public SqlServerCommander(Func<IDbConnection> connectionFactory,string database=null)
        {
            _connectionFactory = connectionFactory;
            _databaseName= FindDatabaseName(database);
        }

        private string FindDatabaseName(string database)
        {
            return database ?? GetConnectionStringDatabaseName() ?? "db_" + Guid.NewGuid().ToString("N");
        }

        private string GetConnectionStringDatabaseName()
        {
            using (var connection=_connectionFactory())
            {
                var match = DatabaseFinder.Match(connection.ConnectionString);
                if (match.Success)
                {
                    return match.Groups["database"].Value;
                }
            }
            return null;
        }

        public Task<string> Create()
        {
            return Task.Run(() =>
            {
                RunSql(ApplyTemplate(CreateDatabaseTemplate, _databaseName),false);
                return _databaseName;
            });
        }

        public Task CreateMigrationHistoryTable()
        {
            return Create();
        }

        private void RunSql(string sql,bool inTransaction=true)
        {
            using (var connection = _connectionFactory())
            {
                if (connection.State != ConnectionState.Open)
                {
                    connection.Open();
                }
                if (inTransaction)
                {
                    using (var transaction = connection.BeginTransaction())
                    {
                        ExecuteCommands(sql, connection, transaction);
                        transaction.Commit();
                    }
                }
                else
                {
                    ExecuteCommands(sql, connection, null);
                }
               
               
            }
        }

        private static void ExecuteCommands(string sql, IDbConnection connection, IDbTransaction transaction)
        {
            var parts = SqlSplitter.Split(sql);
            foreach (var part in parts)
            {
                if (!string.IsNullOrEmpty(part))
                {
                    using (var cmd = connection.CreateCommand())
                    {
                        cmd.CommandText = part;
                        cmd.Transaction = transaction;
                        cmd.ExecuteNonQuery();
                    }
                }
            }
        }

        private T ExecuteScalar<T>(string sql)
        {
            using (var connection = _connectionFactory())
            {
                if (connection.State != ConnectionState.Open)
                {
                    connection.Open();
                }
                using (var cmd = connection.CreateCommand())
                {
                    cmd.CommandText = sql;
                    return (T)cmd.ExecuteScalar();
                }
            }
            
        }
        private string ApplyTemplate(string template, params object[] parameters)
        {
           
            if (!string.IsNullOrEmpty(template))
            {
                return string.Format(template, parameters);
            }
            return null;
        }

        public Task ExecuteMigration(Migration migration)
        {
            return Task.Run(() =>
            {
                if (ExecuteScalar<int>(string.Format("select count(*) from {0}.migrations.log where number='{1}'",_databaseName, migration.Number)) == 0)
                {
                    RunSql(ApplyTemplate(ExecuteMigrationTemplate, _databaseName, migration.Sql,migration.Number));    
                }
                else
                {
                    Logger.InfoFormat("trying to reapply  migration {migrationNumber} to database {database} ",migration.Number,_databaseName);
                    throw new InvalidOperationException("migration already applied!");
                }
                
            });
            
        }

        public Task<DatabaseVersion> CurrentVersion()
        {
            return Task.Run(() =>
            {
                string version = null;
                try
                {
                    version =
                   ExecuteScalar<string>(string.Format("select top 1 number from {0}.migrations.log order by applied desc ", _databaseName));
                }
                catch (Exception ex)
                {
                    Logger.DebugException("got an exception while trying to retrieve the last applied migration number",ex);
                    if (ExecuteScalar<int>(string.Format("select count(*) from master.sys.databases where name='{0}'", _databaseName)) == 0)
                    {
                        return new DatabaseVersion() { Type = DatabaseVersionType.NotCreated };
                    }
                    if (ExecuteScalar<int>(string.Format("select count(*) from {0}.sys.tables t inner join  {0}.sys.schemas s on s.schema_id=t.schema_id where t.name='log' and s.name='migrations' ", _databaseName)) == 0)
                    {
                        return new DatabaseVersion() { Type = DatabaseVersionType.MissingMigrationHistoryTable };
                    }
                }
                
               
               
                return new DatabaseVersion() {Type = DatabaseVersionType.VersionNumber, Number = version};
            });
        }
    }
}