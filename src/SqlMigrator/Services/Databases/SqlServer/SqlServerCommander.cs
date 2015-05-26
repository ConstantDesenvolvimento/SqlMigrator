using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Reflection;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using SqlMigrator.Model;

namespace SqlMigrator.Services.Databases.SqlServer
{
    public class SqlServerCommander : ICommandDatabases
    {
        private readonly Func<IDbConnection> _connectionFactory;
        private readonly string _databaseName;
        private static readonly Regex DatabaseFinder = new Regex(
            @"(database|initial catalog)\s*=\s*(?<database>[^;]+)", RegexOptions.IgnoreCase);
        private static readonly Regex SqlSplitter = new Regex(
            @"^\s*GO\s*$", RegexOptions.IgnoreCase | RegexOptions.Multiline);
        

        private static readonly Dictionary<string, string> Templates=new Dictionary<string, string>();

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
                RunSql(ApplyTemplate("CreateDatabase", _databaseName));
                return _databaseName;
            });
        }

        private void RunSql(string sql)
        {
            using (var connection = _connectionFactory())
            {
                if (connection.State != ConnectionState.Open)
                {
                    connection.Open();
                }
                var parts = SqlSplitter.Split(sql);
                foreach (var part in parts)
                {
                    if (!string.IsNullOrEmpty(part))
                    {
                        using (var cmd = connection.CreateCommand())
                        {
                            cmd.CommandText = part;
                            cmd.ExecuteNonQuery();
                        } 
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
        private string ApplyTemplate(string templateName, params object[] parameters)
        {
            string template = null;
            Templates.TryGetValue(templateName, out template);
            if (template == null)
            {
                using (var stream = Assembly.GetAssembly(typeof (SqlServerCommander))
                    .GetManifestResourceStream(
                        string.Format("SqlMigrator.Services.Databases.SqlServer.Scripts.{0}.tpl", templateName)))
                {
                    if (stream != null)
                    {
                        using (var reader = new StreamReader(stream))
                        {
                            template = reader.ReadToEnd();
                        }
                    }
                }
               
                Templates.Add(templateName,template);
            }
            if (template != null)
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
                    RunSql(ApplyTemplate("ExecuteMigration", _databaseName, migration.Sql));    
                }
                else
                {
                    throw new InvalidOperationException("migration already applied!");
                }
                
            });
            
        }

        public Task<DatabaseVersion> CurrentVersion()
        {
            return Task.Run(() =>
            {
                if (ExecuteScalar<int>(string.Format("select count(*) from master.sys.databases where name='{0}'", _databaseName)) == 0)
                {
                    return new DatabaseVersion() { Type = DatabaseVersionType.NotCreated };    
                }
                var version =
                    ExecuteScalar<string>("select top 1 number from {0}.migrations.log order by applied desc ");
                return new DatabaseVersion() {Type = DatabaseVersionType.VersionNumber, Number = version};
            });
        }
    }
}