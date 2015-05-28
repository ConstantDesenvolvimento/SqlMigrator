/*
   The MIT License (MIT)

   Copyright (c) 2015 Jose Felix

   Permission is hereby granted, free of charge, to any person obtaining a copy
   of this software and associated documentation files (the "Software"), to deal
   in the Software without restriction, including without limitation the rights
   to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
   copies of the Software, and to permit persons to whom the Software is
   furnished to do so, subject to the following conditions:

   The above copyright notice and this permission notice shall be included in all
   copies or substantial portions of the Software.

   THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
   IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
   FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
   AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
   LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
   OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
   SOFTWARE.
*/

using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text.RegularExpressions;
using System.Threading.Tasks;


namespace SqlMigrator
{
    #region Models
    internal class DatabaseVersion
    {
        public DatabaseVersionType Type { get; set; }
        public string Number { get; set; }
    }
    internal class Migration
    {
        public string Number { get; set; }
        public string Sql { get; set; }
    }

    internal enum DatabaseVersionType
    {
        NotCreated,
        MissingMigrationHistoryTable,
        VersionNumber
    }
    #endregion
    #region Services
    internal interface ISupplyMigrations
    {
       IEnumerable<Migration>LoadMigrations();

    }
    internal interface IMigrateDatabases
    {
        void Migrate();
    }
    internal interface ICompareMigrations : IComparer<Migration>
    {
        bool IsMigrationAfterVersion(Migration m, string version);

    }
    internal interface ICommandDatabases
    {
        string Create();
        void CreateMigrationHistoryTable();
        void ExecuteMigration(Migration migration);
        DatabaseVersion CurrentVersion();
    }

    internal interface ILog
    {
        void Info(string message,params object[] parameters);
        void Debug(string message,params object[] parameters);
        void Error(string message,params object[] parameters);

    }

    #endregion
#region ServiceImplementation

    internal class NullLogger:ILog
    {
        public void Info(string message, params object[] parameters)
        {
            
        }

        public void Debug(string message, params object[] parameters)
        {
           
        }

        public void Error(string message, params object[] parameters)
        {
           
        }
    }
    internal class NumberComparer : ICompareMigrations
    {
        private static readonly Regex Numbers = new Regex(@"\d+");

        public int Compare(Migration x, Migration y)
        {
            var matchesX = GetMatches(x.Number);
            var matchesY = GetMatches(y.Number);
            for (var i = 0; i < matchesX.Count; i++)
            {
                if (matchesY.Count <= i)
                {
                    //Since the other version number has less elements and so far they are equals, then this MigrationX is greater then MigrationY
                    return 1;
                }
                var numberX = long.Parse(matchesX[i].Value);
                var numberY = long.Parse(matchesY[i].Value);
                var result = numberX.CompareTo(numberY);
                if (result != 0)
                {
                    return result;
                }
            }
            if (matchesY.Count > matchesX.Count)
            {
                return -1;
            }
            return 0;
        }

        public bool IsMigrationAfterVersion(Migration m, string version)
        {
            return Compare(m, new Migration { Number = version }) == 1;
        }

        private MatchCollection GetMatches(string s)
        {
            var retval = Numbers.Matches(s);
            if (retval.Count == 0)
            {
                throw new FormatException(string.Format("invalid version format for this comparer : {0}", s));
            }
            return retval;
        }
    }
    internal class Migrator : IMigrateDatabases
    {
        private readonly ICompareMigrations _comparer;
        private readonly ICommandDatabases _handler;
        private readonly ISupplyMigrations _source;

        public Migrator(ISupplyMigrations source, ICompareMigrations comparer, ICommandDatabases handler)
        {
            _source = source;
            _comparer = comparer;
            _handler = handler;
        }

        public void Migrate()
        {
            DatabaseVersion current = _handler.CurrentVersion();
            IEnumerable<Migration> migrations = null;
            switch (current.Type)
            {
                case DatabaseVersionType.NotCreated:
                     _handler.Create();
                    migrations =  _source.LoadMigrations().OrderBy(m => m, _comparer);
                    break;
                case DatabaseVersionType.MissingMigrationHistoryTable:
                     _handler.CreateMigrationHistoryTable();
                    migrations =  _source.LoadMigrations().OrderBy(m => m, _comparer);
                    break;
                default:
                    migrations =  _source.LoadMigrations().Where(m => _comparer.IsMigrationAfterVersion(m, current.Number)).OrderBy(m => m, _comparer);
                    break;
            }

            foreach (var migration in migrations.ToList())
            {
                 _handler.ExecuteMigration(migration);
            }

        }
    }
    internal class FileSystemSource : ISupplyMigrations
    {
        private readonly string _path;

        public FileSystemSource(string path)
        {
            _path = path;
        }

        public IEnumerable<Migration> LoadMigrations()
        {
            foreach (var file in Directory.GetFiles(_path))
            {
                using (var reader = File.OpenText(file))
                {
                    yield return new Migration()
                    {
                        Number = Path.GetFileNameWithoutExtension(file),
                        Sql = reader.ReadToEnd()
                    };
                }

            }
            
        }
    }

    internal class AssemblyResourcesSource : ISupplyMigrations
    {
        private readonly Assembly _assembly;
        private readonly string _path;
        private readonly Regex _fileNameRegex;

        public AssemblyResourcesSource(Assembly assembly, string path)
        {
            _assembly = assembly;
            _path = path;
            _fileNameRegex = new Regex(path.Replace(".", @"\.") + @"\.(?<name>[^.]+)\.?([^.]+)?$", RegexOptions.Singleline);
        }

        public IEnumerable<Migration> LoadMigrations()
        {
            foreach (var resourceName in _assembly.GetManifestResourceNames())
            {
                var match = _fileNameRegex.Match(resourceName);
                if (match.Success)
                {
                    using (var stream = _assembly.GetManifestResourceStream(resourceName))
                    {
                        if (stream != null)
                        {
                            using (var reader = new StreamReader(stream))
                            {
                                yield return new Migration() { Sql = reader.ReadToEnd(), Number = match.Groups["name"].Value };
                            }
                        }
                    }
                }


            }

        }

       
    }
    internal class SqlServerCommander : ICommandDatabases
    {
        private readonly ILog _logger;
        private readonly Func<IDbConnection> _connectionFactory;
        private readonly string _service;
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
                begin
	                create table migrations.history (service nvarchar(200) not null,number nvarchar(200) not null,applied datetimeoffset not null , CONSTRAINT PK_history PRIMARY KEY CLUSTERED 	(service,number	) )
                end
            GO
        ";
        private const string ExecuteMigrationTemplate = @"
            use [{0}]
            GO
            {3}
            GO
            insert into migrations.history (service,number,applied) values ('{1}','{2}',getdate())

        ";


        public SqlServerCommander(Func<IDbConnection> connectionFactory,ILog logger=null, string database = null,string service=null)
        {
            _logger = logger??new NullLogger();
            _connectionFactory = connectionFactory;
            
            _service = service??"main";
            _databaseName = FindDatabaseName(database);
            
        }

        private string FindDatabaseName(string database)
        {
            return database ?? GetConnectionStringDatabaseName() ?? "db_" + Guid.NewGuid().ToString("N");
        }

        private IDbConnection OpenConnection()
        {
            var connection = _connectionFactory();
            var newConnectionString = DatabaseFinder.Replace(connection.ConnectionString, string.Empty);

            if (newConnectionString != connection.ConnectionString)
            {
                if (connection.State != ConnectionState.Closed)
                {
                    connection.Close();
                }

                connection.ConnectionString = newConnectionString;
            }

            connection.Open();
            return connection;
        }
        private string GetConnectionStringDatabaseName()
        {
            using (var connection = _connectionFactory())
            {
                var match = DatabaseFinder.Match(connection.ConnectionString);
                if (match.Success)
                {
                    return match.Groups["database"].Value;
                }
            }
            return null;
        }

        public string Create()
        {
            RunSql(ApplyTemplate(CreateDatabaseTemplate, _databaseName), false);
            return _databaseName;
        }

        public void CreateMigrationHistoryTable()
        {
            Create();
        }

        private void RunSql(string sql, bool inTransaction = true)
        {
            using (var connection = OpenConnection())
            {
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
            using (var connection = OpenConnection())
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

        public void ExecuteMigration(Migration migration)
        {
            if (ExecuteScalar<int>(string.Format("select count(*) from {0}.migrations.history where service='{1}' and number='{2}'", _databaseName,_service, migration.Number)) == 0)
            {
                RunSql(ApplyTemplate(ExecuteMigrationTemplate, _databaseName,_service, migration.Number, migration.Sql));
            }
            else
            {
                _logger.Info("trying to reapply  migration {migrationNumber} to database {database} ", migration.Number, _databaseName);
                throw new InvalidOperationException("migration already applied!");
            }
        }

        public DatabaseVersion CurrentVersion()
        {
            
            string version = null;
            try
            {
                version =
                ExecuteScalar<string>(string.Format("select top 1 number from {0}.migrations.history where service='{1}' order by applied desc ", _databaseName,_service));
            }
            catch (Exception ex)
            {
                    
                _logger.Debug("got an exception while trying to retrieve the last applied migration number {@exception}", ex);
                if (ExecuteScalar<int>(string.Format("select count(*) from master.sys.databases where name='{0}'", _databaseName)) == 0)
                {
                    return new DatabaseVersion() { Type = DatabaseVersionType.NotCreated };
                }
                if (ExecuteScalar<int>(string.Format("select count(*) from {0}.sys.tables t inner join  {0}.sys.schemas s on s.schema_id=t.schema_id where t.name='history' and s.name='migrations' ", _databaseName)) == 0)
                {
                    return new DatabaseVersion() { Type = DatabaseVersionType.MissingMigrationHistoryTable };
                }
            }
            return new DatabaseVersion() { Type = DatabaseVersionType.VersionNumber, Number = version };
           
        }
    }
#endregion
}
