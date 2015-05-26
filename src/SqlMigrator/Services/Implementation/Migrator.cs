using System.Collections.Generic;
using System.Threading.Tasks;
using SqlMigrator.Model;
using System.Linq;

namespace SqlMigrator.Services.Implementation
{
    public class Migrator : IMigrateDatabases
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

        public async Task Migrate()
        {
            DatabaseVersion current = await _handler.CurrentVersion();
            IEnumerable<Migration> migrations = null;
            switch (current.Type)
            {
                case DatabaseVersionType.NotCreated:
                    await _handler.Create();
                    migrations = (await _source.LoadMigrations()).OrderBy(m => m, _comparer);
                    break;
                case DatabaseVersionType.MissingMigrationHistoryTable:
                    await _handler.CreateMigrationHistoryTable();
                    migrations = (await _source.LoadMigrations()).OrderBy(m => m, _comparer);
                    break;
                default:
                     migrations = (await _source.LoadMigrations()).Where(m=>_comparer.IsMigrationAfterVersion(m,current.Number)).OrderBy(m => m, _comparer);
                    break;
            }
            
            foreach (var migration in migrations)
            {
                await _handler.ExecuteMigration(migration);
            }
            
        }
    }
}