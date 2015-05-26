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
            if (current.Type == DatabaseVersionType.NotCreated)
            {
                await _handler.Create();
                migrations = (await _source.LoadMigrations()).OrderBy(m => m, _comparer);
            }
            else
            {
                migrations = (await _source.LoadMigrations()).Where(m=>_comparer.IsMigrationAfterVersion(m,current.Number)).OrderBy(m => m, _comparer);
            }
            foreach (var migration in migrations)
            {
                await _handler.ExecuteMigration(migration);
            }
            
        }
    }
}