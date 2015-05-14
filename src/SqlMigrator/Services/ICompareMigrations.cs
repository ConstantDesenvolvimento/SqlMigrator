using System.Collections.Generic;
using SqlMigrator.Model;

namespace SqlMigrator.Services
{
    public interface ICompareMigrations : IComparer<Migration>
    {
        bool IsMigrationAfterVersion(Migration m, string version);

    }
}