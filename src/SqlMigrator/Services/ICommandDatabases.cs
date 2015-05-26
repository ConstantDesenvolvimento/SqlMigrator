using System.Threading.Tasks;
using SqlMigrator.Model;

namespace SqlMigrator.Services
{
    public interface ICommandDatabases
    {
        Task<string> Create();
        Task CreateMigrationHistoryTable();
        Task ExecuteMigration(Migration migration);
        Task<DatabaseVersion> CurrentVersion();
    }
}