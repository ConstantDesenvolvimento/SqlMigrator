using System.Threading.Tasks;
using SqlMigrator.Model;

namespace SqlMigrator.Services
{
    public interface IManipulateDatabases
    {
        Task Create();
        Task ExecuteMigration(Migration migration);
        Task<DatabaseVersion> CurrentVersion();
    }
}