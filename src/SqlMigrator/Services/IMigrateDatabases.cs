using System.Threading.Tasks;

namespace SqlMigrator.Services
{
    public interface IMigrateDatabases
    {
        Task Migrate();
    }
}