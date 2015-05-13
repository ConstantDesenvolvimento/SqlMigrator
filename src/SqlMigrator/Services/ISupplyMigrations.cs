using System.Collections.Generic;
using System.Threading.Tasks;
using SqlMigrator.Model;

namespace SqlMigrator.Services
{
    public interface ISupplyMigrations
    {
        Task<IEnumerable<Migration>> LoadMigrations();
    }
}