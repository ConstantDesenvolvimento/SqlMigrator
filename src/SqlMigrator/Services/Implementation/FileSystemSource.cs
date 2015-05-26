using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using SqlMigrator.Model;

namespace SqlMigrator.Services.Implementation
{
    public class FileSystemSource : ISupplyMigrations
    {
        private readonly string _path;

        public FileSystemSource(string path)
        {
            _path = path;
        }

        public Task<IEnumerable<Migration>> LoadMigrations()
        {
            return Task.Run(() =>
            {
                var migrations = new List<Migration>();
                foreach (var file in Directory.GetFiles(_path))
                {
                    using (var reader=File.OpenText(file))
                    {
                        migrations.Add(new Migration()
                        {
                            Number = Path.GetFileNameWithoutExtension(file),
                            Sql = reader.ReadToEnd()
                        });    
                    }
                    
                }
                return (IEnumerable<Migration>)migrations;
            });
        }
    }
}