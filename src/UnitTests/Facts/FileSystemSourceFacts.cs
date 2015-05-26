using System;
using System.IO;
using System.Linq;
using NUnit.Framework;
using SqlMigrator.Services.Implementation;

namespace UnitTests.Facts
{
    [TestFixture]
    public class FileSystemSourceFacts
    {
        private void CreateFile(string path, string name,string content)
        {
            using (StreamWriter file = File.CreateText(Path.Combine(path, name)))
            {
                file.Write(content);
            }
        }

        private string CreateTempDir()
        {
            string dir = Path.Combine(Path.GetTempPath(), Guid.NewGuid().ToString("N"));
            Directory.CreateDirectory(dir);
            return dir;
        }

        [Test]
        public void load_files()
        {
            var dir = CreateTempDir();
            try
            {
                
                var content = "--sql header comment";
                CreateFile(dir, "20150520.sql", content);
                CreateFile(dir, "20150521.sql", content);
                CreateFile(dir, "20150522.sql", content);
                var source = new FileSystemSource(dir);
                var migrations = source.LoadMigrations().Result;
                Assert.AreEqual(3, migrations.Count());
                foreach (var migration in migrations)
                {
                    Assert.AreEqual(content, migration.Sql);
                }
            }
            finally 
            {
                
                Directory.Delete(dir,true);
            }
           
        }
        [Test]
        public void correct_migration_number()
        {
            var dir = CreateTempDir();
            try
            {

                var content = "--sql header comment";
                CreateFile(dir, "20150520.sql", content);
                var source = new FileSystemSource(dir);
                var migrations = source.LoadMigrations().Result;
                Assert.AreEqual(1, migrations.Count());
                Assert.AreEqual("20150520",migrations.First().Number);
            }
            finally
            {

                Directory.Delete(dir, true);
            }

        }
    }
}