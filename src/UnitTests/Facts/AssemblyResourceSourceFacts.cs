using System.Collections.Generic;
using System.Linq;
using NUnit.Framework;
using SqlMigrator;

namespace UnitTests.Facts
{
    [TestFixture]
    public class AssemblyResourceSourceFacts
    {
        [Test]
        public async void ListMigrations()
        {
            var source = new AssemblyResourcesSource(GetType().Assembly, "Resources");
            List<Migration> migrations = (await source.LoadMigrations()).ToList();
            Assert.AreEqual(1, migrations.Count);
            Assert.AreEqual("--sql header comment",migrations.First().Sql);
            Assert.AreEqual("20150101", migrations.First().Number);
        }
    }
}