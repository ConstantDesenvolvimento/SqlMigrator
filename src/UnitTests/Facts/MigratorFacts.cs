using Moq;
using NUnit.Framework;
using SqlMigrator;

namespace UnitTests.Facts
{
    [TestFixture]
    public class MigratorFacts
    {
        private static Mock<ICommandDatabases> SetupHandler(DatabaseVersion version)
        {
            var handler = new Mock<ICommandDatabases>();
            handler.Setup(h => h.CurrentVersion()).ReturnsAsync(version);
            return handler;
        }

        private static Mock<ISupplyMigrations> SetupSource()
        {
            var source = new Mock<ISupplyMigrations>();
            source.Setup(s => s.LoadMigrations()).ReturnsAsync(new[]
            {
                new Migration {Sql = "create table test (id int not null)", Number = "0"},
                new Migration {Sql = "create table test2 (id int not null)", Number = "1"}
            });
            return source;
        }

        [Test]
        public async void create_database_and_apply_all_migrations()
        {
            Mock<ISupplyMigrations> source = SetupSource();

            Mock<ICommandDatabases> handler = SetupHandler(new DatabaseVersion {Type = DatabaseVersionType.NotCreated});
            var comparer = SetupComparer();
            var migrator = new Migrator(source.Object, comparer.Object, handler.Object);
            await migrator.Migrate();
            handler.Verify(h => h.Create(), Times.Once);
            handler.Verify(h => h.CreateMigrationHistoryTable(), Times.Never);
            handler.Verify(h => h.ExecuteMigration(It.IsAny<Migration>()), Times.Exactly(2));
        }
        [Test]
        public async void create_history_table_and_apply_all_migrations()
        {
            Mock<ISupplyMigrations> source = SetupSource();

            Mock<ICommandDatabases> handler = SetupHandler(new DatabaseVersion { Type = DatabaseVersionType.MissingMigrationHistoryTable });
            var comparer = SetupComparer();
            var migrator = new Migrator(source.Object, comparer.Object, handler.Object);
            await migrator.Migrate();
            handler.Verify(h => h.Create(), Times.Never);
            handler.Verify(h => h.CreateMigrationHistoryTable(), Times.Once);
            handler.Verify(h => h.ExecuteMigration(It.IsAny<Migration>()), Times.Exactly(2));
        }
        [Test]
        public async void apply_all_migrations()
        {
            Mock<ISupplyMigrations> source = SetupSource();

            Mock<ICommandDatabases> handler = SetupHandler(new DatabaseVersion { Type = DatabaseVersionType.VersionNumber,Number = "-1"});
            var comparer = SetupComparer();
            var migrator = new Migrator(source.Object, comparer.Object, handler.Object);
            await migrator.Migrate();
            handler.Verify(h => h.Create(), Times.Never);
            handler.Verify(h => h.CreateMigrationHistoryTable(), Times.Never);
            handler.Verify(h => h.ExecuteMigration(It.IsAny<Migration>()), Times.Exactly(2));
        }
        [Test]
        public async void apply_migrations_not_applied_yet()
        {
            Mock<ISupplyMigrations> source = SetupSource();

            Mock<ICommandDatabases> handler = SetupHandler(new DatabaseVersion { Type = DatabaseVersionType.VersionNumber, Number = "0" });
            var comparer = SetupComparer();
            var migrator = new Migrator(source.Object, comparer.Object, handler.Object);
            await migrator.Migrate();
            handler.Verify(h => h.Create(), Times.Never);
            handler.Verify(h => h.CreateMigrationHistoryTable(), Times.Never);
            handler.Verify(h => h.ExecuteMigration(It.IsAny<Migration>()), Times.Exactly(1));
        }
        [Test]
        public async void apply_no_migrations()
        {
            Mock<ISupplyMigrations> source = SetupSource();

            Mock<ICommandDatabases> handler = SetupHandler(new DatabaseVersion { Type = DatabaseVersionType.VersionNumber, Number = "1" });
            var comparer = SetupComparer();
            var migrator = new Migrator(source.Object, comparer.Object, handler.Object);
            await migrator.Migrate();
            handler.Verify(h => h.Create(), Times.Never);
            handler.Verify(h => h.CreateMigrationHistoryTable(), Times.Never);
            handler.Verify(h => h.ExecuteMigration(It.IsAny<Migration>()), Times.Never);
        }
        private static Mock<ICompareMigrations> SetupComparer()
        {
            var comparer = new Mock<ICompareMigrations>();
            comparer.Setup(c => c.Compare(It.IsAny<Migration>(), It.IsAny<Migration>()))
                .Returns<Migration, Migration>((a, b) => int.Parse(a.Number).CompareTo(int.Parse(b.Number)));
            comparer.Setup(c => c.IsMigrationAfterVersion(It.IsAny<Migration>(), It.IsAny<string>()))
                .Returns<Migration, string>((m, v) => int.Parse(m.Number).CompareTo(int.Parse(v)) > 0);
            return comparer;
        }
    }
}