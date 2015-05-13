namespace SqlMigrator.Model
{
    public class DatabaseVersion
    {
        public DatabaseVersionType Type { get; set; }
        public string Number { get; set; }
    }

    public enum DatabaseVersionType
    {
        NotCreated,
        VersionNumber
    }
}