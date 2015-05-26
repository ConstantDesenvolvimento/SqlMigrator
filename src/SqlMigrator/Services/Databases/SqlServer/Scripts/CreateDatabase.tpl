if not exists (select * from master.sys.databases where name='{0}') 
	create database [{0}]
GO
use [{0}]
GO
if not exists (select * from sys.schemas where name='migrations')
	EXEC('CREATE SCHEMA migrations ');
GO
if not exists (select * from sys.tables t inner join  sys.schemas s on s.schema_id=t.schema_id where t.name='log' and s.name='migrations' ) 
	create table migrations.log (number nvarchar(200) primary key clustered,applied datetimeoffset not null )
GO