$fileToChange = "SqlMigrator.cs"
$clientProjectName = "SqlMigrator"
$rootDir  = Resolve-Path .\
$srcDir = "$rootDir\src\$clientProjectName"	

(Get-Content $srcDir\$fileToChange) | Foreach-Object {
	$_ -replace 'namespace SqlMigrator', 'namespace $rootnamespace$.SqlMigrator' `
	-replace 'using SqlMigrator', 'using $rootnamespace$.SqlMigrator'
	} | Set-Content $srcDir\$fileToChange.pp -Encoding UTF8


