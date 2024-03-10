Download V9_6:OS_X:B64 START
Download V9_6:OS_X:B64 DownloadSize: 207876206
Download V9_6:OS_X:B64 0% 1% 2% 3% 4% 5% 6% 7% 8% 9% 10% 11% 12% 13% 14% 15% 16% 17% 18% 19% 20% 21% 22% 23% 24% 25% 26% 27% 28% 29% 30% 31% 32% 33% 34% 35% 36% 37% 38% 39% 40% 41% 42% 43% 44% 45% 46% 47% 48% 49% 50% 51% 52% 53% 54% 55% 56% 57% 58% 59% 60% 61% 62% 63% 64% 65% 66% 67% 68% 69% 70% 71% 72% 73% 74% 75% 76% 77% 78% 79% 80% 81% 82% 83% 84% 85% 86% 87% 88% 89% 90% 91% 92% 93% 94% 95% 96% 97% 98% 99% 100% Download V9_6:OS_X:B64 downloaded with 443kb/s
Download V9_6:OS_X:B64 DONE
Extract /Users/lcc/.embedpostgresql/postgresql-9.6.11-1-osx-binaries.zip START
.........................................................................................................................................................................................................................................................................................................................................................................................................................................................................................................................................................................................................................................................................................................................................................................................................................................................................................................................................................................................................................................................................................................................................................................................................................................................................................................................................................................................................................................................Extract /Users/lcc/.embedpostgresql/postgresql-9.6.11-1-osx-binaries.zip DONE
[INFO ] 2024-03-10 19:01:48,616 method:de.flapdoodle.embed.process.runtime.Executable.start(Executable.java:109)
start AbstractPostgresConfig{storage=Storage{dbDir=/var/folders/mq/m_t48f8534gb8m0gp5sqvtq00000gn/T/postgresql-embed-7b6b7b6c-fa83-4144-9c18-a2c3a373702e/db-content-a61a5d42-9c0f-4b61-b1c4-7652e2fcf23a, dbName='postgres', isTmpDir=true}, network=Net{host='localhost', port=5432}, timeout=Timeout{startupTimeout=15000}, credentials=Credentials{username='postgres', password='postgres'}, args=[], additionalInitDbParams=[]}
[WARN ] 2024-03-10 19:01:50,625 method:ru.yandex.qatools.embed.postgresql.PostgresProcess.runCmd(PostgresProcess.java:122)
Possibly failed to run initdb:
The files belonging to this database system will be owned by user "lcc".

This user must also own the server process.



The database cluster will be initialized with locales

COLLATE:  C

CTYPE:    zh_CN.UTF-8

MESSAGES: C

MONETARY: C

NUMERIC:  C

TIME:     C

The default database encoding has accordingly been set to "UTF8".

initdb: could not find suitable text search configuration for locale "zh_CN.UTF-8"

The default text search configuration will be set to "simple".



Data page checksums are disabled.



fixing permissions on existing directory /var/folders/mq/m_t48f8534gb8m0gp5sqvtq00000gn/T/postgresql-embed-7b6b7b6c-fa83-4144-9c18-a2c3a373702e/db-content-a61a5d42-9c0f-4b61-b1c4-7652e2fcf23a ... ok

creating subdirectories ... ok

selecting default max_connections ... 100

selecting default shared_buffers ... 128MB

selecting dynamic shared memory implementation ... posix

creating configuration files ... ok

running bootstrap script ... ok


[INFO ] 2024-03-10 19:01:57,846 method:de.flapdoodle.embed.process.runtime.Executable.start(Executable.java:109)
start AbstractPostgresConfig{storage=Storage{dbDir=/var/folders/mq/m_t48f8534gb8m0gp5sqvtq00000gn/T/postgresql-embed-7b6b7b6c-fa83-4144-9c18-a2c3a373702e/db-content-a61a5d42-9c0f-4b61-b1c4-7652e2fcf23a, dbName='postgres', isTmpDir=true}, network=Net{host='localhost', port=5432}, timeout=Timeout{startupTimeout=15000}, credentials=Credentials{username='postgres', password='postgres'}, args=[postgres], additionalInitDbParams=[]}
[INFO ] 2024-03-10 19:01:58,436 method:de.flapdoodle.embed.process.runtime.Executable.start(Executable.java:109)
start AbstractPostgresConfig{storage=Storage{dbDir=/var/folders/mq/m_t48f8534gb8m0gp5sqvtq00000gn/T/postgresql-embed-7b6b7b6c-fa83-4144-9c18-a2c3a373702e/db-content-a61a5d42-9c0f-4b61-b1c4-7652e2fcf23a, dbName='postgres', isTmpDir=true}, network=Net{host='localhost', port=5432}, timeout=Timeout{startupTimeout=15000}, credentials=Credentials{username='postgres', password='postgres'}, args=[], additionalInitDbParams=[]}
启动集成PG，url=jdbc:postgresql://localhost:5432/postgres?user=postgres&password=postgres
[INFO ] 2024-03-10 19:01:58,557 method:de.flapdoodle.embed.process.runtime.Executable.start(Executable.java:109)
start AbstractPostgresConfig{storage=Storage{dbDir=/var/folders/mq/m_t48f8534gb8m0gp5sqvtq00000gn/T/postgresql-embed-7b6b7b6c-fa83-4144-9c18-a2c3a373702e/db-content-a61a5d42-9c0f-4b61-b1c4-7652e2fcf23a, dbName='postgres', isTmpDir=true}, network=Net{host='localhost', port=5432}, timeout=Timeout{startupTimeout=15000}, credentials=Credentials{username='postgres', password='postgres'}, args=[-U, postgres, -d, postgres, -h, localhost, -p, 5432, -f, /Users/lcc/IdeaProjects/github/calcite-data-manage/09-datasource-access/src/main/resources/pg/data.sql], additionalInitDbParams=[]}
[INFO ] 2024-03-10 19:02:04,279 method:ru.yandex.qatools.embed.postgresql.PostgresProcess.stopInternal(PostgresProcess.java:147)
trying to stop postgresql
[INFO ] 2024-03-10 19:02:04,403 method:de.flapdoodle.embed.process.runtime.Executable.start(Executable.java:109)
start AbstractPostgresConfig{storage=Storage{dbDir=/var/folders/mq/m_t48f8534gb8m0gp5sqvtq00000gn/T/postgresql-embed-7b6b7b6c-fa83-4144-9c18-a2c3a373702e/db-content-a61a5d42-9c0f-4b61-b1c4-7652e2fcf23a, dbName='postgres', isTmpDir=true}, network=Net{host='localhost', port=5432}, timeout=Timeout{startupTimeout=15000}, credentials=Credentials{username='postgres', password='postgres'}, args=[stop], additionalInitDbParams=[]}

[INFO ] 2024-03-10 19:02:05,700 method:de.flapdoodle.embed.process.runtime.ProcessControl.executeCommandLine(ProcessControl.java:231)
execSuccess: false [kill, 93690]