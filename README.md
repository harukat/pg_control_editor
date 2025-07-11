# pg\_control\_editor

pg\_control\_editor is a pg\_control file editor.

This tool can be described as pg\_resetwal minus the ability to reset the WAL.
It is used when you want to modify the control file (e.g. next OID) but do not want to reset the WAL.
It was developed for PostgreSQL 17.x, but can be built and installed on 15.x and 16.x and will probably work.
It is largely made up of a copy of the pg\_resetwal source code.

See the output of `pg\_control\_editor --help` for usage.

