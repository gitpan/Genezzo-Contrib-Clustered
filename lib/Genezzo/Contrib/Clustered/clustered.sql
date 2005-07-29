REM prepare database for Genezzo::Contrib::Clustered
REM @havok.sql and @syshook.sql first
insert into sys_hook (xid, pkg, hook, replace, xtype, xname, args, owner, creationdate, version) values (1000, 'Genezzo::BufCa::BCFile', 'ReadBlock', 'ReadBlock_Hook', 'require', 'Genezzo::Contrib::Clustered::Clustered', 'ReadBlock', 'SYSTEM', '2005-07-25T12:12', '1');
insert into sys_hook (xid, pkg, hook, replace, xtype, xname, args, owner, creationdate, version) values (1001, 'Genezzo::BufCa::DirtyScalar', 'STORE', 'DirtyBlock_Hook', 'require', 'Genezzo::Contrib::Clustered::Clustered', 'DirtyBlock', 'SYSTEM', '2005-07-25T12:12', '1');
insert into sys_hook (xid, pkg, hook, replace, xtype, xname, args, owner, creationdate, version) values (1002, 'Genezzo::GenDBI', 'Kgnz_Commit', 'Commit_Hook', 'require', 'Genezzo::Contrib::Clustered::Clustered', 'Commit', 'SYSTEM', '2005-07-25T12:12', '1');
insert into sys_hook (xid, pkg, hook, replace, xtype, xname, args, owner, creationdate, version) values (1003, 'Genezzo::GenDBI', 'Kgnz_Rollback', 'Rollback_Hook', 'require', 'Genezzo::Contrib::Clustered::Clustered', 'Rollback', 'SYSTEM', '2005-07-25T12:12', '1');
commit;
shutdown;
REM restart gendba.pl from command line, so havok routines won't be redefined
quit;

