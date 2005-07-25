#use 5.008004;
use ExtUtils::MakeMaker;
# See lib/ExtUtils/MakeMaker.pm for details of how to influence
# the contents of the Makefile that is written.
WriteMakefile(
    NAME              => 'Genezzo::Contrib::Clustered',
    VERSION_FROM      => 'lib/Genezzo/Contrib/Clustered/Clustered.pm', # finds $VERSION
    PREREQ_PM         => { FreezeThaw => '0.0', Genezzo::GenDBI => '0.45' }, # e.g., Module::Name => 1.1
    ($] >= 5.005 ?     ## Add these new keywords supported since 5.005
      (ABSTRACT  =>    'Shared Data Cluster support for Genezzo', 
       AUTHOR         => 'Eric Rollins (rollins@acm.org)',
       'EXE_FILES' => [ 'lib/Genezzo/Contrib/Clustered/genprepundo.pl']):()),
);
