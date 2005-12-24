#!/usr/bin/perl
#
# genloopclient.pl
#
# This client tests the locking interaction between Clustered Genezzo clients.

use strict;
use warnings;

use Genezzo::GenDBI;
use Genezzo::Contrib::Clustered::Clustered;
use Genezzo::Contrib::Clustered::GLock::GLock;

my $dbh = Genezzo::GenDBI->connect();  # $gnz_home, "NOUSER", "NOPASSWORD");

unless (defined($dbh))
{
    die("could not find database");
}

die("startup failed") unless $dbh->do("startup");

die("failed initial rollback") unless($dbh->do("rollback"));

while(1){
    print STDERR ".";

    if(Genezzo::Contrib::Clustered::GLock::GLock::ast_poll()){
	print STDERR "\n  ast_poll returned true; rolling back\n";
	die("failed rollback") unless($dbh->do("rollback"));
    }

    sleep(1);
}
