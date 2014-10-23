#!/usr/bin/perl
#
# ModPerlWrap.pm
# Eric Rollins 2005
#
use strict;
use warnings;

package Genezzo::Contrib::Clustered::ModPerlWrap;

use File::Path;
use File::Spec;
use Data::Dumper;
use Genezzo::GenDBI;
use Genezzo::Contrib::Clustered::Clustered;
use Genezzo::Contrib::Clustered::GLock::GLock;
use POSIX;

require Exporter;
our @ISA = ("Exporter");

our @EXPORT = qw(PrintForm StartPage ProcessStmt Rollback Commit 
		 Connect FinishPage);

our $dbh;
our $query_num;
our $dbi_gzerr;

our $processing = 1;
our $need_restart = 0;


sub restart_exit {
    print STDERR "\nExiting on restart_exit\n";
    CORE::exit();
}

sub normal_exit {
    if($need_restart){
	restart_exit();
    }

    $processing = 0;
    # Apache remaps this to ModPerl::Util::exit()
    exit();
}

$dbi_gzerr = sub {
    my %args = (@_);

    return
	unless (exists($args{msg}));

    my $sev = "UNKNOWN";
    
    if (exists($args{severity}))
    {
	$sev = uc($args{severity});
    
	$sev = 'WARNING'
	    if ($sev =~ m/warn/i);

	# don't print 'INFO' 
	if ($args{severity} =~ m/info/i)
	{
	    return;
	}
    }

    # log error
    print STDERR "ERROR $sev", __PACKAGE__, ": ",  $args{msg};

    print "<error>\n";
    print "  <error-severity>";
    print $sev;
    print "</error-severity>\n";
    print "  <error-message>";
    print __PACKAGE__, ": ",  $args{msg};
    print "</error-message>\n";
    print "</error>\n";
};

sub return_error
{
    my ($where) = @_;

    print "  <error-location>", $where, "</error-location>\n";
    print "</results>\n";
    normal_exit();
};

sub sig_handler {
    if(Genezzo::Contrib::Clustered::GLock::GLock::ast_poll()){
	if($processing){
	    $need_restart = 1;  # restart after processing completed
	}else{
	    # message print may not be signal-safe.
	    print STDERR "\nExiting immediately due to lock request\n";
	    # restart_exit doesn't work.  WHY???
	    # restart_exit();     # restart now
	    CORE::exit();
	}
    }
}

BEGIN {
    # Perl "safe" signals prevent signals from being recd during Apache 
    # event loop.
    #$SIG{USR2} = \&sig_handler;
    POSIX::sigaction(SIGUSR2,
                     POSIX::SigAction->new(\&sig_handler))
		     or die "Error setting SIGUSR2 handler: $!\n";

    Genezzo::Contrib::Clustered::GLock::GLock::set_notify();
}

sub Connect {
    my ($gnz_home) = @_;

    if(defined($dbh)){
	return;  # already connected
    }

    $query_num = 0;

    $dbh = Genezzo::GenDBI->connect($gnz_home, 
				    "NOUSER", 
				    "NOPASSWORD",
				    {GZERR => $dbi_gzerr,
				     PrintError => 0,
				     RaiseError => 0});

    if(defined($dbh)){
	$dbh->do("startup"); # start the database

	my $ret = $dbh->do("rollback");  # Grab shared locks.

	if(!defined($ret)){
	    return_error("rollback");
	}
    }
}

use CGI qw(:standard escapeHTML);

sub PrintForm {
    print header();
    print start_html("Genezzo");
    print "  <h2>Enter Genezzo SQL Statement:</h2>\n";
    print "  <form>\n";
    print "    <textarea rows=\"5\" cols=\"60\" name=\"query\"></textarea>\n";
    print "    <p>\n";
    print "    <input type=\"submit\"/>\n";
    print "  </form>\n";
    print end_html();
}

sub StartPage {
    print "Content-type: text/xml\n\n";
    print '<?xml version="1.0" encoding="iso-8859-1"?>';
    print "\n";
    print "<results>\n"; 
    $query_num++;
    print "<query_num>$query_num</query_num>\n";   # to debug mod_perl reuse
}

# For queries prints results as XML.
sub ProcessStmt {
    my ($stmt) = @_;

    if(!defined($dbh)){
	return_error("connect");
    }

    my $sth = $dbh->prepare($stmt);

    if(!defined($sth)){
	return_error("prepare");
    }

    my $ret = $sth->execute();

    if(!defined($ret)){
	$dbh->do("rollback"); 
	return_error("execute");
    }

    my $numFields = $sth->{NUM_OF_FIELDS};

    if(!defined($numFields)){
	return;
    }

    while (1)
    {
	my @ggg = $sth->fetchrow_array();

	last
	    unless (scalar(@ggg));

	print "   <row>\n";
 
	my $i;

	for($i = 0; $i < $numFields; $i++){
	    print "      <", $sth->{NAME}->[$i], ">";
	    print $ggg[$i];
	    print "</", $sth->{NAME}->[$i], ">";
	    print "\n";
	}

	print "   </row>\n";
    }

    # Can fatal errors occur during fetch, requiring rollback?
}

sub Rollback {
    my $ret = $dbh->do("rollback");
    
    if(!defined($ret)){
	return_error("rollback");
    }
}

sub Commit {
    my $ret = $dbh->do("commit");
    
    if(!defined($ret)){
	return_error("commit");
    }
}

sub FinishPage {    
    print "</results>\n";
    normal_exit();
}

1;

__END__

=head1 NAME

Genezzo::Contrib::Clustered::ModPerlWrap - Mod Perl wrappers for Genezzo

=head1 SYNOPSIS
    
    StartPage();
    Connect("/dev/raw");
    ProcessStmt("insert into t1 values (10, 'test10')");
    ProcessStmt("insert into t1 values (11, 'test11')");
    Commit();
    FinishPage();

or

    StartPage();
    Connect("/dev/raw");
    ProcessStmt("select * from t1");
    FinishPage();

=head1 DESCRIPTION

The Apache web server is used to provide multi-user XML over HTTP access 
to the Clustered Genezzo database.  A page containing multiple SQL statements
acts much like a stored procedure.  The web page
parameters are used like stored procedure parameters, and the
processing on the page forms the transaction boundary.

Note control flow on page does not continue after errors or FinishPage().

See genezzo_form.pl for examples.

=head1 FUNCTIONS

=over 4

=item Connect GNZ_HOME

Connects to Clustered Genezzo database with home GNZ_HOME.  Only performed
once per process.

=item PrintForm

Prints form which can be used to sent SQL statements to web server.

=item StartPage

Prints initial HTML and XML at beginning of response.

=item ProcessStmt STMT

Processes SQL statement.  Result rows are wrapped in XML and printed.
On execute errors automatically does rollback and ends page processing.

=item Rollback

Rolls back transaction.  Often unnecessary as ProcessStmt execute errors are
automatically rolled back.

=item Commit

Commits transaction.

=item Finish Page

Prints final closing XML tags and ends page processing.  Note control flow
does not continue beyond this point.

=back

=head2 EXPORT

none

=head1 LIMITATIONS

This is pre-alpha software; don't use it to store any data you hope
to see again!

=head1 SEE ALSO

L<http://www.genezzo.com>

L<http://eric_rollins.home.mindspring.com/genezzo/ClusteredGenezzoDesign.html>

L<http://eric_rollins.home.mindspring.com/genezzo/cluster.html>

=head1 AUTHOR

Eric Rollins, rollins@acm.org

=head1 COPYRIGHT AND LICENSE

    Copyright (C) 2005 by Eric Rollins.  All rights reserved.

    This program is free software; you can redistribute it and/or modify
    it under the terms of the GNU General Public License as published by
    the Free Software Foundation; either version 2 of the License, or
    any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU General Public License for more details.

    You should have received a copy of the GNU General Public License
    along with this program; if not, write to the Free Software
    Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA  02110-1301  USA

Address bug reports and comments to rollins@acm.org

=cut
