#!/usr/bin/perl
#
# genprepundo.pl
#
# Initialize undo file for Clustered Genezzo
#
#
#use strict;
use Genezzo::GenDBI;
use Getopt::Long;
use Data::Dumper;
use Carp;
use Genezzo::Dict;
use Genezzo::Block::RDBlock;
use Genezzo::Block::Std;
use FreezeThaw;
use Genezzo::Util;
use Pod::Usage;

=head1 NAME

genprepundo.pl - Prepare undo file for Clustered Genezzo

=head1 SYNOPSIS

B<genprepundo.pl> [options]

Options:

    -help            brief help message
    -man             full documentation
    -gnz_home        supply a directory for the gnz_home
    -undo_filename   name of the undo file

=head1 OPTIONS

=over 8

=item B<-help>

    Print a brief help message and exits.

=item B<-man>
    
    Prints the manual page and exits.

=item B<-gnz_home>
    
    Supply the location for the gnz_home installation.  If 
    specified, it overrides the GNZ_HOME environment variable.

=item B<-undo_filename>

    Supply the name of the undo file.  If not specified, it
    defaults to undo.und for file system devices.  It must
    be specified for raw devices.

=back

=head1 DESCRIPTION

  Creates or re-initializes undo file under GNZ_HOME.  By default
  file is named undo.und.  File header contains basic information about
  all other files in Genezzo installation.  genprepundo.pl must
  be run whenever a new file is added to the Genezzo installation.

  Undo file format is documented in Clustered.pm.

=head1 TODO
  
  Expand command-line argument support (numprocesses, blocks_per_process)

=head1 AUTHOR

  Eric Rollins, rollins@acm.org

=head1 COPYRIGHT AND LICENSE

  Copyright (c) 2005 Eric Rollins.  All rights reserved.

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

our $GZERR = sub {
    my %args = (@_);

    return 
        unless (exists($args{msg}));

    my $warn = 0;
    if (exists($args{severity}))
    {
        my $sev = uc($args{severity});
        $sev = 'WARNING'
            if ($sev =~ m/warn/i);

        # don't print 'INFO' prefix
        if ($args{severity} !~ m/info/i)
        {
            printf ("%s: ", $sev);
            $warn = 1;
        }

    }
    print $args{msg};
    # add a newline if necessary
    print "\n" unless $args{msg}=~/\n$/;
#    carp $args{msg}
#      if (warnings::enabled() && $warn);
    
};

my $glob_init;
my $glob_gnz_home;
my $glob_shutdown; 
my $glob_id;
my $glob_defs;
my $glob_undo_filename;
my $glob_procs = 256;			# number of processes supported
my $glob_blocks_per_proc = 220;		# undo blocks per process

sub setinit
{
    $glob_init     = shift;
    $glob_gnz_home = shift;
    $glob_shutdown = shift;
    $glob_defs     = shift;
    $glob_undo_filename = shift;
}

BEGIN {
    my $man  = 0;
    my $help = 0;
    my $init = 0;
    my $shutdown = 0;
    my $gnz_home = '';
    my $undo_filename = '';
    my %defs = ();      # list of --define key=value

    GetOptions(
               'help|?' => \$help, man => \$man, 
               'gnz_home=s' => \$gnz_home,
	       'undo_filename=s' => \$undo_filename)
        or pod2usage(2);

    $glob_id = "Genezzo Version $Genezzo::GenDBI::VERSION - $Genezzo::GenDBI::RELSTATUS $Genezzo::GenDBI::RELDATE\n\n"; 

    pod2usage(-msg => $glob_id, -exitstatus => 1) if $help;
    pod2usage(-msg => $glob_id, -exitstatus => 0, -verbose => 2) if $man;

    setinit($init, $gnz_home, $shutdown, \%defs, $undo_filename);

    print "beginning initialization...\n" ;
}

my $dbh = Genezzo::GenDBI->connect($glob_gnz_home,
                                   "NOUSER",
                                   "NOPASSWORD",
                                   {GZERR => $dbi_gzerr});

if(!defined($dbh)){
  Carp::croak("failed connect");
}

$dbh->do("startup"); # start the database

#-----------------------------------------------------------------
# locate home
$stmt =    "select pref_value from _pref1 where pref_key='home'";

$sth = $dbh->prepare($stmt);
	   

if(!defined($sth)){
    Carp::croak("failed prepare 2");
}

$ret = $sth->execute();

if(!defined($ret)){
    Carp::croak("failed execute 2");
}

$ggg = $sth->fetchrow_hashref();

if(!defined($ggg)){
    Carp::croak("zero rows 2");
}

my $pref_home = $ggg->{pref_value};

if ($glob_undo_filename eq ""){
    if($pref_home eq "/dev/raw"){
	Carp::croak("raw device undo name required");
    }else{
	$glob_undo_filename = "undo.und";
    }
}

#-----------------------------------------------------------------
# read per-file info
my $stmt =    "select fileidx, filename, blocksize, numblocks from _tsfiles";

my $sth = $dbh->prepare($stmt);
	   

if(!defined($sth)){
    Carp::croak("failed prepare");
}

my $ret = $sth->execute();

if(!defined($ret)){
    Carp::croak("failed execute");
}

$data = {
    "procs" => $glob_procs,
    "blocks_per_proc" => $glob_blocks_per_proc,
    "files" => {}
    };

while(1)
{
    my $ggg = $sth->fetchrow_hashref();

    last
	unless(defined($ggg));

    # look up header size in file FIXME
    my $full_filename;

    if($pref_home eq "/dev/raw"){
	$full_filename = "$pref_home/$ggg->{filename}";
    }else{
	$full_filename = "$pref_home/ts/$ggg->{filename}";
    }

    $ggg->{full_filename} = $full_filename;

    my $fh;
    open($fh, "<$full_filename")
	or die "open $full_filename failed: $!\n";

    my ($hdrsize, $version, $blocksize, $h1) = 
	Genezzo::Util::FileGetHeaderInfo($fh, $full_filename);

    $ggg->{hdrsize} = $hdrsize;

    $data->{files}->{$ggg->{fileidx}} = $ggg;
}

#-----------------------------------------------------------------
# use both insert and update to set undo_filename
$stmt = 
  "update _pref1 set pref_value='$glob_undo_filename' where pref_key='undo_filename'";

$sth = $dbh->prepare($stmt);
	   
if(!defined($sth)){
    Carp::croak("failed prepare 3");
}

$ret = $sth->execute();

if(!defined($ret)){
    Carp::croak("failed execute 3")
}

if($ret != 1){
    my $time = Genezzo::Dict::time_iso8601();

    $stmt = "insert into _pref1 (pref_key, pref_value, creationdate) values " .
	"('undo_filename', '$glob_undo_filename','$time')";

    $sth = $dbh->prepare($stmt);
	   
    if(!defined($sth)){
	Carp::croak("failed prepare 3");
    }

    $ret = $sth->execute();

    if(!defined($ret)){
	Carp::croak("failed execute 3");
    } 
}

$dbh->do("commit");

my $full_filename;

if($pref_home eq "/dev/raw"){
    $full_filename = "$pref_home/$glob_undo_filename";
}else{
    $full_filename = "$pref_home/ts/$glob_undo_filename";
}

#-----------------------------------------------------------------
# store per-file info in file header block
$frozen_data = FreezeThaw::freeze $data;

# construct an empty byte buffer
my $blocksize = $Genezzo::Block::Std::DEFBLOCKSIZE; 
my $buff = "\0" x $blocksize;

my %tied_hash = ();

my $tie_val = 
    tie %tied_hash, 'Genezzo::Block::RDBlock', (refbufstr => \$buff);

my $newkey = $tie_val->HPush($frozen_data);

my $fh;
open($fh, ">$full_filename")
    or die "open $full_filename failed: $!\n";

gnz_write ($fh, $buff, $blocksize)
    == $blocksize
    or die "bad write - file $full_filename : $! \n";

#-----------------------------------------------------------------
# mark process status block for each process as no outstanding transaction
my $buff;
my $i;
for($i = 0; $i < $glob_procs; $i++){
    $buff = "-" x 10;
    my $procstr = sprintf("%10d", $i);
    $buff = $buff . $procstr;
    $buff = $buff . ( "-" x ($blocksize - 20) );

    gnz_write ($fh, $buff, $blocksize)
	== $blocksize
	or die "bad write - file $full_filename ($i): $! \n";
}

close $fh;

print "finished initialization of $full_filename.\n" ;

