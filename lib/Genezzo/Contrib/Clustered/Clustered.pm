#
# copyright (c) 2005, Eric Rollins, all rights reserved, worldwide
#
#

package Genezzo::Contrib::Clustered::Clustered;

#use 5.008004;
use strict;
use warnings;
use Genezzo::Util;
#use Genezzo::Contrib::Clustered::GLock::GTXLock;
use Data::Dumper;
use FreezeThaw;
use IO::File;
use Genezzo::Block::RDBlock;
use warnings::register;

our $VERSION = '0.05';

our $init_done;

our $ReadBlock_Hook;
our $DirtyBlock_Hook;
our $Commit_Hook;
our $Rollback_Hook;

# since we are single-threaded anyway, lets create our own context
# here
our $cl_ctx;
our $undo_file;

# since we are single-threaded, for now use this flag to avoid
# processing write to buffer during read
our $inReadBlock = 0;

# constant blocks 
our $committed_buff;
our $rolledback_buff;
our $pending_buff;
our $clear_buff;

our $committed_code;
our $rolledback_code;
our $pending_code;
our $clear_code;

our $undo_blocksize;

####################################################################
# wraps Genezzo::BufCa::BCFile::ReadBlock
sub ReadBlock
{
    my @tmpArgs = @_;
    my $self = shift @tmpArgs;
    my %args = (@tmpArgs);
    my $fnum = $args{filenum};
    my $bnum = $args{blocknum};
    whisper "Genezzo::Contrib::Clustered::ReadBlock(filenum => $fnum, blocknum => $bnum)\n";

    if(!defined($init_done) || !$init_done){
        _init();
    }

    #my $gtxLock = $cl_ctx->{gtxLock};
    # add in fnum later...
    #$gtxLock->lock(lock => $bnum, shared => 1);
    
    # avoid processing DirtyBlock during read
    $inReadBlock = 1;
    my $ret = &$ReadBlock_Hook(@_);
    $inReadBlock = 0;

    return $ret;
}

####################################################################
# wraps Genezzo::BufCa::DirtyScalar::STORE
sub DirtyBlock
{
    my $h;
    my $dirty;

    if($_[0]->{bce}){
        $h = $_[0]->{bce}->GetInfo();
        $dirty = $_[0]->{bce}->_dirty();
    }

    if(!defined($init_done) || !$init_done){
        _init();
    }
 
    if(!$inReadBlock &&
       defined($h) && 
        ((!(defined($h->{filenum}))) || (!(defined($h->{blocknum})))) &&
        !$dirty)
    {
        whisper "Genezzo::Contrib::Clustered::DirtyBlock bad undefined\n";
    }

    if(!$inReadBlock &&
       defined($h) && 
       defined($h->{filenum}) && defined($h->{blocknum}))
    {
        whisper "Genezzo::Contrib::Clustered::DirtyBlock(filenum => $h->{filenum}, blocknum => $h->{blocknum}, dirty => $dirty)\n";

        if(!($cl_ctx->{have_begin_trans})){
	    BeginTransaction();
	    $cl_ctx->{have_begin_trans} = 1;
	}

        if($dirty == 0){
            #my $gtxLock = $cl_ctx->{gtxLock};
	    # add in fnum later...
	    #$gtxLock->lock(lock => $h->{blocknum}, shared => 0);

	    CopyBlockToOrFromTail($h->{filenum}, $h->{blocknum}, "TO");
	    AddAndWriteUndo($h->{filenum}, $h->{blocknum});

	    # add undo proc id entry to block in buffer cache (how?) TODO
        }
    }

    return &$DirtyBlock_Hook(@_);
}

####################################################################
#wraps Genezzo::GenDBI::Kgnz_Commit
sub Commit
{
    whisper "Genezzo::Contrib::Clustered::Commit()\n";

    my @tmpArgs = @_;
    my $self = shift @tmpArgs;

    if(!defined($init_done) || !$init_done){
        _init();
    }

    # assume this writes all blocks in buffer cache to disk
    my $ret = &$Commit_Hook(@_);

    WriteTransactionState($committed_buff);

    # use undo to find all affected blocks and now clear undo proc id in
    # all of them and write them all again (with sync)

    # release all blocks in buffer cache (how?)

    #my $gtxLock = $cl_ctx->{gtxLock};
    #$gtxLock->unlockAll();

    ResetUndo();
    WriteTransactionState($clear_buff);
    $cl_ctx->{have_begin_trans} = 0;

    return $ret;
}

####################################################################
# wraps Genezzo::GenDBI::Kgnz_Rollback
sub Rollback
{
    whisper "Genezzo::Contrib::Clustered::Rollback()\n";

    my @tmpArgs = @_;
    my $self = shift @tmpArgs;

    if(!defined($init_done) || !$init_done){
        _init();
    }

    WriteTransactionState($rolledback_buff);

    Rollback_Internal();

    ResetUndo();
    WriteTransactionState($clear_buff);
    $cl_ctx->{have_begin_trans} = 0;

    #my $gtxLock = $cl_ctx->{gtxLock};
    #$gtxLock->unlockAll();

    # currently this generates lots of writes, and another transaction
    # which is never committed.  Investigate.
    my $ret = &$Rollback_Hook(@_);

    # for now, we never want to rollback this extra info, so
    ResetUndo();
    WriteTransactionState($clear_buff);
    $cl_ctx->{have_begin_trans} = 0;

    return $ret;
}

####################################################################
# what about locking?
sub Rollback_Internal()
{
    whisper "beginning Rollback_Internal()\n";
    # for each block in undo, for each row in block, replace disk contents
    my $undo_blockid;
    my $tx_id;

    for($undo_blockid = 0; 
	$undo_blockid < ($cl_ctx->{data}->{blocks_per_proc})/2;
	$undo_blockid++)
    {
        whisper "rollback internal undo_blockid = $undo_blockid\n";
	# utilize paired blocks later...
	my $offset = $undo_blockid * 2;
	
	my $blk = ($cl_ctx->{proc_undo_blocknum} + $offset)*$undo_blocksize;
	$undo_file->sysseek($blk, 0)
	    or die "bad seek - file undo block $blk: $! \n";

	my $buff;

	Genezzo::Util::gnz_read ($undo_file, $buff, $undo_blocksize)
	    == $undo_blocksize
	    or die 
            "bad read - file undo : block $blk : $! \n";
    
	my %tied_hash = ();
	my $tie_val =
        tie %tied_hash, 'Genezzo::Block::RDBlock', (refbufstr => \$buff);

	my $frozen_row = $tied_hash{1}; 
	my ( $row ) = FreezeThaw::thaw $frozen_row;
	
	if($undo_blockid == 0){
	    $tx_id = $row->{tx};
	}else{
	    if($tx_id != $row->{tx}){
		last;
	    }
	}

	my $rownum = 2;

	while(1){
	    $frozen_row = $tied_hash{$rownum};

	    if(!defined($frozen_row)){
		last;
	    }

	    ( $row ) = FreezeThaw::thaw $frozen_row;

	    CopyBlockToOrFromTail($row->{f}, $row->{b}, "FROM");
	    $rownum++;
	}

	if($rownum == 2){
	    # block is empty, so don't go on to next
	    last;
	}
    }

    whisper "finished Rollback_Internal()\n";
}


# copies before image of block to end of file
# direction TO:    copy from body to tail
#           FROM:  copy from tail to body
sub CopyBlockToOrFromTail
{
    my ($fileno, $blockno, $direction) = @_;

    whisper "CopyBlockToOrFromTail $direction\n";

    my $fh = $cl_ctx->{open_files}->{$fileno};
    my $full_filename = $cl_ctx->{data}->{files}->{$fileno}->{full_filename};

    if(!defined($fh)){
        whisper "opening $fileno\n";

        $fh = new IO::File "+<$full_filename"
            or die "open $full_filename failed: $!\n";

	$cl_ctx->{open_files}->{$fileno} = $fh;
    }

    my $file_blocksize = $cl_ctx->{data}->{files}->{$fileno}->{blocksize};
    my $file_numblocks = $cl_ctx->{data}->{files}->{$fileno}->{numblocks};
    my $file_hdrsize = $cl_ctx->{data}->{files}->{$fileno}->{hdrsize};

    my $src_offset;
    my $dst_offset;

    if($direction eq "TO"){
        $src_offset = $file_hdrsize + ($file_blocksize * $blockno);
	# not clear if numblocks includes header; lets be safe
	$dst_offset = $src_offset + ($file_numblocks * $file_blocksize);
    }elsif($direction eq "FROM"){
        $dst_offset = $file_hdrsize + ($file_blocksize * $blockno);
	# not clear if numblocks includes header; lets be safe
	$src_offset = $dst_offset + ($file_numblocks * $file_blocksize);
    }else{
        die "invalid direction $direction in CopyBlockToOrFromTail";
    }

    $fh->sysseek ($src_offset, 0 )
        or die "bad seek - file $full_filename : src $src_offset : $!";

    my $buf;

    Genezzo::Util::gnz_read ($fh, $buf, $file_blocksize)
        == $file_blocksize
        or die 
            "bad read - file $full_filename : src $src_offset : $! \n";

    $fh->sysseek ($dst_offset, 0 )
        or die "bad seek - file $full_filename : dst $dst_offset : $!";

    Genezzo::Util::gnz_write ($fh, $buf, $file_blocksize)
	== $file_blocksize
        or die 
	"bad write - file $full_filename : dst $dst_offset : $! \n";

    #if($direction eq "TO"){
        $fh->sync;
    #}
}

sub WriteTransactionState{
    my ($state_buff) = @_;

    my $blk = $cl_ctx->{proc_state_blocknum}*$undo_blocksize;
    $undo_file->sysseek($blk, 0)
	or die "bad seek - file undo block $blk: $! \n";
    Genezzo::Util::gnz_write($undo_file, $state_buff, $undo_blocksize)
	or die "bad write - file undo block $blk: $! \n";
    $undo_file->sync;
}

# returns single character code
sub ReadTransactionState(){
    my $buf;

    my $blk = $cl_ctx->{proc_state_blocknum}*$undo_blocksize;
    $undo_file->sysseek($blk, 0)
	or die "bad seek - file undo block $blk: $! \n";
    Genezzo::Util::gnz_read($undo_file, $buf, $undo_blocksize)
	or die "bad write - file undo block $blk: $! \n";

    return substr($buf,0,1);
}

sub ResetUndo
{
    $cl_ctx->{tx_id} = $cl_ctx->{tx_id} + 1;

    $cl_ctx->{current_undo_blockid} = 0;

    # create empty undo block
    CreateUndoBlock();
    # write it out
    WriteUndoBlock();
}

sub BeginTransaction
{
    whisper "Genezzo::Contrib::Clustered::BeginTransaction\n";
    # increment transaction id
    $cl_ctx->{tx_id} = $cl_ctx->{tx_id} + 1;
    # mark transaction pending
    WriteTransactionState($pending_buff);

    $cl_ctx->{current_undo_blockid} = 0;

    # create empty undo block
    CreateUndoBlock();
    # write it out
    WriteUndoBlock();
}    
   
sub CreateUndoBlock
{
    my $buff = "\0" x $undo_blocksize;
    my %tied_hash = ();
    my $tie_val =
        tie %tied_hash, 'Genezzo::Block::RDBlock', (refbufstr => \$buff);
    $cl_ctx->{current_undo_block} = $tie_val;
    $cl_ctx->{current_undo_block_buf} = \$buff;
    # add tx id
    # this should be metadata; for now store it as 1st row
    my $row = { "tx" => $cl_ctx->{tx_id} };
    my $frozen_row = FreezeThaw::freeze $row;
    $cl_ctx->{current_undo_block}->HPush($frozen_row);
}

sub AddAndWriteUndo
{
    whisper "AddAndWriteUndo\n";
    my ($fileno, $blockno) = @_;
    my $row = { "f" => $fileno,
                "b" => $blockno };
    my $frozen_row = FreezeThaw::freeze $row;
    my $newkey = $cl_ctx->{current_undo_block}->HPush($frozen_row);
   
    if(defined($newkey)){
        WriteUndoBlock();
	return;
    }

    # current block is full (and already written)
    # create new block
    CreateUndoBlock();
    # move to next block
    $cl_ctx->{current_undo_blockid} = $cl_ctx->{current_undo_blockid} + 1;
    my $offset = $cl_ctx->{current_undo_blockid}*2;

    if(($offset) >= ($cl_ctx->{data}->{blocks_per_proc}-1))
    {
        die("Undo Full:  undo offset $offset >= block_per_proc $cl_ctx->{data}->{blocks_per_proc} - 1\n");
    }

    $newkey = $cl_ctx->{current_undo_block}->HPush($frozen_row);
    WriteUndoBlock();
}

sub WriteUndoBlock
{
    whisper "write undo block\n";
    # note paired blocks means we multiply blockid by 2
    my $offset = $cl_ctx->{current_undo_blockid} * 2;

    if($offset >= ($cl_ctx->{data}->{blocks_per_proc}-1)){
        die("Undo Full:  undo offset $offset >= block_per_proc $cl_ctx->{data}->{blocks_per_proc} - 1\n");
    }
    
    my $blk = ($cl_ctx->{proc_undo_blocknum} + $offset)*$undo_blocksize;
    $undo_file->sysseek($blk, 0)
	or die "bad seek - file undo block $blk: $! \n";

    my $bp = $cl_ctx->{current_undo_block_buf};
    Genezzo::Util::gnz_write($undo_file, $$bp, $undo_blocksize)
        == $undo_blocksize
	or die "bad write of undo to undo : $! \n";
    # write it again to block+1 (paired writes)
    Genezzo::Util::gnz_write($undo_file, $$bp, $undo_blocksize)
        == $undo_blocksize
        or die "bad write (2) of undo to undo : $! \n";
    $undo_file->sync;
}

sub InitConstBuff(\$$)
{
    my ($b, $code) = @_;

    my $buff = $code;
    $buff = $buff. ("-" x 9);
    my $procstr = sprintf("%10d", $cl_ctx->{proc_num});
    $buff = $buff . $procstr;
    $buff = $buff . ( "-" x ($undo_blocksize - 20) );
    $$b = $buff;
}

sub _init
{
    if(defined($init_done) && $init_done){
        return;
    }

    whisper "Genezzo::Contrib::Clustered::_init called\n";

    $cl_ctx = {};
    $inReadBlock = 0;

    $committed_buff = "";
    $rolledback_buff = "";
    $pending_buff = "";
    $clear_buff = "";

    $committed_code = "C";
    $rolledback_code = "R";
    $pending_code = "P";
    $clear_code = "-";

    $undo_blocksize = $Genezzo::Block::Std::DEFBLOCKSIZE;

    my $full_filename;

if(0){
    my $dict = $Genezzo::Dict::the_dict;
    my $undo_filename = $dict->{prefs}->{undo_filename};

    my $fhts;   # gnz_home table space

    if(getUseRaw()){
        $fhts = $dict->{gnz_home};
    }else{
        $fhts = File::Spec->catdir($dict->{gnz_home}, "ts");
    }

    $full_filename =
        File::Spec->rel2abs(
            File::Spec->catfile($fhts, $undo_filename));
}else{
    # FIXME super hack -- need global $Genezzo::Dict::the_dict, above,
    # to get gnz_home, etc.  for now hard code
    if(getUseRaw()){
	$full_filename = "/dev/raw/raw2";
    }else{
	$full_filename = "$ENV{HOME}/gnz_home/ts/undo.und";
    }
}

    $undo_file = new IO::File "+<$full_filename"
        or die "sysopen $full_filename failed: $!\n";

    #construct an empty byte buffer
    my $buff;

    Genezzo::Util::gnz_read($undo_file, $buff, $undo_blocksize) 
        == $undo_blocksize
    	or die "bad read - file $full_filename: $!\n";
    
    my %tied_hash = ();
    my $tie_val =
        tie %tied_hash, 'Genezzo::Block::RDBlock', (refbufstr => \$buff);
    
    my $frozen_data = $tied_hash{1};
    my ( $data ) = FreezeThaw::thaw $frozen_data;
    $cl_ctx->{data} = $data;

    # TODO: read proc_num from somewhere later
    $cl_ctx->{proc_num} = 0;
    $cl_ctx->{proc_state_blocknum} = $cl_ctx->{proc_num} + 1;  # 1 for data
    $cl_ctx->{proc_undo_blocknum} = 
        1 + $cl_ctx->{data}->{procs} + 
	($cl_ctx->{data}->{blocks_per_proc} * $cl_ctx->{proc_num});
    InitConstBuff($committed_buff, $committed_code);
    InitConstBuff($rolledback_buff, $rolledback_code);
    InitConstBuff($pending_buff, $pending_code);
    InitConstBuff($clear_buff, $clear_code);

    # hashed on fileno
    $cl_ctx->{open_files} = {};

    #my $gtxLock = Genezzo::Contrib::Clustered::GLock::GTXLock->new();
    #$cl_ctx->{gtxLock} = $gtxLock;

    my $tx_state = ReadTransactionState();

    if(($tx_state eq $pending_code) || ($tx_state eq $rolledback_code)){
	print "rollback at startup necessary!\n";
	Rollback_Internal();
	# what about restarting???
	print "PLEASE TYPE ROLLBACK COMMAND\n";
	# print "FOLLOWED BY COMMIT COMMAND\n";
	# note here no rollback work will occur, but system will restart
	# from disk (I hope)
        # currently wrong, actually lots of work is done and leaves open tx
        # that needs committing
	# but it is ignored since we mark 'clear' at end
    }elsif($tx_state eq $committed_code){
	# need to clear undo proc id in blocks...
    }

    $cl_ctx->{tx_id} = 0;	# TODO:  add timestamp 

    whisper "begin init undo\n";
    # init all undo blocks
    CreateUndoBlock();
    my $tmp_undo_blockid;

    for($tmp_undo_blockid = 0; 
	$tmp_undo_blockid < ($cl_ctx->{data}->{blocks_per_proc}/2);
	$tmp_undo_blockid++)
    {
	$cl_ctx->{current_undo_blockid} = $tmp_undo_blockid;
	WriteUndoBlock();
    }

    whisper "end init undo\n";

    $cl_ctx->{tx_id} = 1;	# TODO:  add timestamp 
    WriteTransactionState($clear_buff);
    $cl_ctx->{have_begin_trans} = 0;

    whisper "Genezzo::Contrib::Clustered::_init finished\n";
    $init_done = 1;
}

BEGIN
{
    print "Genezzo::Contrib::Clustered will be installed\n"; 
}

1;
__END__

=head1 NAME

Genezzo::Contrib::Clustered::Clustered - Shared data cluster support for Genezzo

=head1 SYNOPSIS

  genprepundo.pl

  gendba.pl
  >@havok.sql
  >@syshook.sql
  >@clustered.sql

=head1 DESCRIPTION

Genezzo is an extensible database with SQL and DBI.  It is written in Perl.
Basic routines inside Genezzo are overridden via Havok SysHooks.  Override
routines will (eventually) provide support for shared data clusters.  Routines
will provide transactions, distributed locking, undo, and recovery.  Today
these routines support a single-user single-threaded database, and provide
basic transactional commit and rollback via undo.  Locking routines are
currently stubs.

=head2 Undo File Format

All blocks are $Genezzo::Block::Std::DEFBLOCKSIZE 

=head3 Header 
  
  (block 0)

Frozen data structure stored via Genezzo::Block::RDBlock->HPush()

  {
     "procs" => $processes,
     "blocks_per_proc" => $blocks_per_process,
     "files" => {
	 per fileidx =>
	 { fileidx, filename, full_filename, blocksize, numblocks, hdrsize }
     }
  };

=head3 Process Status Block 

  (block 1 to $processes+1)

 ----------processid(10)---------------- to end of block

 1st character is status:

    - = clear
    C = committed
    R = rolledback
    P = pending

=head3 Undo Blocks 

  (array of $blocks_per_process * $processes)

These are written paired (for recoverability), so only half number is 
actually available.

Undo blocks contain multiple rows.  1st row is {"tx"}, a transaction id.
following rows are {"f" = $fileno, "b" = $blockno}.  All are
Frozen data structures stored via Genezzo::Block::RDBlock->HPush().

The list of fileno/blockno indicate which blocks should be replaced if
the transaction rolls back, or which blocks should have the process id
cleared (not yet implemented) if the transaction commits.

At process startup undo blocks for the process are initially all written 
with tx 0, so we can distinguish when we move to a block left over from 
a previous transaction.

=head2 Before-Image Block Storage

The before image of each block is written at the tail of the file where
it originates, at position $declared_file_length + $blocknum.  So when
this module is enabled data files actually grow to twice their declared
size.  Note dynamic data file growth (increase_by) is not supported 
with this module.

=head1 FUNCTIONS

=over 4

=item ReadBlock

 Wraps Genezzo::BufCa::BCFile::ReadBlock

=item DirtyBlock

 Wraps Genezzo::BufCa::DirtyScalar::STORE

=item Commit

 Wraps Genezzo::GenDBI::Kgnz_Commit

=item Rollback

 Wraps Genezzo::GenDBI::Kgnz_Rollback

=back

=head2 EXPORT

  none

=head1 LIMITATIONS

  No Distributed/clustered functionality today.  Still single machine, 
  single process, single user, single threaded.

  This is pre-alpha software; don't use it to store any data you hope
  to see again!

=head1 SEE ALSO

For more information, please visit the Genezzo homepage 
at L<http://www.genezzo.com>

also L<http://eric_rollins.home.mindspring.com/genezzo/ClusteredGenezzoDesign.html>
and L<http://eric_rollins.home.mindspring.com/genezzo/cluster.html>

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
