#!/usr/bin/perl
#
# copyright (c) 2005, Eric Rollins, all rights reserved, worldwide
#
#
#

package Genezzo::Contrib::Clustered::GLock::GLock;

use Carp;
use strict;
use warnings;

our $LOCKER = 1;    # IPC::Locker
our $DLM =    2;    # opendlm
our $NONE =   3;    # no locking (still error check)
our $IMPL = $NONE;  # this should be $NONE in distribution

if($IMPL == $LOCKER){
  require IPC::Locker;
}elsif($IMPL == $DLM){
  require Genezzo::Contrib::Clustered::GLock::GLockDLM;
}

# options lock:  lockName
#         block: 1 for blocking (default)
sub new {
  @_ >= 1 or croak 'usage:  GLock->new({options})';
  my $proto = shift;
  my $tmp = { @_,};
  my $lockName = $tmp->{lock};
  $lockName = "lock" if !defined($lockName);
  my $block = $tmp->{block};
  $block = 1 if !defined($block);
  my $class = ref($proto) || $proto;
  my $self = {};
  $self->{block} = $block;
  $self->{lock} = $lockName;

  if($IMPL == $LOCKER){
    my $l = IPC::Locker->new(lock => $lockName, block => $block,
    	timeout => 0);
    $self->{impl} = $l;
  }elsif($IMPL == $DLM){
  }

  bless $self, $class;
  return $self;
}

# options:  shared:     1 for shared lock 
# 			0 for exclusive (default)
# returns undef for failure
sub lock{
  my $self = shift;

  my $tmp = { @_,};
  my $shared = $tmp->{shared};
  $shared = 0 if !defined($shared);

  if(defined($self->{"shared"})){
    croak "GLock::lock():  lock $self->{lock} already locked in mode $self->{shared}";
  }

  $self->{shared} = $shared;

  if($IMPL == $LOCKER){
    my $l = $self->{impl}; 
    return $l->lock();
  }elsif($IMPL == $DLM){
    my $l = Genezzo::Contrib::Clustered::GLock::GLockDLM::dlm_lock(
        $self->{lock},$shared,$self->{block});

    if($l == 0){
      $l = undef;
    }

    $self->{impl} = $l;
    return $l;
  }else{
    return 1;
  }

  return undef;
}

sub unlock {
  my $self = shift;

  if(!defined($self->{shared})){
    croak "GLock::unlock():  lock $self->{lock} not locked";
  }

  $self->{shared} = undef;

  if($IMPL == $LOCKER){
    my $l = $self->{impl};
    $l->unlock();
  }elsif($IMPL == $DLM){
    my $r = Genezzo::Contrib::Clustered::GLock::GLockDLM::dlm_unlock(
        $self->{impl});
  }else{
  }

  return 1;
}

# promote shared to exclusive
# returns undef for failure
sub promote {
  my $self = shift;
  my $tmp = { @_,};

  if(!defined($self->{shared}) || ($self->{shared} != 1)){
    croak "GLock::promote():  lock $self->{lock} not locked in shared mode";
  }

  $self->{"shared"} = 0;

  if($IMPL == $LOCKER){
    my $l = $self->{impl};
    # nothing to do
    return $l;
  }elsif ($IMPL == $DLM){
    my $l = Genezzo::Contrib::Clustered::GLock::GLockDLM::dlm_promote(
        $self->{lock},$self->{impl},$self->{block});
    if($l == 0){ 
      $l = undef;
    }

    return $l;
  }else{
    return 1;
  }

  return undef;
}

sub isShared {
  my $self = shift;
  return $self->{shared};
}

1;

__DATA__

=head1 NAME

Genezzo::Contrib::Clustered::GLock::GLock - Generic locking for Genezzo

=head1 SYNOPSIS

    $curLock = new GLock(lock => $lockName, block => 1);
    $curLock->lock(shared => 0);
    $curLock->promote();
    $curLock->unlock();

=head1 DESCRIPTION

Basic locking for Genezzo.  Available implementations include NONE and OpenDLM.

=head1 FUNCTIONS

=over 4

=item new (lock => NAME, block => BLOCKING)

Creates new lock with name NAME.  Blocking if BLOCKING=1 (default).
Depending on implementation, new or following lock() may be blocking.

=item lock (shared => SHARED)

Locks lock.  Shared if SHARED=1, otherwise Exclusive (default).
Returns undef for failure.

=item promote

Promotes lock from Shared to Exclusive.  Returns undef for failure.

=item unlock

Unlocks lock. 

=back

=head1 LIMITATIONS

Edit $IMPL to choose implementation.  This will eventually be configured
from somewhere else.

=head1 AUTHOR

Eric Rollins, rollins@acm.org

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

For more information, please visit the Genezzo homepage 
at L<http://www.genezzo.com>

=cut

