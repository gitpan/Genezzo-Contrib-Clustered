#!/usr/bin/perl
#
# copyright (c) 2005, Eric Rollins, all rights reserved, worldwide
#
#
#

use strict;
use warnings;

package Genezzo::Contrib::Clustered::GLock::GLockDLM;

# Locking for Genezzo using OpenDLM 
use Inline (C => 'DATA', LIBS => '-ldlm');
require Exporter;

Inline->init;  # help for "require GLockDLM"

our @ISA = qw(Exporter);
our @EXPORT = qw(dlm_lock dlm_unlock dlm_promote);

sub dlm_lock()
{
    my ($name, $shared, $blocking) = @_;

    return dlm_lock_impl($name, $shared, $blocking);
}

sub dlm_unlock()
{
    my ($lockid) = @_;

    return dlm_unlock_impl($lockid);
}

sub dlm_promote()
{
    my ($name, $lockid, $blocking) = @_;

    return dlm_promote_impl($name, $lockid, $blocking);
}

BEGIN
{
    print "Genezzo::Contrib::Clustered::GLock::GLockDLM installed\n";
}

1;

__DATA__

=head1 NAME

Genezzo::Contrib::Clustered::GLock::GLockDLM - OpenDLM locking implementation for Genezzo

=head1 SYNOPSIS

    my $lockid = dlm_lock($name, $shared, $blocking);
    my $success = dlm_promote($name, $lockid, $blocking);
    my $success = dlm_unlock($lockid);

=head1 DESCRIPTION

    Provides Perl wrappers to basic OpenDLM C functions.

=head1 FUNCTIONS

=over 4

=item dlm_lock NAME, SHARED, BLOCKING

Locks lock with name NAME.  Shared if SHARED=1, otherwise
exclusive.  Blocking if BLOCKING=1, otherwise returns immediately.
Returns lockid,or 0 for failure.

=item dlm_promote NAME, LOCKID, BLOCKING

Promotes lock with name NAME and lockid LOCKID to exclusive mode.
Returns 1 for success, or 0 for failure. 

=item dlm_unlock LOCKID

Releases lock with lockid LOCKID.  Returns 1 for success, 0 for failure.

=back

=head2 EXPORT

dlm_lock, dlm_promote, dlm_unlock

=head1 LIMITATIONS

 Relies on Perl Inline::C module.  Requires OpenDLM be installed.

=head1 AUTHOR

Eric Rollins, rollins@acm.org

=head1 SEE ALSO

L<perl(1)>.

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

__C__

//
#include <opendlm/dlm.h>

// returns lockid, or 0 for failure
int dlm_lock_impl(char *name, int shared, int block){
  dlm_stats_t status;
  struct lockstatus lksb;
  lksb.timeout = 0;
  int mode = LKM_EXMODE;

  if(shared) mode = LKM_PRMODE;

  int flags = 0;

  if(!block) flags = LKM_NOQUEUE;

  status = dlmlock_sync(mode,
			&lksb,
			flags,
			name,
			strlen(name),
			0,0);

  if(status != DLM_NORMAL) {
    dlm_perror("dlmlock_sync");
    return 0;
  }

  if((!block) && (lksb.status == DLM_NOTQUEUED)){
    return 0;
  } 

  if(lksb.status != DLM_NORMAL){
    dlm_perror("dlmlock_sync(2)");
    return 0;
  }

  return lksb.lockid;
}

// returns 1 for success, or 0 for failure
int dlm_unlock_impl(int lockid){
  dlm_stats_t status;
  struct lockstatus lksb;
  lksb.timeout = 0;

  status = dlmunlock_sync(lockid,
			  &(lksb.value[0]),
			  0);

  if(status != DLM_NORMAL) {
    dlm_perror("dlmunlock_sync");
    return 0;
  }

  return 1;
}

// returns 1 for success, or 0 for failure
int dlm_promote_impl(char *name, int lockid, int block){
  dlm_stats_t status;
  struct lockstatus lksb;
  lksb.timeout = 0;
  lksb.lockid = lockid;
  int mode = LKM_EXMODE;
  int flags = LKM_CONVERT;

  if(!block) flags |= LKM_NOQUEUE;

  status = dlmlock_sync(mode,
			&lksb,
			flags,
			name,
			strlen(name),
			0,0);

  if(status != DLM_NORMAL) {
    dlm_perror("dlmlock_sync(3)");
    return 0;
  }

  if((!block) && (lksb.status == DLM_NOTQUEUED)){
    return 0;
  } 

  if(lksb.status != DLM_NORMAL){
    dlm_perror("dlmlock_sync(4)");
    return 0;
  }

  return 1;
}

