###############################################################################
## ----------------------------------------------------------------------------
## MCE::Mutex - Simple semaphore for Many-Core Engine.
##
###############################################################################

package MCE::Mutex;

use strict;
use warnings;

no warnings 'threads';
no warnings 'recursion';
no warnings 'uninitialized';

use MCE::Util qw( $LF );
use bytes;

our $VERSION = '1.601';

sub DESTROY {

   my ($_mutex, $_arg) = @_;

   if (!defined $_arg || $_arg ne 'shutdown') {
      return if (defined $MCE::VERSION && !defined $MCE::MCE->{_wid});
      return if (defined $MCE::MCE && $MCE::MCE->{_wid});
   }

   MCE::Util::_destroy_sockets($_mutex, qw(_w_sock _r_sock));

   return;
}

###############################################################################
## ----------------------------------------------------------------------------
## Public methods.
##
###############################################################################

sub new {

   my ($_class, %_argv) = @_;

   @_ = ();

   my $_mutex = {}; bless($_mutex, ref($_class) || $_class);

   MCE::Util::_make_socket_pair($_mutex, qw(_w_sock _r_sock));

   syswrite $_mutex->{_w_sock}, '0';

   return $_mutex;
}

sub lock {

 # my ($_mutex) = @_;

   sysread $_[0]->{_r_sock}, my $_b, 1;

   return;
}

sub synchronize {

   my ($_mutex, $_code) = (shift, shift);

   if (ref $_code eq 'CODE') {
      if (defined wantarray) {
         sysread  $_mutex->{_r_sock}, my $_b, 1;
         my @_a = $_code->(@_);
         syswrite $_mutex->{_w_sock}, '0';

         return wantarray ? @_a : $_a[0];
      }
      else {
         sysread  $_mutex->{_r_sock}, my $_b, 1;
         $_code->(@_);
         syswrite $_mutex->{_w_sock}, '0';
      }
   }

   return;
}

sub unlock {

 # my ($_mutex) = @_;

   syswrite $_[0]->{_w_sock}, '0';

   return;
}

1;

__END__

###############################################################################
## ----------------------------------------------------------------------------
## Module usage.
##
###############################################################################

=head1 NAME

MCE::Mutex - Simple semaphore for Many-Core Engine

=head1 VERSION

This document describes MCE::Mutex version 1.601

=head1 SYNOPSIS

   use MCE::Flow max_workers => 4;
   use MCE::Mutex;

   print "## running a\n";
   my $a = MCE::Mutex->new;

   mce_flow sub {
      $a->lock;

      ## access shared resource
      my $wid = MCE->wid; MCE->say($wid); sleep 1;

      $a->unlock;
   };

   print "## running b\n";
   my $b = MCE::Mutex->new;

   mce_flow sub {
      $b->synchronize( sub {

         ## access shared resource
         my ($wid) = @_; MCE->say($wid); sleep 1;

      }, MCE->wid );
   };

=head1 DESCRIPTION

This module implements locking methods that can be used to coordinate access
to shared data from multiple workers spawned as threads or processes.

The inspiration for this module came from reading Mutex for Ruby.

=head1 API DOCUMENTATION

=head2 MCE::Mutex->new ( void )

Creates a new mutex.

=head2 $mutex->lock ( void )

Attempts to grab the lock and waits if not available.

=head2 $mutex->unlock ( void )

Releases the lock.

=head2 $mutex->synchronize ( sub { ... }, @_ )

Obtains a lock, runs the code block, and releases the lock after the block
completes.

=head1 INDEX

L<MCE|MCE>

=head1 AUTHOR

Mario E. Roy, S<E<lt>marioeroy AT gmail DOT comE<gt>>

=head1 LICENSE

This program is free software; you can redistribute it and/or modify it
under the terms of either: the GNU General Public License as published
by the Free Software Foundation; or the Artistic License.

See L<http://dev.perl.org/licenses/> for more information.

=cut

