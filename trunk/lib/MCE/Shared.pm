###############################################################################
## ----------------------------------------------------------------------------
## MCE::Shared - MCE extension for sharing data structures between workers.
##
###############################################################################

package MCE::Shared;

use strict;
use warnings;

## no critic (Subroutines::ProhibitSubroutinePrototypes)
## no critic (TestingAndDebugging::ProhibitNoStrict)

use Scalar::Util qw( reftype );
use MCE::Mutex;

our $VERSION = '1.600';

###############################################################################
## ----------------------------------------------------------------------------
## Import routine.
##
###############################################################################

my $_loaded;

sub import {

   my $_class = shift; return if ($_loaded++);

   unless (defined $MCE::VERSION) {
      $\ = undef; require Carp;
      Carp::croak(
         "MCE::Shared requires MCE. Please consult the MCE::Shared\n".
         "documentation for more information.\n\n"
      );
   }

   no strict 'refs'; no warnings 'redefine';

   *{ caller().'::mce_shared_handler' } = \&shared_handler;
   *{ caller().'::mce_share' } = \&share;

   return;
}

###############################################################################
## ----------------------------------------------------------------------------
## Share routines.
##
###############################################################################

our ($_HDLR, $_LOCK);

sub shared_handler (;@) {

   ## This method is not call typically. It is mainly used for passing the
   ## main MCE instance to handle shared requests in a multi-MCE sessions
   ## running simultaneously.

   if ($_[0] && reftype($_[0]) eq 'HASH') {
      $_HDLR = $_[0]; $_HDLR->{_lock_chn} = 1;
   } else {
      $_HDLR = undef;
   }

   return;
}

sub share (@) {

   shift if (defined $_[0] && $_[0] eq 'MCE::Shared');

   MCE::_croak('mce_share: method cannot be called by the worker process')
      if (MCE->wid);

   foreach my $_ref (@_) {
      my $_ref_type = reftype($_ref);

      if ($_ref_type eq 'SCALAR') {
         unless (defined $MCE::Shared::Scalar::VERSION) {
            require MCE::Shared::Scalar; MCE::Shared::Scalar->import();
         }
         MCE::Shared::Scalar::_share($_ref);
      }
      elsif ($_ref_type eq 'ARRAY') {
         unless (defined $MCE::Shared::Array::VERSION) {
            require MCE::Shared::Array; MCE::Shared::Array->import();
         }
         MCE::Shared::Array::_share($_ref);
      }
      elsif ($_ref_type eq 'HASH') {
         unless (defined $MCE::Shared::Hash::VERSION) {
            require MCE::Shared::Hash; MCE::Shared::Hash->import();
         }
         MCE::Shared::Hash::_share($_ref);
      }
      else {
         MCE::_croak(
            "Only unblessed scalar, array, and hash references\n".
            "are supported by mce_share.\n\n"
         );
      }
   }

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

MCE::Shared - MCE extension for sharing data structures between workers

=head1 VERSION

This document describes MCE::Shared version 1.600

=head1 SYNOPSIS

   use feature 'say';

   use MCE::Flow;
   use MCE::Shared;

   mce_share \ my $cnt;   ## pass references to mce_share
   mce_share \ my @a1;
   mce_share \ my %h1;

   mce_flow {
      max_workers => 4
   },
   sub {
      my ($mce) = @_;
      my ($pid, $wid) = (MCE->pid, MCE->wid);

      ## Locking is required when many workers update the same element.
      ## This requires 2 trips to the manager process (fetch and store).

      MCE->synchronize( sub {
         $cnt += 1;
      });

      ## This requires one trip, thus locking is not necessary.
      ## One trip; ->add (+=), ->concat (.=), ->substract (-=)

      tied($cnt)->add(1);

      ## Locking is also not necessary for updating uniquely.

      $a1[ $wid - 1 ] = $pid;
      $h1{ $pid }     = $wid;

      return;
   };

   say "scalar : $cnt";
   say " array : $_" foreach (@a1);
   say "  hash : $_ => $h1{$_}" foreach (sort keys %h1);

   -- Output

   scalar : 8
    array : 72127
    array : 72128
    array : 72129
    array : 72130
     hash : 72127 => 1
     hash : 72128 => 2
     hash : 72129 => 3
     hash : 72130 => 4

=head1 DESCRIPTION

This module provides data sharing for MCE supporting threads and processes.

TODO

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

