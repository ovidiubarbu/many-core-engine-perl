
package MCE::Core::Input::Generator;

our $VERSION = '1.499_001'; $VERSION = eval $VERSION;

## Items below are folded into MCE.

package MCE;

use strict;
use warnings;

## Warnings are disabled to minimize bits of noise when user or OS signals
## the script to exit. e.g. MCE_script.pl < infile | head

no warnings 'threads'; no warnings 'uninitialized';

###############################################################################
## ----------------------------------------------------------------------------
## Worker process -- Sequence Generator (equal distribution among workers).
##
###############################################################################

sub _worker_sequence_generator {

   my MCE $self = $_[0];

   @_ = ();

   die "Private method called" unless (caller)[0]->isa( ref($self) );

   _croak("MCE::_worker_sequence_generator: 'user_func' is not specified")
      unless (defined $self->{user_func});

   my $_max_workers = $self->{max_workers};
   my $_chunk_size  = $self->{chunk_size};
   my $_wuf         = $self->{_wuf};

   my ($_begin, $_end, $_step, $_fmt);

   if (ref $self->{sequence} eq 'ARRAY') {
      ($_begin, $_end, $_step, $_fmt) = @{ $self->{sequence} };
   }
   else {
      $_begin = $self->{sequence}->{begin};
      $_end   = $self->{sequence}->{end};
      $_step  = $self->{sequence}->{step};
      $_fmt   = $self->{sequence}->{format};
   }

   my $_wid      = $self->{_task_wid} || $self->{_wid};
   my $_next     = ($_wid - 1) * $_chunk_size * $_step + $_begin;
   my $_chunk_id = $_wid;

   ## -------------------------------------------------------------------------

   $self->{_last_jmp} = sub { goto _WORKER_SEQ_GEN__LAST; };

   if ($_begin == $_end) {                        ## Both are identical.

      if ($_wid == 1) {
         $self->{_next_jmp} = sub { goto _WORKER_SEQ_GEN__LAST; };

         local $_ = (defined $_fmt) ? sprintf("%$_fmt", $_next) : $_next;

         if ($_chunk_size > 1) {
            $_wuf->($self, [ $_ ], $_chunk_id);
         } else {
            $_wuf->($self, $_, $_chunk_id);
         }
      }
   }
   elsif ($_chunk_size == 1) {                    ## Does no chunking.

      $self->{_next_jmp} = sub { goto _WORKER_SEQ_GEN__NEXT_A; };

      my $_flag = ($_begin < $_end);

      while (1) {
         return if ( $_flag && $_next > $_end);
         return if (!$_flag && $_next < $_end);

         local $_ = (defined $_fmt) ? sprintf("%$_fmt", $_next) : $_next;
         $_wuf->($self, $_, $_chunk_id);

         _WORKER_SEQ_GEN__NEXT_A:

         $_chunk_id += $_max_workers;
         $_next      = ($_chunk_id - 1) * $_step + $_begin;
      }
   }
   else {                                         ## Yes, does chunking.

      $self->{_next_jmp} = sub { goto _WORKER_SEQ_GEN__NEXT_B; };

      while (1) {
         my $_n_begin = $_next;
         my @_n = ();

         if ($_begin < $_end) {
            for (1 .. $_chunk_size) {
               last if ($_next > $_end);
               push @_n, (defined $_fmt) ? sprintf("%$_fmt", $_next) : $_next;
               $_next = $_step * $_ + $_n_begin;
            }
         }
         else {
            for (1 .. $_chunk_size) {
               last if ($_next < $_end);
               push @_n, (defined $_fmt) ? sprintf("%$_fmt", $_next) : $_next;
               $_next = $_step * $_ + $_n_begin;
            }
         }

         return unless (@_n > 0);

         local $_ = \@_n;
         $_wuf->($self, \@_n, $_chunk_id);

         _WORKER_SEQ_GEN__NEXT_B:

         $_chunk_id += $_max_workers;
         $_next      = ($_chunk_id - 1) * $_chunk_size * $_step + $_begin;
      }
   }

   _WORKER_SEQ_GEN__LAST:

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

MCE::Core::Input::Generator - Sequence of numbers (used by task > 0 workers).

=head1 SYNOPSIS

There is no public API.

=head1 DESCRIPTION

This package provides a sequence of numbers used internally by the worker
process. Distribution is divided equally among workers. This allows sequence
to be configured independently among multiple user tasks.

=head1 AUTHOR

Mario E. Roy, S<E<lt>marioeroy AT gmail DOT comE<gt>>

=head1 LICENSE

This program is free software; you can redistribute it and/or modify it
under the terms of either: the GNU General Public License as published
by the Free Software Foundation; or the Artistic License.

See L<http://dev.perl.org/licenses/> for more information.

=cut

