###############################################################################
## ----------------------------------------------------------------------------
## MCE::Core::Input::Sequence - Sequence of numbers (for task_id == 0).
##
## This package provides a sequence of numbers used internally by the worker
## process. Distribution follows a bank-queuing model.
##
## There is no public API.
##
###############################################################################

package MCE::Core::Input::Sequence;

our $VERSION = '1.520'; $VERSION = eval $VERSION;

## Items below are folded into MCE.

package MCE;

use strict;
use warnings;

## Warnings are disabled to minimize bits of noise when user or OS signals
## the script to exit. e.g. MCE_script.pl < infile | head

no warnings 'threads'; no warnings 'uninitialized';

my $_que_read_size = $MCE::_que_read_size;
my $_que_template  = $MCE::_que_template;

###############################################################################
## ----------------------------------------------------------------------------
## Worker process -- Sequence Queue (distribution via bank queuing model).
##
###############################################################################

sub _worker_sequence_queue {

   my $self = $_[0];

   @_ = ();

   die "Private method called" unless (caller)[0]->isa( ref($self) );

   _croak("MCE::_worker_sequence_queue: 'user_func' is not specified")
      unless (defined $self->{user_func});

   my $_QUE_R_SOCK  = $self->{_que_r_sock};
   my $_QUE_W_SOCK  = $self->{_que_w_sock};
   my $_bounds_only = $self->{bounds_only} || 0;
   my $_chunk_size  = $self->{chunk_size};
   my $_wuf         = $self->{_wuf};

   my ($_next, $_chunk_id, $_seq_n, $_begin, $_end, $_step, $_fmt);
   my ($_abort, $_offset);

   if (ref $self->{sequence} eq 'ARRAY') {
      ($_begin, $_end, $_step, $_fmt) = @{ $self->{sequence} };
   }
   else {
      $_begin = $self->{sequence}->{begin};
      $_end   = $self->{sequence}->{end};
      $_step  = $self->{sequence}->{step};
      $_fmt   = $self->{sequence}->{format};
   }

   $_abort    = $self->{_abort_msg};
   $_chunk_id = $_offset = 0;

   $_fmt =~ s/%// if (defined $_fmt);

   ## -------------------------------------------------------------------------

   $self->{_next_jmp} = sub { goto _WORKER_SEQUENCE__NEXT; };
   $self->{_last_jmp} = sub { goto _WORKER_SEQUENCE__LAST; };

   while (1) {

      ## Obtain the next chunk_id and sequence number.
      sysread $_QUE_R_SOCK, $_next, $_que_read_size;
      ($_chunk_id, $_offset) = unpack($_que_template, $_next);

      if ($_offset >= $_abort) {
         syswrite $_QUE_W_SOCK, pack($_que_template, 0, $_offset);
         return;
      }

      syswrite $_QUE_W_SOCK,
         pack($_que_template, $_chunk_id + 1, $_offset + 1);

      $_chunk_id++;

      ## Call user function.
      if ($_chunk_size == 1) {
         $_seq_n = $_offset * $_step + $_begin;
         $_seq_n = sprintf("%$_fmt", $_seq_n) if (defined $_fmt);
         local $_ = $_seq_n;
         $_wuf->($self, $_seq_n, $_chunk_id);
      }
      else {
         my $_n_begin = ($_offset * $_chunk_size) * $_step + $_begin;
         my @_n = ();    $_seq_n = $_n_begin;

         ## -------------------------------------------------------------------

         if ($_bounds_only) {
            my $_tmp_b = $_seq_n; my $_tmp_e;

            if ($_begin < $_end) {
               if ($_step * $_chunk_size + $_n_begin <= $_end) {
                  $_tmp_e = $_step * ($_chunk_size - 1) + $_n_begin;
               }
               else {
                  my $_start = int(
                     ($_end - $_seq_n) / $_chunk_size / $_step * $_chunk_size
                  );
                  $_start = 1 if ($_start < 1);

                  for ($_start .. $_chunk_size) {
                     last if ($_seq_n > $_end);
                     $_tmp_e = $_seq_n;
                     $_seq_n = $_step * $_ + $_n_begin;
                  }
               }
            }
            else {
               if ($_step * $_chunk_size + $_n_begin >= $_end) {
                  $_tmp_e = $_step * ($_chunk_size - 1) + $_n_begin;
               }
               else {
                  my $_start = int(
                     ($_seq_n - $_end) / $_chunk_size / $_step * $_chunk_size
                  );
                  $_start = 1 if ($_start < 1);

                  for ($_start .. $_chunk_size) {
                     last if ($_seq_n < $_end);
                     $_tmp_e = $_seq_n;
                     $_seq_n = $_step * $_ + $_n_begin;
                  }
               }
            }

            if (defined $_fmt) {
               @_n = (sprintf("%$_fmt", $_tmp_b), sprintf("%$_fmt", $_tmp_e));
            } else {
               @_n = ($_tmp_b, $_tmp_e);
            }
         }

         ## -------------------------------------------------------------------

         else {
            if ($_begin < $_end) {
               if (!defined $_fmt && $_step == 1) {
                  local $_ = ($_seq_n + $_chunk_size <= $_end)
                     ? [ $_seq_n .. $_seq_n + $_chunk_size - 1 ]
                     : [ $_seq_n .. $_end ];

                  $_wuf->($self, $_, $_chunk_id);
                  next;
               }
               else {
                  for (1 .. $_chunk_size) {
                     last if ($_seq_n > $_end);

                     push @_n, (defined $_fmt)
                        ? sprintf("%$_fmt", $_seq_n) : $_seq_n;

                     $_seq_n = $_step * $_ + $_n_begin;
                  }
               }
            }
            else {
               for (1 .. $_chunk_size) {
                  last if ($_seq_n < $_end);

                  push @_n, (defined $_fmt)
                     ? sprintf("%$_fmt", $_seq_n) : $_seq_n;

                  $_seq_n = $_step * $_ + $_n_begin;
               }
            }
         }

         ## -------------------------------------------------------------------

         local $_ = \@_n;
         $_wuf->($self, \@_n, $_chunk_id);
      }

      _WORKER_SEQUENCE__NEXT:
   }

   _WORKER_SEQUENCE__LAST:

   return;
}

1;

