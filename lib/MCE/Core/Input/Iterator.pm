###############################################################################
## ----------------------------------------------------------------------------
## MCE::Core::Input::Iterator - Iterator reader.
##
## This package, used interally by the worker process, provides support for
## user specified iterators assigned to input_data.
##
## There is no public API.
##
###############################################################################

package MCE::Core::Input::Iterator;

use strict;
use warnings;

our $VERSION = '1.600';

## Items below are folded into MCE.

package MCE;

## Warnings are disabled to minimize bits of noise when user or OS signals
## the script to exit. e.g. MCE_script.pl < infile | head

no warnings 'threads';
no warnings 'uninitialized';

###############################################################################
## ----------------------------------------------------------------------------
## Worker process -- User Iterator.
##
###############################################################################

sub _worker_user_iterator {

   my ($self) = @_;

   @_ = ();

   die 'Private method called' unless (caller)[0]->isa( ref $self );

   _croak('MCE::_worker_user_iterator: (user_func) is not specified')
      unless (defined $self->{user_func});

   my $_chn         = $self->{_chn};
   my $_DAT_LOCK    = $self->{_dat_lock};
   my $_DAT_W_SOCK  = $self->{_dat_w_sock}->[0];
   my $_DAU_W_SOCK  = $self->{_dat_w_sock}->[$_chn];
   my $_lock_chn    = $self->{_lock_chn};
   my $_chunk_size  = $self->{chunk_size};
   my $_I_FLG       = (!$/ || $/ ne $LF);
   my $_wuf         = $self->{_wuf};

   my ($_chunk_id, $_len, $_is_ref);

   ## -------------------------------------------------------------------------

   $self->{_next_jmp} = sub { goto _WORKER_USER_ITERATOR__NEXT; };
   $self->{_last_jmp} = sub { goto _WORKER_USER_ITERATOR__LAST; };

   local $_;

   _WORKER_USER_ITERATOR__NEXT:

   while (1) {
      undef $_ if (length > MAX_CHUNK_SIZE);

      $_ = '';

      ## Obtain the next chunk of data.
      {
         local $\ = undef if (defined $\); local $/ = $LF if ($_I_FLG);

         flock $_DAT_LOCK, LOCK_EX if ($_lock_chn);
         print {$_DAT_W_SOCK} OUTPUT_U_ITR . $LF . $_chn . $LF;
         chomp($_len = <$_DAU_W_SOCK>);

         if ($_len < 0) {
            flock $_DAT_LOCK, LOCK_UN if ($_lock_chn);
            return;
         }

         $_is_ref = chop $_len;

         chomp($_chunk_id = <$_DAU_W_SOCK>);
         read $_DAU_W_SOCK, $_, $_len;

         flock $_DAT_LOCK, LOCK_UN if ($_lock_chn);
      }

      ## Call user function.
      if ($_is_ref) {
         my $_chunk_ref = $self->{thaw}($_); undef $_;
         $_ = ($_chunk_size == 1) ? $_chunk_ref->[0] : $_chunk_ref;
         $_wuf->($self, $_chunk_ref, $_chunk_id);
      }
      else {
         $_wuf->($self, [ $_ ], $_chunk_id);
      }
   }

   _WORKER_USER_ITERATOR__LAST:

   return;
}

1;

