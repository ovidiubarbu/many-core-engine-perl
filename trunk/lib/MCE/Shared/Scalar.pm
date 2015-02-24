###############################################################################
## ----------------------------------------------------------------------------
## MCE::Shared::Scalar - For sharing data structures between workers.
##
## There is no public API.
##
###############################################################################

package MCE::Shared::Scalar;

use strict;
use warnings;

## no critic (Subroutines::ProhibitExplicitReturnUndef)

use Scalar::Util qw( refaddr weaken );
use Socket ':crlf'; use Fcntl ':flock';
use bytes;

our $VERSION = '1.600';

use constant {
   SHR_S_STO => 'S~STO',   ## STORE
   SHR_S_FCH => 'S~FCH',   ## FETCH

   SHR_S_ADD => 'S~ADD',   ## Addition
   SHR_S_CAT => 'S~CAT',   ## Concatenation
   SHR_S_SUB => 'S~SUB',   ## Subtraction

   WA_UNDEF  => 0,         ## Wants nothing
   WA_SCALAR => 2,         ## Wants scalar
};

###############################################################################
## ----------------------------------------------------------------------------
## Import and share routines.
##
###############################################################################

my ($_all, $_loc, $_sid) = ({}, {}, 0); my $_loaded;

sub import {

   my $_class = shift; return if ($_loaded++);

   if (defined $MCE::VERSION) {
      _mce_m_init();
   }
   else {
      $\ = undef; require Carp;
      Carp::croak(
         "MCE::Shared::Scalar cannot be used directly. Please consult the\n".
         "MCE::Shared documentation for more information.\n\n"
      );
   }

   return;
}

sub _share {

   ## Simply return if already shared.
   my $_addr = refaddr($_[0]);

   unless (exists $_loc->{ $_addr }) {
      my $_id = ++$_sid;  $_all->{ $_id } = ${ $_[0] };
      tie ${ $_[0] }, 'MCE::Shared::Scalar::Tie', \$_id;
      $_loc->{ $_addr } = 1;
   }

   return;
}

###############################################################################
## ----------------------------------------------------------------------------
## Output routines for the manager process.
##
###############################################################################

{
   my ($_MCE, $_DAU_R_SOCK_REF, $_DAU_R_SOCK, $_id, $_len, $_wa);

   my $_cb_op = sub {

      $_DAU_R_SOCK = ${ $_DAU_R_SOCK_REF };

      chomp($_id  = <$_DAU_R_SOCK>);
      chomp($_wa  = <$_DAU_R_SOCK>);
      chomp($_len = <$_DAU_R_SOCK>);

      weaken $_[0]; $_[0]->();

      print {$_DAU_R_SOCK} length($_all->{ $_id }) . $LF . $_all->{ $_id }
         if $_wa;

      return;
   };

   ## -------------------------------------------------------------------------

   my %_output_function = (

      SHR_S_STO.$LF => sub {                      ## Scalar STORE
         $_DAU_R_SOCK = ${ $_DAU_R_SOCK_REF };

         chomp($_id  = <$_DAU_R_SOCK>);
         chomp($_len = <$_DAU_R_SOCK>);

         if ($_len > 0) {
            read $_DAU_R_SOCK, (my $_buf), $_len;
            $_all->{ $_id } = (chop $_buf) ? $_MCE->{thaw}($_buf) : $_buf;
         } else {
            $_all->{ $_id } = undef;
         }

         return;
      },

      SHR_S_FCH.$LF => sub {                      ## Scalar FETCH
         $_DAU_R_SOCK = ${ $_DAU_R_SOCK_REF };

         chomp($_id = <$_DAU_R_SOCK>);
         my $_buf = $_all->{ $_id };

         unless (defined $_buf) {
            print {$_DAU_R_SOCK} -1 . $LF;
         }
         else {
            if (ref $_buf) {
               $_buf  = $_MCE->{freeze}($_buf) . '1';
            } else {
               $_buf .= '0';
            }
            print {$_DAU_R_SOCK} length($_buf) . $LF . $_buf;
         }

         return;
      },

      ## ----------------------------------------------------------------------

      SHR_S_ADD.$LF => sub {                      ## Addition
         $_cb_op->( sub {
            read $_DAU_R_SOCK, (my $_buf), $_len;
            $_all->{ $_id } += $_buf;
         });
      },

      SHR_S_CAT.$LF => sub {                      ## Concatenation
         $_cb_op->( sub {
            read $_DAU_R_SOCK, (my $_buf), $_len;
            $_all->{ $_id } .= $_buf;
         });
      },

      SHR_S_SUB.$LF => sub {                      ## Subtraction
         $_cb_op->( sub {
            read $_DAU_R_SOCK, (my $_buf), $_len;
            $_all->{ $_id } -= $_buf;
         });
      },

   );

   ## -------------------------------------------------------------------------

   sub _mce_m_loop_begin {

      ($_MCE, $_DAU_R_SOCK_REF) = @_;

      if (defined $MCE::Shared::_HDLR &&
            refaddr($_MCE) != refaddr($MCE::Shared::_HDLR)) {

         $_MCE = $MCE::Shared::_HDLR;
         $_DAU_R_SOCK_REF = \ $_MCE->{_dat_r_sock}->[1];
      }

      return;
   }

   sub _mce_m_loop_end {

      $_MCE = $_DAU_R_SOCK_REF = $_DAU_R_SOCK = undef;
      $_id = $_len = $_wa = undef;

      return;
   }

   sub _mce_m_init {

      MCE::_attach_plugin(
         \%_output_function, \&_mce_m_loop_begin, \&_mce_m_loop_end,
         \&_mce_w_init
      );

      return;
   }
}

###############################################################################
## ----------------------------------------------------------------------------
## Worker routines.
##
###############################################################################

my ($_MCE, $_DAT_LOCK, $_DAT_W_SOCK, $_DAU_W_SOCK, $_chn, $_lock_chn);
my ($_id, $_len, $_wa);

sub _mce_w_init {

   ($_MCE) = @_;

   if (defined $MCE::Shared::_HDLR &&
         refaddr($_MCE) != refaddr($MCE::Shared::_HDLR)) {

      ($_MCE, $_chn, $_lock_chn) = ($MCE::Shared::_HDLR, 1, 1);

      unless (defined $MCE::Shared::_LOCK) {
         my $_sess_dir = $_MCE->{_sess_dir};
         open $MCE::Shared::_LOCK, '+>>:raw:stdio', "$_sess_dir/_dat.lock.1"
            or die "(W) open error $_sess_dir/_dat.lock.1: $!\n";
      }

      $_DAT_LOCK = $MCE::Shared::_LOCK;
   }
   else {
      ($_chn, $_lock_chn) = ($_MCE->{_chn}, $_MCE->{_lock_chn});
      $_DAT_LOCK = $_MCE->{_dat_lock};
   }

   $_DAT_W_SOCK = $_MCE->{_dat_w_sock}->[0];
   $_DAU_W_SOCK = $_MCE->{_dat_w_sock}->[$_chn];

   return;
}

my $_do_op = sub {

   $_wa = (!defined wantarray) ? WA_UNDEF : WA_SCALAR;
   local $\ = undef if (defined $\);

   flock $_DAT_LOCK, LOCK_EX if ($_lock_chn);
   print {$_DAT_W_SOCK} $_[0] . $LF . $_chn . $LF;
   print {$_DAU_W_SOCK} $_[1] . $LF . $_wa . $LF . length($_[2]) . $LF . $_[2];

   my $_buf; if ($_wa) {
      local $/ = $LF if (!$/ || $/ ne $LF);
      chomp($_len = <$_DAU_W_SOCK>);
      read  $_DAU_W_SOCK, $_buf, $_len;
   }

   flock $_DAT_LOCK, LOCK_UN if ($_lock_chn);

   return $_buf;
};

###############################################################################
## ----------------------------------------------------------------------------
## Scalar tie package.
##
###############################################################################

package MCE::Shared::Scalar::Tie;

use Scalar::Util qw( looks_like_number reftype );
use Socket ':crlf'; use Fcntl ':flock';
use bytes;

use constant {
   SHR_S_STO => 'S~STO',   ## STORE
   SHR_S_FCH => 'S~FCH',   ## FETCH

   SHR_S_ADD => 'S~ADD',   ## Addition
   SHR_S_CAT => 'S~CAT',   ## Concatenation
   SHR_S_SUB => 'S~SUB',   ## Subtraction
};

sub TIESCALAR { bless $_[1] => $_[0]; }

## ----------------------------------------------------------------------------

sub STORE {                                       ## Scalar STORE
   $_id = ${ $_[0] };

   unless ($MCE::MCE->{_wid}) {
      $_all->{ $_id } = $_[1];
   }
   else {
      my ($_buf, $_tmp);

      unless (defined $_[1]) {
         $_buf = $_id . $LF . -1 . $LF;
      }
      elsif (reftype $_[1]) {
         $_tmp = $_MCE->{freeze}($_[1]);
         $_buf = $_id . $LF . (length($_tmp) + 1) . $LF . $_tmp . '1';
      }
      else {
         $_buf = $_id . $LF . (length($_[1]) + 1) . $LF . $_[1] . '0';
      }

      local $\ = undef if (defined $\);

      flock $_DAT_LOCK, LOCK_EX if ($_lock_chn);
      print {$_DAT_W_SOCK} SHR_S_STO . $LF . $_chn . $LF;
      print {$_DAU_W_SOCK} $_buf;
      flock $_DAT_LOCK, LOCK_UN if ($_lock_chn);

      return;
   }
}

sub FETCH {                                       ## Scalar FETCH
   $_id = ${ $_[0] };

   unless ($MCE::MCE->{_wid}) {
      $_all->{ $_id };
   }
   else {
      local $\ = undef if (defined $\);
      local $/ = $LF if (!$/ || $/ ne $LF);

      flock $_DAT_LOCK, LOCK_EX if ($_lock_chn);
      print {$_DAT_W_SOCK} SHR_S_FCH . $LF . $_chn . $LF;
      print {$_DAU_W_SOCK} $_id . $LF;

      chomp($_len = <$_DAU_W_SOCK>);

      if ($_len < 0) {
         flock $_DAT_LOCK, LOCK_UN if ($_lock_chn);
         return undef;
      }

      read  $_DAU_W_SOCK, (my $_buf), $_len;
      flock $_DAT_LOCK, LOCK_UN if ($_lock_chn);

      return (chop $_buf) ? $_MCE->{thaw}($_buf) : $_buf;
   }
}

## ----------------------------------------------------------------------------

sub add {                                         ## Addition
   $_id = ${ $_[0] };

   MCE::_croak('Use of uninitialized value in addition (+)')
      unless (defined $_[1]);
   MCE::_croak("Argument \"$_[1]\" isn't numeric in addition (+)")
      unless (looks_like_number($_[1]));

   unless ($MCE::MCE->{_wid}) {
      $_all->{ $_id } += $_[1];
   } else {
      $_do_op->(SHR_S_ADD, $_id, $_[1]);
   }
}

sub concat {                                      ## Concatenation
   $_id = ${ $_[0] };

   MCE::_croak('Use of uninitialized value in concatenation (.)')
      unless (defined $_[1]);

   unless ($MCE::MCE->{_wid}) {
      $_all->{ $_id } .= $_[1];
   } else {
      $_do_op->(SHR_S_CAT, $_id, $_[1]);
   }
}

sub subtract {                                    ## Substraction
   $_id = ${ $_[0] };

   MCE::_croak('Use of uninitialized value in substraction (-)')
      unless (defined $_[1]);
   MCE::_croak("Argument \"$_[1]\" isn't numeric in substraction (-)")
      unless (looks_like_number($_[1]));

   unless ($MCE::MCE->{_wid}) {
      $_all->{ $_id } -= $_[1];
   } else {
      $_do_op->(SHR_S_SUB, $_id, $_[1]);
   }
}

1;

