###############################################################################
## ----------------------------------------------------------------------------
## MCE::Shared::Hash - For sharing data structures between workers.
##
## There is no public API.
##
###############################################################################

package MCE::Shared::Hash;

use strict;
use warnings;

## no critic (Subroutines::ProhibitExplicitReturnUndef)

use Scalar::Util qw( refaddr weaken );
use Socket ':crlf'; use Fcntl ':flock';
use bytes;

our $VERSION = '1.600';

use constant {
   SHR_H_STO => 'H~STO',   ## STORE
   SHR_H_FCH => 'H~FCH',   ## FETCH
   SHR_H_FST => 'H~FST',   ## FIRSTKEY / NEXTKEY
   SHR_H_EXI => 'H~EXI',   ## EXISTS
   SHR_H_DEL => 'H~DEL',   ## DELETE
   SHR_H_CLR => 'H~CLR',   ## CLEAR
   SHR_H_SCA => 'H~SCA',   ## SCALAR

   SHR_H_ADD => 'H~ADD',   ## Addition
   SHR_H_CAT => 'H~CAT',   ## Concatenation
   SHR_H_SUB => 'H~SUB',   ## Subtraction

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
         "MCE::Shared::Hash cannot be used directly. Please consult the\n".
         "MCE::Shared documentation for more information.\n\n"
      );
   }

   return;
}

sub _share {

   ## Simply return if already shared.
   my $_addr = refaddr($_[0]);

   unless (exists $_loc->{ $_addr }) {
      my $_id = ++$_sid;  %{ $_all->{ $_id } } = %{ $_[0] };
      tie %{ $_[0] }, 'MCE::Shared::Hash::Tie', \$_id;
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
   my ($_MCE, $_DAU_R_SOCK_REF, $_DAU_R_SOCK, $_id, $_key, $_len, $_ret, $_wa);

   my $_cb_op = sub {

      $_DAU_R_SOCK = ${ $_DAU_R_SOCK_REF };

      chomp($_id  = <$_DAU_R_SOCK>);
      chomp($_wa  = <$_DAU_R_SOCK>);
      chomp($_len = <$_DAU_R_SOCK>);

      read $_DAU_R_SOCK, $_key, $_len;
      chomp($_len = <$_DAU_R_SOCK>);

      weaken $_[0]; $_[0]->();

      if ($_wa) {
         print {$_DAU_R_SOCK} length($_all->{ $_id }->{ $_key }) . $LF .
            $_all->{ $_id }->{ $_key };
      }

      return;
   };

   my $_cb_ret = sub {

      unless (defined $_[0]) {
         print {$_DAU_R_SOCK} -1 . $LF;
      }
      else {
         if (ref $_[0]) {
            $_[0]  = $_MCE->{freeze}($_[0]) . '1';
         } else {
            $_[0] .= '0';
         }
         print {$_DAU_R_SOCK} length($_[0]) . $LF . $_[0];
      }

      return;
   };

   ## -------------------------------------------------------------------------

   my %_output_function = (

      SHR_H_STO.$LF => sub {                      ## Hash STORE
         $_DAU_R_SOCK = ${ $_DAU_R_SOCK_REF };

         chomp($_id  = <$_DAU_R_SOCK>);
         chomp($_len = <$_DAU_R_SOCK>);

         read $_DAU_R_SOCK, $_key, $_len;
         chomp($_len = <$_DAU_R_SOCK>);
 
         if ($_len > 0) {
            read $_DAU_R_SOCK, (my $_buf), $_len;
            $_all->{ $_id }->{ $_key } =
               (chop $_buf) ? $_MCE->{thaw}($_buf) : $_buf;
         } else {
            $_all->{ $_id }->{ $_key } = undef;
         }

         return;
      },

      SHR_H_FCH.$LF => sub {                      ## Hash FETCH
         $_DAU_R_SOCK = ${ $_DAU_R_SOCK_REF };

         chomp($_id  = <$_DAU_R_SOCK>);
         chomp($_len = <$_DAU_R_SOCK>);

         read $_DAU_R_SOCK, $_key, $_len;

         my $_buf = $_all->{ $_id }->{ $_key };
         $_cb_ret->($_buf);

         return;
      },

      SHR_H_FST.$LF => sub {                      ## Hash FIRSTKEY / NEXTKEY
         $_DAU_R_SOCK = ${ $_DAU_R_SOCK_REF };

         chomp($_id = <$_DAU_R_SOCK>);
         my @_a = keys %{ $_all->{ $_id } };

         if (scalar @_a) {
            my $_buf = $_MCE->{freeze}(\@_a);
            print {$_DAU_R_SOCK} length($_buf) . $LF . $_buf;
         } else {
            print {$_DAU_R_SOCK} -1 . $LF;
         }

         return;
      },

      SHR_H_EXI.$LF => sub {                      ## Hash EXISTS
         $_DAU_R_SOCK = ${ $_DAU_R_SOCK_REF };

         chomp($_id  = <$_DAU_R_SOCK>);
         chomp($_len = <$_DAU_R_SOCK>);

         read $_DAU_R_SOCK, $_key, $_len;
         $_ret = (exists $_all->{ $_id }->{ $_key }) ? 1 : 0;

         print {$_DAU_R_SOCK} $_ret . $LF;

         return;
      },

      SHR_H_DEL.$LF => sub {                      ## Hash DELETE
         $_DAU_R_SOCK = ${ $_DAU_R_SOCK_REF };

         chomp($_id  = <$_DAU_R_SOCK>);
         chomp($_wa  = <$_DAU_R_SOCK>);
         chomp($_len = <$_DAU_R_SOCK>);

         read $_DAU_R_SOCK, $_key, $_len;

         unless ($_wa) {
            delete $_all->{ $_id }->{ $_key };
         } else {
            my $_buf = delete $_all->{ $_id }->{ $_key };
            $_cb_ret->($_buf);
         }

         return;
      },

      SHR_H_CLR.$LF => sub {                      ## Hash CLEAR
         $_DAU_R_SOCK = ${ $_DAU_R_SOCK_REF };

         chomp($_id = <$_DAU_R_SOCK>);
         %{ $_all->{ $_id } } = ();

         return;
      },

      SHR_H_SCA.$LF => sub {                      ## Hash SCALAR
         $_DAU_R_SOCK = ${ $_DAU_R_SOCK_REF };

         chomp($_id = <$_DAU_R_SOCK>);
         $_ret = scalar %{ $_all->{ $_id } };

         print {$_DAU_R_SOCK} $_ret . $LF;

         return;
      },

      ## ----------------------------------------------------------------------

      SHR_H_ADD.$LF => sub {                      ## Addition
         $_cb_op->( sub {
            read $_DAU_R_SOCK, (my $_buf), $_len;
            $_all->{ $_id }->{ $_key } += $_buf;
         });
      },

      SHR_H_CAT.$LF => sub {                      ## Concatenation
         $_cb_op->( sub {
            read $_DAU_R_SOCK, (my $_buf), $_len;
            $_all->{ $_id }->{ $_key } .= $_buf;
         });
      },

      SHR_H_SUB.$LF => sub {                      ## Subtraction
         $_cb_op->( sub {
            read $_DAU_R_SOCK, (my $_buf), $_len;
            $_all->{ $_id }->{ $_key } -= $_buf;
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
      $_id = $_key = $_len = $_ret = $_wa = undef;

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
my ($_id, $_len, $_ret, $_wa);

sub _mce_w_init {

   ($_MCE) = @_;

   unless (defined $MCE::Shared::_HDLR &&
         refaddr($_MCE) != refaddr($MCE::Shared::_HDLR)) {

      ($_chn, $_lock_chn) = ($_MCE->{_chn}, $_MCE->{_lock_chn});

      $_DAT_LOCK = $_MCE->{_dat_lock};
   }
   else {
      ## running separate MCE instances simultaneously
      ($_MCE, $_chn, $_lock_chn) = ($MCE::Shared::_HDLR, 1, 1);

      unless (defined $MCE::Shared::_LOCK) {
         my $_sess_dir = $_MCE->{_sess_dir};
         open $MCE::Shared::_LOCK, '+>>:raw:stdio', "$_sess_dir/_dat.lock.1"
            or die "(W) open error $_sess_dir/_dat.lock.1: $!\n";
      }

      $_DAT_LOCK = $MCE::Shared::_LOCK;
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
   print {$_DAU_W_SOCK} $_[1] . $LF . $_wa . $LF .
      length($_[2]) . $LF . $_[2] . length($_[3]) . $LF . $_[3];

   my $_buf; if ($_wa) {
      local $/ = $LF if (!$/ || $/ ne $LF);
      chomp($_len = <$_DAU_W_SOCK>);
      read  $_DAU_W_SOCK, $_buf, $_len;
   }

   flock $_DAT_LOCK, LOCK_UN if ($_lock_chn);

   return $_buf;
};

my $_do_ret = sub {

   local $/ = $LF if (!$/ || $/ ne $LF);
   chomp($_len = <$_DAU_W_SOCK>);

   if ($_len < 0) {
      flock $_DAT_LOCK, LOCK_UN if ($_lock_chn);
      return undef;
   }

   read  $_DAU_W_SOCK, (my $_buf), $_len;
   flock $_DAT_LOCK, LOCK_UN if ($_lock_chn);

   return (chop $_buf) ? $_MCE->{thaw}($_buf) : $_buf;
};

###############################################################################
## ----------------------------------------------------------------------------
## Hash tie package.
##
###############################################################################

package MCE::Shared::Hash::Tie;

use Scalar::Util qw( looks_like_number reftype );
use Socket ':crlf'; use Fcntl ':flock';
use bytes;

use constant {
   SHR_H_STO => 'H~STO',   ## STORE
   SHR_H_FCH => 'H~FCH',   ## FETCH
   SHR_H_FST => 'H~FST',   ## FIRSTKEY / NEXTKEY
   SHR_H_EXI => 'H~EXI',   ## EXISTS
   SHR_H_DEL => 'H~DEL',   ## DELETE
   SHR_H_CLR => 'H~CLR',   ## CLEAR
   SHR_H_SCA => 'H~SCA',   ## SCALAR

   SHR_H_ADD => 'H~ADD',   ## Addition
   SHR_H_CAT => 'H~CAT',   ## Concatenation
   SHR_H_SUB => 'H~SUB',   ## Subtraction

   WA_UNDEF  => 0,         ## Wants nothing
   WA_SCALAR => 2,         ## Wants scalar
};

sub TIEHASH { bless $_[1] => $_[0]; }

## ----------------------------------------------------------------------------

sub STORE {                                       ## Hash STORE
   $_id = ${ $_[0] };

   unless ($MCE::MCE->{_wid}) {
      $_all->{ $_id }->{ $_[1] } = $_[2];
   }
   else {
      my ($_buf, $_tmp);

      unless (defined $_[2]) {
         $_buf = $_id . $LF . length($_[1]) . $LF . $_[1] . -1 . $LF;
      }
      elsif (reftype $_[2]) {
         $_tmp = $_MCE->{freeze}($_[2]);
         $_buf = $_id . $LF . length($_[1]) . $LF . $_[1] .
            (length($_tmp) + 1) . $LF . $_tmp . '1';
      }
      else {
         $_buf = $_id . $LF . length($_[1]) . $LF . $_[1] .
            (length($_[2]) + 1) . $LF . $_[2] . '0';
      }

      local $\ = undef if (defined $\);

      flock $_DAT_LOCK, LOCK_EX if ($_lock_chn);
      print {$_DAT_W_SOCK} SHR_H_STO . $LF . $_chn . $LF;
      print {$_DAU_W_SOCK} $_buf;
      flock $_DAT_LOCK, LOCK_UN if ($_lock_chn);

      return;
   }
}

sub FETCH {                                       ## Hash FETCH
   $_id = ${ $_[0] };

   unless ($MCE::MCE->{_wid}) {
      $_all->{ $_id }->{ $_[1] };
   }
   else {
      local $\ = undef if (defined $\);

      flock $_DAT_LOCK, LOCK_EX if ($_lock_chn);
      print {$_DAT_W_SOCK} SHR_H_FCH . $LF . $_chn . $LF;
      print {$_DAU_W_SOCK} $_id . $LF . length($_[1]) . $LF . $_[1];

      $_do_ret->();
   }
}

sub FIRSTKEY {                                    ## Hash FIRSTKEY
   $_id = ${ $_[0] };

   if ($MCE::MCE->{_wid}) {
      local $\ = undef if (defined $\);
      local $/ = $LF if (!$/ || $/ ne $LF);

      %{ $_all->{ $_id } } = ();   ## cache keys locally, not data

      flock $_DAT_LOCK, LOCK_EX if ($_lock_chn);
      print {$_DAT_W_SOCK} SHR_H_FST . $LF . $_chn . $LF;
      print {$_DAU_W_SOCK} $_id . $LF;

      chomp($_len = <$_DAU_W_SOCK>);

      if ($_len < 0) {
         flock $_DAT_LOCK, LOCK_UN if ($_lock_chn);
         return undef;
      }

      read  $_DAU_W_SOCK, (my $_buf), $_len;
      flock $_DAT_LOCK, LOCK_UN if ($_lock_chn);

      $_all->{ $_id }->{ $_ } = 1 foreach (@{ $_MCE->{thaw}($_buf) });
   }

   my $_a = scalar keys %{ $_all->{ $_id } };
   each %{ $_all->{ $_id } };
}

sub NEXTKEY {                                     ## Hash NEXTKEY
   $_id = ${ $_[0] };

   unless ($MCE::MCE->{_wid}) {
      each %{ $_all->{ $_id } };
   }
   else {
      $_ret = each %{ $_all->{ $_id } };

      unless (defined $_ret) {
         %{ $_all->{ $_id } } = ();
         delete $_all->{ $_id };
      }

      return $_ret;
   }
}

sub EXISTS {                                      ## Hash EXISTS
   $_id = ${ $_[0] };

   unless ($MCE::MCE->{_wid}) {
      exists $_all->{ $_id }->{ $_[1] };
   }
   else {
      local $\ = undef if (defined $\);
      local $/ = $LF if (!$/ || $/ ne $LF);

      flock $_DAT_LOCK, LOCK_EX if ($_lock_chn);
      print {$_DAT_W_SOCK} SHR_H_EXI . $LF . $_chn . $LF;
      print {$_DAU_W_SOCK} $_id . $LF . length($_[1]) . $LF . $_[1];

      chomp($_ret = <$_DAU_W_SOCK>);
      flock $_DAT_LOCK, LOCK_UN if ($_lock_chn);

      return $_ret;
   }
}

sub DELETE {                                      ## Hash DELETE
   $_id = ${ $_[0] };

   unless ($MCE::MCE->{_wid}) {
      delete $_all->{ $_id }->{ $_[1] };
   }
   else {
      $_wa = (!defined wantarray) ? WA_UNDEF : WA_SCALAR;
      local $\ = undef if (defined $\);

      flock $_DAT_LOCK, LOCK_EX if ($_lock_chn);
      print {$_DAT_W_SOCK} SHR_H_DEL . $LF . $_chn . $LF;
      print {$_DAU_W_SOCK} $_id . $LF . $_wa . $LF . length($_[1]).$LF . $_[1];

      unless ($_wa) {
         flock $_DAT_LOCK, LOCK_UN if ($_lock_chn);
      } else {
         $_do_ret->();
      }
   }
}

sub CLEAR {                                       ## Hash CLEAR
   $_id = ${ $_[0] };

   unless ($MCE::MCE->{_wid}) {
      %{ $_all->{ $_id } } = ();
   }
   else {
      local $\ = undef if (defined $\);

      flock $_DAT_LOCK, LOCK_EX if ($_lock_chn);
      print {$_DAT_W_SOCK} SHR_H_CLR . $LF . $_chn . $LF;
      print {$_DAU_W_SOCK} $_id . $LF;
      flock $_DAT_LOCK, LOCK_UN if ($_lock_chn);

      return;
   }
}

sub SCALAR {                                      ## Hash SCALAR
   $_id = ${ $_[0] };

   unless ($MCE::MCE->{_wid}) {
      scalar %{ $_all->{ $_id } };
   }
   else {
      local $\ = undef if (defined $\);
      local $/ = $LF if (!$/ || $/ ne $LF);

      flock $_DAT_LOCK, LOCK_EX if ($_lock_chn);
      print {$_DAT_W_SOCK} SHR_H_SCA . $LF . $_chn . $LF;
      print {$_DAU_W_SOCK} $_id . $LF;

      chomp($_ret = <$_DAU_W_SOCK>);
      flock $_DAT_LOCK, LOCK_UN if ($_lock_chn);

      return $_ret;
   }
}

## ----------------------------------------------------------------------------

sub add {                                         ## Addition
   $_id = ${ $_[0] };

   MCE::_croak('Use of uninitialized key in hash element')
      unless (defined $_[1]);
   MCE::_croak('Use of uninitialized value in addition (+)')
      unless (defined $_[2]);
   MCE::_croak("Argument \"$_[2]\" isn't numeric in addition (+)")
      unless (looks_like_number($_[2]));

   unless ($MCE::MCE->{_wid}) {
      $_all->{ $_id }->{ $_[1] } += $_[2];
   } else {
      $_do_op->(SHR_H_ADD, $_id, $_[1], $_[2]);
   }
}

sub concat {                                      ## Concatenation
   $_id = ${ $_[0] };

   MCE::_croak('Use of uninitialized key in hash element')
      unless (defined $_[1]);
   MCE::_croak('Use of uninitialized value in concatenation (.)')
      unless (defined $_[2]);

   unless ($MCE::MCE->{_wid}) {
      $_all->{ $_id }->{ $_[1] } .= $_[2];
   } else {
      $_do_op->(SHR_H_CAT, $_id, $_[1], $_[2]);
   }
}

sub subtract {                                    ## Substraction
   $_id = ${ $_[0] };

   MCE::_croak('Use of uninitialized key in hash element')
      unless (defined $_[1]);
   MCE::_croak('Use of uninitialized value in substraction (-)')
      unless (defined $_[2]);
   MCE::_croak("Argument \"$_[2]\" isn't numeric in substraction (-)")
      unless (looks_like_number($_[2]));

   unless ($MCE::MCE->{_wid}) {
      $_all->{ $_id }->{ $_[1] } -= $_[2];
   } else {
      $_do_op->(SHR_H_SUB, $_id, $_[1], $_[2]);
   }
}

1;

