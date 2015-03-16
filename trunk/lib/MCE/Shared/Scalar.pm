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

no warnings 'threads';
no warnings 'recursion';
no warnings 'uninitialized';

use Scalar::Util qw( blessed refaddr reftype weaken );
use bytes;

our $VERSION  = '1.699';

our @CARP_NOT = qw( MCE::Shared MCE );

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
## Import routine.
##
###############################################################################

my $_all = {};

my $LF = "\012"; Internals::SvREADONLY($LF, 1);
my $_loaded;

sub import {

   my $_class = shift; return if ($_loaded++);

   if (defined $MCE::VERSION) {
      _mce_m_init();
   } else {
      $\ = undef; require Carp;
      Carp::croak(
         "MCE::Shared::Scalar cannot be used directly. Please see the\n".
         "MCE::Shared documentation for more information.\n\n"
      );
   }

   return;
}

###############################################################################
## ----------------------------------------------------------------------------
## Private methods.
##
###############################################################################

sub _share_w_init {

   %{ $_all } = ();
}

sub _share {

   my ($_cloned, $_item) = (shift, shift);
   my ($_id, $_copy) = (refaddr($_item), );

   return $_item if (tied ${ $_item });

   unless (exists $_all->{ $_id }) {

      if (scalar @_ > 0) {
         $_all->{ $_id } = \do{ my $_scalar = $_[0]; };
      }
      else {
         if (reftype($_item) eq 'SCALAR') {
            $_all->{ $_id } = \do{ my $_scalar = ${ $_item }; };
         }
         else {
            ## Special handling for $x = \$x
            if ($_id == refaddr(${ $_item })) {
               $_all->{ $_id } = \$_copy;
            } else {
               $_all->{ $_id } = \do{
                  my $_scalar = MCE::Shared::_copy($_cloned, ${ $_item });
               };
            }
         }
      }

      $_cloned->{ $_id } = $_all->{ $_id };

      my $_class   = blessed($_item);
      my $_rdonly1 = Internals::SvREADONLY(${ $_item });
      my $_rdonly2 = Internals::SvREADONLY(   $_item  );

      if ($] >= 5.008003) {
         Internals::SvREADONLY(${ $_item }, 0) if $_rdonly1;
         Internals::SvREADONLY(   $_item  , 0) if $_rdonly2;
      }

      undef ${ $_item };

      tie ${ $_item }, 'MCE::Shared::Scalar::Tie', \$_id;

      ## Bless copy into the same class. Clone READONLY flag.
      bless($_item, $_class) if $_class;

      if ($] >= 5.008003) {
         Internals::SvREADONLY(${ $_item }, 1) if $_rdonly1;
         Internals::SvREADONLY(   $_item  , 1) if $_rdonly2;
      }
   }

   return $_item;
}

sub _share2 {

   if ($_[0] eq 'HASH') {
      MCE::Shared::Hash::_share({}, ${ $_all->{ $_[1] } });
   } elsif ($_[0] eq 'ARRAY') {
      MCE::Shared::Array::_share({}, ${ $_all->{ $_[1] } });
   } elsif ($_[0] eq 'SCALAR' || $_[0] eq 'REF') {
      MCE::Shared::Scalar::_share({}, ${ $_all->{ $_[1] } });
   } else {
      MCE::Shared::_unsupported($_[0]);
   }
}

sub _untie2 {

   if ($_[0] eq 'HASH') {
      untie %{ ${ $_all->{ $_[1] } } };
   } elsif ($_[0] eq 'ARRAY') {
      untie @{ ${ $_all->{ $_[1] } } };
   } elsif ($_[0] eq 'SCALAR' || $_[0] eq 'REF') {
      untie ${ ${ $_all->{ $_[1] } } };
   }
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

      if ($_wa) {
         print {$_DAU_R_SOCK} length(${ $_all->{ $_id } }) . $LF .
            ${ $_all->{ $_id } };
      }

      return;
   };

   ## -------------------------------------------------------------------------

   my %_output_function = (

      SHR_S_STO.$LF => sub {                      ## Scalar STORE
         $_DAU_R_SOCK = ${ $_DAU_R_SOCK_REF };

         chomp($_id  = <$_DAU_R_SOCK>);
         chomp($_len = <$_DAU_R_SOCK>);

         if (my $_r = reftype(${ $_all->{ $_id } })) {
            _untie2($_r, $_id);
         }

         if ($_len > 0) {
            read $_DAU_R_SOCK, (my $_buf), $_len;
            ${ $_all->{ $_id } } = (chop $_buf) ? $_MCE->{thaw}($_buf) : $_buf;
         }
         else {
            ${ $_all->{ $_id } } = undef;
         }

         if (my $_r = reftype(${ $_all->{ $_id } })) {
            _share2($_r, $_id);
         }

         return;
      },

      SHR_S_FCH.$LF => sub {                      ## Scalar FETCH
         $_DAU_R_SOCK = ${ $_DAU_R_SOCK_REF };

         chomp($_id = <$_DAU_R_SOCK>);
         my $_buf = ${ $_all->{ $_id } };

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
            if (reftype($_all->{ $_id }) eq 'SCALAR') {
               ${ $_all->{ $_id } } += $_buf;
            }
         });
      },

      SHR_S_CAT.$LF => sub {                      ## Concatenation
         $_cb_op->( sub {
            read $_DAU_R_SOCK, (my $_buf), $_len;
            if (reftype($_all->{ $_id }) eq 'SCALAR') {
               ${ $_all->{ $_id } } .= $_buf;
            }
         });
      },

      SHR_S_SUB.$LF => sub {                      ## Subtraction
         $_cb_op->( sub {
            read $_DAU_R_SOCK, (my $_buf), $_len;
            if (reftype($_all->{ $_id }) eq 'SCALAR') {
               ${ $_all->{ $_id } } -= $_buf;
            }
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
      $_len = $_wa = $_id = undef;

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
my ($_len, $_wa);

sub _mce_w_init {

   ($_MCE) = @_;

   if (!defined $MCE::Shared::_HDLR ||
         refaddr($_MCE) == refaddr($MCE::Shared::_HDLR)) {

      ## MCE::Queue, MCE::Shared data managed by each manager process.
     ($_chn, $_lock_chn) = ($_MCE->{_chn}, $_MCE->{_lock_chn});
      $_DAT_LOCK = $_MCE->{_dat_lock};
   }
   else {
      ## Data is managed by the top MCE instance; shared_handler => 1.
      $_chn = $_MCE->{_wid} % $MCE::Shared::_HDLR->{_data_channels} + 1;

     ($_MCE, $_lock_chn) = ($MCE::Shared::_HDLR, 1);
      $_DAT_LOCK = $_MCE->{'_cmutex_'.$_chn};
   }

   $_DAT_W_SOCK  = $_MCE->{_dat_w_sock}->[0];
   $_DAU_W_SOCK  = $_MCE->{_dat_w_sock}->[$_chn];

   return;
}

my $_do_op = sub {

   $_wa = (!defined wantarray) ? WA_UNDEF : WA_SCALAR;
   local $\ = undef if (defined $\);

   $_DAT_LOCK->lock() if ($_lock_chn);
   print {$_DAT_W_SOCK} $_[0] . $LF . $_chn . $LF;
   print {$_DAU_W_SOCK} $_[1] . $LF . $_wa . $LF . length($_[2]) . $LF . $_[2];

   my $_buf; if ($_wa) {
      local $/ = $LF if (!$/ || $/ ne $LF);
      chomp($_len = <$_DAU_W_SOCK>);
      read $_DAU_W_SOCK, $_buf, $_len;
   }

   $_DAT_LOCK->unlock() if ($_lock_chn);

   return $_buf;
};

###############################################################################
## ----------------------------------------------------------------------------
## Scalar tie package.
##
###############################################################################

package MCE::Shared::Scalar::Tie;

no warnings 'threads';
no warnings 'recursion';
no warnings 'uninitialized';

use Scalar::Util qw( looks_like_number reftype );
use bytes;

our @CARP_NOT = qw( MCE::Shared MCE );

use constant {
   SHR_S_STO => 'S~STO',   ## STORE
   SHR_S_FCH => 'S~FCH',   ## FETCH

   SHR_S_ADD => 'S~ADD',   ## Addition
   SHR_S_CAT => 'S~CAT',   ## Concatenation
   SHR_S_SUB => 'S~SUB',   ## Subtraction
};

sub TIESCALAR { bless $_[1] => $_[0]; }

sub UNTIE {
   my $_id = ${ $_[0] };

   if ($_MCE->{_wid}) {
      MCE::_croak("Method (UNTIE) is not allowed by the worker process");
   }

   if ($_[1]) {
      my $s = ($_[1] > 1) ? 's' : '';
      Carp::croak("Untied attempted while $_[1] reference${s} still exist")
         if (!defined($MCE::Shared::untie_warn));
      Carp::carp("Untied attempted while $_[1] reference${s} still exist")
         if ($MCE::Shared::untie_warn);
   }

   if (my $_r = reftype(${ $_all->{ $_id } })) {
      MCE::Shared::Scalar::_untie2($_r, $_id);
   }

   undef ${ $_all->{ $_id } };
   delete $_all->{ $_id };

   return;
}

## ----------------------------------------------------------------------------

sub STORE {                                       ## Scalar STORE
   my $_id = ${ $_[0] };

   unless ($_MCE->{_wid}) {
      if (my $_r = reftype(${ $_all->{ $_id } })) {
         MCE::Shared::Scalar::_untie2($_r, $_id);
      }
      ${ $_all->{ $_id } } = $_[1];

      if (my $_r = reftype(${ $_all->{ $_id } })) {
         MCE::Shared::Scalar::_share2($_r, $_id);
      }
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

      $_DAT_LOCK->lock() if ($_lock_chn);
      print {$_DAT_W_SOCK} SHR_S_STO . $LF . $_chn . $LF;
      print {$_DAU_W_SOCK} $_buf;
      $_DAT_LOCK->unlock() if ($_lock_chn);

      return;
   }
}

sub FETCH {                                       ## Scalar FETCH
   my $_id = ${ $_[0] };

   unless ($_MCE->{_wid}) {
      ${ $_all->{ $_id } };
   }
   else {
      local $\ = undef if (defined $\);
      local $/ = $LF if (!$/ || $/ ne $LF);

      $_DAT_LOCK->lock() if ($_lock_chn);
      print {$_DAT_W_SOCK} SHR_S_FCH . $LF . $_chn . $LF;
      print {$_DAU_W_SOCK} $_id . $LF;

      chomp($_len = <$_DAU_W_SOCK>);

      if ($_len < 0) {
         $_DAT_LOCK->unlock() if ($_lock_chn);
         return undef;
      }

      read $_DAU_W_SOCK, (my $_buf), $_len;
      $_DAT_LOCK->unlock() if ($_lock_chn);

      return (chop $_buf) ? $_MCE->{thaw}($_buf) : $_buf;
   }
}

## ----------------------------------------------------------------------------

sub add {                                         ## Addition
   my $_id = ${ $_[0] };

   MCE::_croak('Use of uninitialized value in addition (+)')
      unless (defined $_[1]);
   MCE::_croak("Argument \"$_[1]\" isn't numeric in addition (+)")
      unless (looks_like_number($_[1]));

   unless ($_MCE->{_wid}) {
      if (reftype($_all->{ $_id }) eq 'SCALAR') {
         ${ $_all->{ $_id } } += $_[1];
      }
   } else {
      $_do_op->(SHR_S_ADD, $_id, $_[1]);
   }
}

sub concat {                                      ## Concatenation
   my $_id = ${ $_[0] };

   MCE::_croak('Use of uninitialized value in concatenation (.)')
      unless (defined $_[1]);

   unless ($_MCE->{_wid}) {
      if (reftype($_all->{ $_id }) eq 'SCALAR') {
         ${ $_all->{ $_id } } .= $_[1];
      }
   } else {
      $_do_op->(SHR_S_CAT, $_id, $_[1]);
   }
}

sub subtract {                                    ## Substraction
   my $_id = ${ $_[0] };

   MCE::_croak('Use of uninitialized value in substraction (-)')
      unless (defined $_[1]);
   MCE::_croak("Argument \"$_[1]\" isn't numeric in substraction (-)")
      unless (looks_like_number($_[1]));

   unless ($_MCE->{_wid}) {
      if (reftype($_all->{ $_id }) eq 'SCALAR') {
         ${ $_all->{ $_id } } -= $_[1];
      }
   } else {
      $_do_op->(SHR_S_SUB, $_id, $_[1]);
   }
}

1;

