###############################################################################
## ----------------------------------------------------------------------------
## MCE::Shared::Array - For sharing data structures between workers.
##
## There is no public API.
##
###############################################################################

package MCE::Shared::Array;

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
   SHR_A_FSZ => 'A~FSZ',   ## FETCHSIZE
   SHR_A_SSZ => 'A~SSZ',   ## STORESIZE
   SHR_A_STO => 'A~STO',   ## STORE
   SHR_A_FCH => 'A~FCH',   ## FETCH
   SHR_A_CLR => 'A~CLR',   ## CLEAR
   SHR_A_POP => 'A~POP',   ## POP
   SHR_A_PSH => 'A~PSH',   ## PUSH
   SHR_A_SFT => 'A~SFT',   ## SHIFT
   SHR_A_UFT => 'A~UFT',   ## UNSHIFT
   SHR_A_EXI => 'A~EXI',   ## EXISTS
   SHR_A_DEL => 'A~DEL',   ## DELETE
   SHR_A_SPL => 'A~SPL',   ## SPLICE

   SHR_A_ADD => 'A~ADD',   ## Addition
   SHR_A_CAT => 'A~CAT',   ## Concatenation
   SHR_A_SUB => 'A~SUB',   ## Subtraction

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
         "MCE::Shared::Array cannot be used directly. Please see the\n".
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

   return $_item if (tied @{ $_item });

   unless (exists $_all->{ $_id }) {
      $_all->{ $_id } = $_copy = []; $_cloned->{ $_id } = $_copy;

      if (scalar @_ > 0) {
         push @{ $_copy }, map { MCE::Shared::_copy($_cloned, $_) } @_;
         @_ = ();
      } else {
         push @{ $_copy }, map { MCE::Shared::_copy($_cloned, $_) } @{ $_item };
      }

      my $_class   = blessed($_item);
      my $_rdonly1 = Internals::SvREADONLY(@{ $_item });
      my $_rdonly2 = Internals::SvREADONLY(   $_item  );

      if ($] >= 5.008003) {
         Internals::SvREADONLY(@{ $_item }, 0) if $_rdonly1;
         Internals::SvREADONLY(   $_item  , 0) if $_rdonly2;
      }

      @{ $_item } = ();

      tie @{ $_item }, 'MCE::Shared::Array::Tie', \$_id;

      ## Bless copy into the same class. Clone READONLY flag.
      bless($_item, $_class) if $_class;

      if ($] >= 5.008003) {
         Internals::SvREADONLY(@{ $_item }, 1) if $_rdonly1;
         Internals::SvREADONLY(   $_item  , 1) if $_rdonly2;
      }
   }

   return $_item;
}

sub _share2 {

   if ($_[0] eq 'HASH') {
      MCE::Shared::Hash::_share({}, $_all->{ $_[1] }->[ $_[2] ]);
   } elsif ($_[0] eq 'ARRAY') {
      MCE::Shared::Array::_share({}, $_all->{ $_[1] }->[ $_[2] ]);
   } elsif ($_[0] eq 'SCALAR' || $_[0] eq 'REF') {
      MCE::Shared::Scalar::_share({}, $_all->{ $_[1] }->[ $_[2] ]);
   } else {
      MCE::Shared::_unsupported($_[0]);
   }
}

sub _untie2 {

   if ($_[0] eq 'HASH') {
      untie %{ $_all->{ $_[1] }->[ $_[2] ] };
   } elsif ($_[0] eq 'ARRAY') {
      untie @{ $_all->{ $_[1] }->[ $_[2] ] };
   } elsif ($_[0] eq 'SCALAR' || $_[0] eq 'REF') {
      untie ${ $_all->{ $_[1] }->[ $_[2] ] };
   }
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
      chomp($_key = <$_DAU_R_SOCK>);
      chomp($_len = <$_DAU_R_SOCK>);

      weaken $_[0]; $_[0]->();

      if ($_wa) {
         print {$_DAU_R_SOCK} length($_all->{ $_id }->[ $_key ]) . $LF .
            $_all->{ $_id }->[ $_key ];
      }

      return;
   };

   my $_cb_push = sub {

      $_DAU_R_SOCK = ${ $_DAU_R_SOCK_REF };

      chomp($_id  = <$_DAU_R_SOCK>);
      chomp($_len = <$_DAU_R_SOCK>);

      read $_DAU_R_SOCK, (my $_buf), $_len;

      if (chop $_buf) {
         if ($_[0]) {
            push(@{ $_all->{ $_id } }, @{ $_MCE->{thaw}($_buf) });
         } else {
            unshift(@{ $_all->{ $_id } }, @{ $_MCE->{thaw}($_buf) });
         }
      }
      else {
         if ($_[0]) {
            push(@{ $_all->{ $_id } }, $_buf);
         } else {
            unshift(@{ $_all->{ $_id } }, $_buf);
         }
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

      SHR_A_FSZ.$LF => sub {                      ## Array FETCHSIZE
         $_DAU_R_SOCK = ${ $_DAU_R_SOCK_REF };

         chomp($_id = <$_DAU_R_SOCK>);
         $_ret = scalar @{ $_all->{ $_id } };

         print {$_DAU_R_SOCK} $_ret . $LF;

         return;
      },

      SHR_A_SSZ.$LF => sub {                      ## Array STORESIZE
         $_DAU_R_SOCK = ${ $_DAU_R_SOCK_REF };

         chomp($_id  = <$_DAU_R_SOCK>);
         chomp($_len = <$_DAU_R_SOCK>);

         $#{ $_all->{ $_id } } = $_len - 1;

         return;
      },

      SHR_A_STO.$LF => sub {                      ## Array STORE
         $_DAU_R_SOCK = ${ $_DAU_R_SOCK_REF };

         chomp($_id  = <$_DAU_R_SOCK>);
         chomp($_key = <$_DAU_R_SOCK>);
         chomp($_len = <$_DAU_R_SOCK>);

         if (my $_r = reftype($_all->{ $_id }->[ $_key ])) {
            _untie2($_r, $_id, $_key);
         }

         if ($_len > 0) {
            read $_DAU_R_SOCK, (my $_buf), $_len;
            $_all->{ $_id }->[ $_key ] =
               (chop $_buf) ? $_MCE->{thaw}($_buf) : $_buf;
         }
         else {
            $_all->{ $_id }->[ $_key ] = undef;
         }

         if (my $_r = reftype($_all->{ $_id }->[ $_key ])) {
            _share2($_r, $_id, $_key);
         }

         return;
      },

      SHR_A_FCH.$LF => sub {                      ## Array FETCH
         $_DAU_R_SOCK = ${ $_DAU_R_SOCK_REF };

         chomp($_id  = <$_DAU_R_SOCK>);
         chomp($_key = <$_DAU_R_SOCK>);

         my $_buf = $_all->{ $_id }->[ $_key ];
         $_cb_ret->($_buf);

         return;
      },

      SHR_A_CLR.$LF => sub {                      ## Array CLEAR
         $_DAU_R_SOCK = ${ $_DAU_R_SOCK_REF };

         my $_id; chomp($_id = <$_DAU_R_SOCK>);

         for my $_k (0 .. @{ $_all->{ $_id } } - 1) {
            if (my $_r = reftype($_all->{ $_id }->[ $_k ])) {
               _untie2($_r, $_id, $_k);
            }
         }

         @{ $_all->{ $_id } } = ();

         return;
      },

      SHR_A_POP.$LF => sub {                      ## Array POP
         $_DAU_R_SOCK = ${ $_DAU_R_SOCK_REF };

         chomp($_id = <$_DAU_R_SOCK>);

         my $_buf = pop(@{ $_all->{ $_id } });
         $_cb_ret->($_buf);

         return;
      },

      SHR_A_PSH.$LF => sub {                      ## Array PUSH

         $_cb_push->(1);
      },

      SHR_A_SFT.$LF => sub {                      ## Array SHIFT
         $_DAU_R_SOCK = ${ $_DAU_R_SOCK_REF };

         chomp($_id = <$_DAU_R_SOCK>);

         my $_buf = shift(@{ $_all->{ $_id } });
         $_cb_ret->($_buf);

         return;
      },

      SHR_A_UFT.$LF => sub {                      ## Array UNSHIFT

         $_cb_push->(0);
      },

      SHR_A_EXI.$LF => sub {                      ## Array EXISTS
         $_DAU_R_SOCK = ${ $_DAU_R_SOCK_REF };

         chomp($_id  = <$_DAU_R_SOCK>);
         chomp($_key = <$_DAU_R_SOCK>);

         $_ret = (exists $_all->{ $_id }->[ $_key ]) ? 1 : 0;

         print {$_DAU_R_SOCK} $_ret . $LF;

         return;
      },

      SHR_A_DEL.$LF => sub {                      ## Array DELETE
         $_DAU_R_SOCK = ${ $_DAU_R_SOCK_REF };

         chomp($_id  = <$_DAU_R_SOCK>);
         chomp($_wa  = <$_DAU_R_SOCK>);
         chomp($_key = <$_DAU_R_SOCK>);

         if (my $_r = reftype($_all->{ $_id }->[ $_key ])) {
            if ($_r eq 'HASH') {
               my %_buf; %_buf = %{ $_all->{ $_id }->[ $_key ] } if ($_wa);
               untie %{ $_all->{ $_id }->[ $_key ] };
               delete $_all->{ $_id }->[ $_key ];
               $_cb_ret->(\%_buf) if ($_wa);
               return;
            } elsif ($_r eq 'ARRAY') {
               my @_buf; @_buf = @{ $_all->{ $_id }->[ $_key ] } if ($_wa);
               untie @{ $_all->{ $_id }->[ $_key ] };
               delete $_all->{ $_id }->[ $_key ];
               $_cb_ret->(\@_buf) if ($_wa);
               return;
            } elsif ($_r eq 'SCALAR' || $_r eq 'REF') {
               my $_buf; $_buf = ${ $_all->{ $_id }->[ $_key ] } if ($_wa);
               untie ${ $_all->{ $_id }->[ $_key ] };
               delete $_all->{ $_id }->[ $_key ];
               $_cb_ret->(\$_buf) if ($_wa);
               return;
            }
         }

         if ($_wa) {
            my $_buf = delete $_all->{ $_id }->[ $_key ];
            $_cb_ret->($_buf);
         } else {
            delete $_all->{ $_id }->[ $_key ];
         }

         return;
      },

      SHR_A_SPL.$LF => sub {                      ## Array SPLICE
         $_DAU_R_SOCK = ${ $_DAU_R_SOCK_REF };

         chomp($_id  = <$_DAU_R_SOCK>);
         chomp($_wa  = <$_DAU_R_SOCK>);
         chomp($_len = <$_DAU_R_SOCK>);

         read $_DAU_R_SOCK, (my $_buf), $_len;
         my @_a = @{ $_MCE->{thaw}($_buf) };

         my $_sz  = scalar @{ $_all->{ $_id } };
         my $_off = @_a ? shift(@_a) : 0;
         $_off   += $_sz if $_off < 0;
         $_len    = @_a ? shift(@_a) : $_sz - $_off;

         if ($_wa == 1) {
            my @_tmp = splice(@{ $_all->{ $_id } }, $_off, $_len, @_a);
            $_buf = $_MCE->{freeze}(\@_tmp);
         } else {
            $_buf = splice(@{ $_all->{ $_id } }, $_off, $_len, @_a);
         }

         $_cb_ret->($_buf) if ($_wa);

         return;
      },

      ## ----------------------------------------------------------------------

      SHR_A_ADD.$LF => sub {                      ## Addition
         $_cb_op->( sub {
            read $_DAU_R_SOCK, (my $_buf), $_len;
            if (reftype($_all->{ $_id }) eq 'ARRAY') {
               $_all->{ $_id }->[ $_key ] += $_buf;
            }
         });
      },

      SHR_A_CAT.$LF => sub {                      ## Concatenation
         $_cb_op->( sub {
            read $_DAU_R_SOCK, (my $_buf), $_len;
            if (reftype($_all->{ $_id }) eq 'ARRAY') {
               $_all->{ $_id }->[ $_key ] .= $_buf;
            }
         });
      },

      SHR_A_SUB.$LF => sub {                      ## Subtraction
         $_cb_op->( sub {
            read $_DAU_R_SOCK, (my $_buf), $_len;
            if (reftype($_all->{ $_id }) eq 'ARRAY') {
               $_all->{ $_id }->[ $_key ] -= $_buf;
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
      $_key = $_len = $_ret = $_wa = $_id = undef;

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

my ($_MCE, $_DAT_LOCK, $_DAT_W_SOCK, $_DAU_W_SOCK, $_chn, $_dat_ex, $_dat_un);
my ($_len, $_ret, $_tag, $_wa);

sub _mce_w_init {

   ($_MCE) = @_;

   if (!defined $MCE::Shared::_HDLR ||
         refaddr($_MCE) == refaddr($MCE::Shared::_HDLR)) {

      ## MCE::Queue, MCE::Shared data managed by each manager process.
      $_chn = $_MCE->{_chn}; $_DAT_LOCK = $_MCE->{_dat_lock};
   }
   else {
      ## Data is managed by the top MCE instance; shared_handler => 1.
      $_chn = $_MCE->{_wid} % $MCE::Shared::_HDLR->{_data_channels} + 1;

      $_MCE = $MCE::Shared::_HDLR;
      $_DAT_LOCK = $_MCE->{'_mutex_'.$_chn};
   }

   $_dat_ex = sub { sysread(  $_DAT_LOCK->{_r_sock}, my $_b, 1 ) };
   $_dat_un = sub { syswrite( $_DAT_LOCK->{_w_sock}, '0' ) };

   $_DAT_W_SOCK = $_MCE->{_dat_w_sock}->[0];
   $_DAU_W_SOCK = $_MCE->{_dat_w_sock}->[$_chn];

   return;
}

my $_do_op = sub {

   $_wa = (!defined wantarray) ? WA_UNDEF : WA_SCALAR;
   local $\ = undef if (defined $\);

   $_dat_ex->();

   print {$_DAT_W_SOCK} $_[0] . $LF . $_chn . $LF;
   print {$_DAU_W_SOCK} $_[1] . $LF . $_wa . $LF .
      $_[2] . $LF . length($_[3]) . $LF . $_[3];

   my $_buf; if ($_wa) {
      local $/ = $LF if (!$/ || $/ ne $LF);
      chomp($_len = <$_DAU_W_SOCK>);
      read $_DAU_W_SOCK, $_buf, $_len;
   }

   $_dat_un->();

   return $_buf;
};

my $_do_push = sub {

   my ($_buf, $_tmp); my $_id = shift;

   if (scalar @_ > 1 || ref $_[0] || !defined $_[0]) {
      $_tmp = $_MCE->{freeze}(\@_);
      $_buf = $_id . $LF . (length($_tmp) + 1) . $LF . $_tmp . '1';
   } else {
      $_buf = $_id . $LF . (length($_[0]) + 1) . $LF . $_[0] . '0';
   }

   local $\ = undef if (defined $\);

   $_dat_ex->();
   print {$_DAT_W_SOCK} $_tag . $LF . $_chn . $LF;
   print {$_DAU_W_SOCK} $_buf;
   $_dat_un->();

   return;
};

my $_do_ret = sub {

   local $/ = $LF if (!$/ || $/ ne $LF);
   chomp($_len = <$_DAU_W_SOCK>);

   if ($_len < 0) {
      $_dat_un->();
      return undef;
   }

   read $_DAU_W_SOCK, (my $_buf), $_len;
   $_dat_un->();

   return (chop $_buf) ? $_MCE->{thaw}($_buf) : $_buf;
};

###############################################################################
## ----------------------------------------------------------------------------
## Array tie package.
##
###############################################################################

package MCE::Shared::Array::Tie;

no warnings 'threads';
no warnings 'recursion';
no warnings 'uninitialized';

use Scalar::Util qw( looks_like_number reftype );
use bytes;

our @CARP_NOT = qw( MCE::Shared MCE );

use constant {
   SHR_A_FSZ => 'A~FSZ',   ## FETCHSIZE
   SHR_A_SSZ => 'A~SSZ',   ## STORESIZE
   SHR_A_STO => 'A~STO',   ## STORE
   SHR_A_FCH => 'A~FCH',   ## FETCH
   SHR_A_CLR => 'A~CLR',   ## CLEAR
   SHR_A_POP => 'A~POP',   ## POP
   SHR_A_PSH => 'A~PSH',   ## PUSH
   SHR_A_SFT => 'A~SFT',   ## SHIFT
   SHR_A_UFT => 'A~UFT',   ## UNSHIFT
   SHR_A_EXI => 'A~EXI',   ## EXISTS
   SHR_A_DEL => 'A~DEL',   ## DELETE
   SHR_A_SPL => 'A~SPL',   ## SPLICE

   SHR_A_ADD => 'A~ADD',   ## Addition
   SHR_A_CAT => 'A~CAT',   ## Concatenation
   SHR_A_SUB => 'A~SUB',   ## Subtraction

   WA_UNDEF  => 0,         ## Wants nothing
   WA_ARRAY  => 1,         ## Wants list
   WA_SCALAR => 2,         ## Wants scalar
};

sub TIEARRAY { bless $_[1] => $_[0]; }
sub EXTEND   { }

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

   for my $_k (0 .. @{ $_all->{ $_id } } - 1) {
      if (my $_r = reftype($_all->{ $_id }->[ $_k ])) {
         MCE::Shared::Array::_untie2($_r, $_id, $_k);
      }
   }

   delete $_all->{ $_id };

   return;
}

## ----------------------------------------------------------------------------

sub FETCHSIZE {                                   ## Array FETCHSIZE
   my $_id = ${ $_[0] };

   unless ($_MCE->{_wid}) {
      scalar @{ $_all->{ $_id } };
   }
   else {
      local $\ = undef if (defined $\);
      local $/ = $LF if (!$/ || $/ ne $LF);

      $_dat_ex->();
      print {$_DAT_W_SOCK} SHR_A_FSZ . $LF . $_chn . $LF;
      print {$_DAU_W_SOCK} $_id . $LF;

      chomp($_ret = <$_DAU_W_SOCK>);
      $_dat_un->();

      return $_ret;
   }
}

sub STORESIZE {                                   ## Array STORESIZE
   my $_id = ${ $_[0] };

   unless ($_MCE->{_wid}) {
      $#{ $_all->{ $_id } } = $_[1] - 1;
   }
   else {
      local $\ = undef if (defined $\);

      $_dat_ex->();
      print {$_DAT_W_SOCK} SHR_A_SSZ . $LF . $_chn . $LF;
      print {$_DAU_W_SOCK} $_id . $LF . $_[1] . $LF;
      $_dat_un->();

      return;
   }
}

sub STORE {                                       ## Array STORE
   my $_id = ${ $_[0] };

   unless ($_MCE->{_wid}) {
      if (my $_r = reftype($_all->{ $_id }->[ $_[1] ])) {
         MCE::Shared::Array::_untie2($_r, $_id, $_[1]);
      }
      $_all->{ $_id }->[ $_[1] ] = $_[2];

      if (my $_r = reftype($_all->{ $_id }->[ $_[1] ])) {
         MCE::Shared::Array::_share2($_r, $_id, $_[1]);
      }
   }
   else {
      my ($_buf, $_tmp);

      unless (defined $_[2]) {
         $_buf = $_id . $LF . $_[1] . $LF . -1 . $LF;
      }
      elsif (reftype $_[2]) {
         $_tmp = $_MCE->{freeze}($_[2]);
         $_buf = $_id . $LF . $_[1] . $LF .
            (length($_tmp) + 1) . $LF . $_tmp . '1';
      }
      else {
         $_buf = $_id . $LF . $_[1] . $LF .
            (length($_[2]) + 1) . $LF . $_[2] . '0';
      }

      local $\ = undef if (defined $\);

      $_dat_ex->();
      print {$_DAT_W_SOCK} SHR_A_STO . $LF . $_chn . $LF;
      print {$_DAU_W_SOCK} $_buf;
      $_dat_un->();

      return;
   }
}

sub FETCH {                                       ## Array FETCH
   my $_id = ${ $_[0] };

   unless ($_MCE->{_wid}) {
      $_all->{ $_id }->[ $_[1] ];
   }
   else {
      local $\ = undef if (defined $\);

      $_dat_ex->();
      print {$_DAT_W_SOCK} SHR_A_FCH . $LF . $_chn . $LF;
      print {$_DAU_W_SOCK} $_id . $LF . $_[1] . $LF;

      $_do_ret->();
   }
}

sub CLEAR {                                       ## Array CLEAR
   my $_id = ${ $_[0] };

   unless ($_MCE->{_wid}) {
      for my $_k (0 .. @{ $_all->{ $_id } } - 1) {
         if (my $_r = reftype($_all->{ $_id }->[ $_k ])) {
            MCE::Shared::Array::_untie2($_r, $_id, $_k);
         }
      }
      @{ $_all->{ $_id } } = ();
   }
   else {
      local $\ = undef if (defined $\);

      $_dat_ex->();
      print {$_DAT_W_SOCK} SHR_A_CLR . $LF . $_chn . $LF;
      print {$_DAU_W_SOCK} $_id . $LF;
      $_dat_un->();

      return;
   }
}

sub POP {                                         ## Array POP
   my $_id = ${ $_[0] };

   unless ($_MCE->{_wid}) {
      pop(@{ $_all->{ $_id } });
   }
   else {
      local $\ = undef if (defined $\);

      $_dat_ex->();
      print {$_DAT_W_SOCK} SHR_A_POP . $LF . $_chn . $LF;
      print {$_DAU_W_SOCK} $_id . $LF;

      $_do_ret->();
   }
}

sub PUSH {                                        ## Array PUSH
   my $_id = ${ (shift) };

   unless ($_MCE->{_wid}) {
      push(@{ $_all->{ $_id } }, @_);
   }
   else {
      if (scalar @_) {
         $_tag = SHR_A_PSH; $_do_push->($_id, @_);
      } else {
         Carp::carp('Useless use of push with no values');
      }
   }
}

sub SHIFT {                                       ## Array SHIFT
   my $_id = ${ $_[0] };

   unless ($_MCE->{_wid}) {
      shift(@{ $_all->{ $_id } });
   }
   else {
      local $\ = undef if (defined $\);

      $_dat_ex->();
      print {$_DAT_W_SOCK} SHR_A_SFT . $LF . $_chn . $LF;
      print {$_DAU_W_SOCK} $_id . $LF;

      $_do_ret->();
   }
}

sub UNSHIFT {                                     ## Array UNSHIFT
   my $_id = ${ (shift) };

   unless ($_MCE->{_wid}) {
      unshift(@{ $_all->{ $_id } }, @_);
   }
   else {
      if (scalar @_) {
         $_tag = SHR_A_UFT; $_do_push->($_id, @_);
      } else {
         Carp::carp('Useless use of unshift with no values');
      }
   }
}

sub EXISTS {                                      ## Array EXISTS
   my $_id = ${ $_[0] };

   unless ($_MCE->{_wid}) {
      exists $_all->{ $_id }->[ $_[1] ];
   }
   else {
      local $\ = undef if (defined $\);
      local $/ = $LF if (!$/ || $/ ne $LF);

      $_dat_ex->();
      print {$_DAT_W_SOCK} SHR_A_EXI . $LF . $_chn . $LF;
      print {$_DAU_W_SOCK} $_id . $LF . $_[1] . $LF;

      chomp($_ret = <$_DAU_W_SOCK>);
      $_dat_un->();

      return $_ret;
   }
}

sub DELETE {                                      ## Array DELETE
   my $_id = ${ $_[0] };

   unless ($_MCE->{_wid}) {
      my $_k = $_[1];

      if (my $_r = reftype($_all->{ $_id }->[ $_k ])) {
         if ($_r eq 'HASH') {
            my %_buf = %{ $_all->{ $_id }->[ $_k ] };
            untie %{ $_all->{ $_id }->[ $_k ] };
            delete $_all->{ $_id }->[ $_k ];
            return \%_buf;
         } elsif ($_r eq 'ARRAY') {
            my @_buf = @{ $_all->{ $_id }->[ $_k ] };
            untie @{ $_all->{ $_id }->[ $_k ] };
            delete $_all->{ $_id }->[ $_k ];
            return \@_buf;
         } elsif ($_r eq 'SCALAR' || $_r eq 'REF') {
            my $_buf = ${ $_all->{ $_id }->[ $_k ] };
            untie ${ $_all->{ $_id }->[ $_k ] };
            delete $_all->{ $_id }->[ $_k ];
            return \$_buf;
         }
      }

      delete $_all->{ $_id }->[ $_k ];
   }
   else {
      $_wa = (!defined wantarray) ? WA_UNDEF : WA_SCALAR;
      local $\ = undef if (defined $\);

      $_dat_ex->();
      print {$_DAT_W_SOCK} SHR_A_DEL . $LF . $_chn . $LF;
      print {$_DAU_W_SOCK} $_id . $LF . $_wa . $LF . $_[1] . $LF;

      unless ($_wa) {
         $_dat_un->();
      } else {
         $_do_ret->();
      }
   }
}

sub SPLICE {                                      ## Array SPLICE
   my $_id = ${ (shift) };

   unless ($_MCE->{_wid}) {
      my $_sz  = scalar @{ $_all->{ $_id } };
      my $_off = @_ ? shift : 0;
      $_off   += $_sz if $_off < 0;
      my $_len = @_ ? shift : $_sz - $_off;
      splice(@{ $_all->{ $_id } }, $_off, $_len, @_);
   }
   else {
      $_wa = !defined wantarray ? WA_UNDEF : wantarray ? WA_ARRAY : WA_SCALAR;
      local $\ = undef if (defined $\);

      my $_tmp = $_MCE->{freeze}(\@_);
      my $_buf = $_id . $LF . $_wa . $LF . length($_tmp) . $LF . $_tmp;

      $_dat_ex->();
      print {$_DAT_W_SOCK} SHR_A_SPL . $LF . $_chn . $LF;
      print {$_DAU_W_SOCK} $_buf;

      unless ($_wa) {
         $_dat_un->();
      }
      elsif ($_wa == 1) {
         local $/ = $LF if (!$/ || $/ ne $LF);
         chomp($_len = <$_DAU_W_SOCK>);

         read $_DAU_W_SOCK, (my $_buf), $_len;
         $_dat_un->();

         chop $_buf; return @{ $_MCE->{thaw}($_buf) };
      }
      else {
         $_do_ret->();
      }
   }
}

## ----------------------------------------------------------------------------

sub add {                                         ## Addition
   my $_id = ${ $_[0] };

   MCE::_croak('Use of uninitialized key in array element')
      unless (defined $_[1]);
   MCE::_croak('Use of uninitialized value in addition (+)')
      unless (defined $_[2]);
   MCE::_croak("Argument \"$_[1]\" isn't numeric in array element")
      unless (looks_like_number($_[1]));
   MCE::_croak("Argument \"$_[2]\" isn't numeric in addition (+)")
      unless (looks_like_number($_[2]));

   unless ($_MCE->{_wid}) {
      if (reftype($_all->{ $_id }) eq 'ARRAY') {
         $_all->{ $_id }->[ $_[1] ] += $_[2];
      }
   } else {
      $_do_op->(SHR_A_ADD, $_id, $_[1], $_[2]);
   }
}

sub concat {                                      ## Concatenation
   my $_id = ${ $_[0] };

   MCE::_croak('Use of uninitialized key in array element')
      unless (defined $_[1]);
   MCE::_croak('Use of uninitialized value in concatenation (.)')
      unless (defined $_[2]);
   MCE::_croak("Argument \"$_[1]\" isn't numeric in array element")
      unless (looks_like_number($_[1]));

   unless ($_MCE->{_wid}) {
      if (reftype($_all->{ $_id }) eq 'ARRAY') {
         $_all->{ $_id }->[ $_[1] ] .= $_[2];
      }
   } else {
      $_do_op->(SHR_A_CAT, $_id, $_[1], $_[2]);
   }
}

sub subtract {                                    ## Substraction
   my $_id = ${ $_[0] };

   MCE::_croak('Use of uninitialized key in array element')
      unless (defined $_[1]);
   MCE::_croak('Use of uninitialized value in substraction (-)')
      unless (defined $_[2]);
   MCE::_croak("Argument \"$_[1]\" isn't numeric in array element")
      unless (looks_like_number($_[1]));
   MCE::_croak("Argument \"$_[2]\" isn't numeric in substraction (-)")
      unless (looks_like_number($_[2]));

   unless ($_MCE->{_wid}) {
      if (reftype($_all->{ $_id }) eq 'ARRAY') {
         $_all->{ $_id }->[ $_[1] ] -= $_[2];
      }
   } else {
      $_do_op->(SHR_A_SUB, $_id, $_[1], $_[2]);
   }
}

1;

