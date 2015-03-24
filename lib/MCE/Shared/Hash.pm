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

no warnings 'threads';
no warnings 'recursion';
no warnings 'uninitialized';

use Scalar::Util qw( blessed refaddr reftype );
use bytes;

our $VERSION  = '1.699';

our @CARP_NOT = qw( MCE::Shared MCE );

use constant {
   SHR_H_STO => 'H~STO',   ## STORE
   SHR_H_FCH => 'H~FCH',   ## FETCH
   SHR_H_FST => 'H~FST',   ## FIRSTKEY / NEXTKEY
   SHR_H_EXI => 'H~EXI',   ## EXISTS
   SHR_H_DEL => 'H~DEL',   ## DELETE
   SHR_H_CLR => 'H~CLR',   ## CLEAR
   SHR_H_SCA => 'H~SCA',   ## SCALAR
};

###############################################################################
## ----------------------------------------------------------------------------
## Import routine.
##
###############################################################################

my $_all = {}; my $_cache = {};

my $LF = "\012"; Internals::SvREADONLY($LF, 1);
my $_loaded;

sub import {

   my $_class = shift; return if ($_loaded++);

   if (defined $MCE::VERSION) {
      _mce_m_init();
   } else {
      $\ = undef; require Carp;
      Carp::croak(
         "MCE::Shared::Hash cannot be used directly. Please see the\n".
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

   return $_item if (tied %{ $_item });

   unless (exists $_all->{ $_id }) {
      $_all->{ $_id } = $_copy = {}; $_cloned->{ $_id } = $_copy;

      if (scalar @_ > 0) {
         while (scalar @_) {
            my ($_key, $_value) = (shift, shift);
            $_copy->{ $_key } = MCE::Shared::_copy($_cloned, $_value);
         }
      } else {
         for my $_key (keys %{ $_item }) {
            $_copy->{ $_key } = MCE::Shared::_copy($_cloned, $_item->{ $_key });
         }
      }

      my $_class   = blessed($_item);
      my $_rdonly1 = Internals::SvREADONLY(%{ $_item });
      my $_rdonly2 = Internals::SvREADONLY(   $_item  );

      if ($] >= 5.008003) {
         Internals::SvREADONLY(%{ $_item }, 0) if $_rdonly1;
         Internals::SvREADONLY(   $_item  , 0) if $_rdonly2;
      }

      %{ $_item } = ();

      tie %{ $_item }, 'MCE::Shared::Hash::Tie', \$_id;

      ## Bless copy into the same class. Clone READONLY flag.
      bless($_item, $_class) if $_class;

      if ($] >= 5.008003) {
         Internals::SvREADONLY(%{ $_item }, 1) if $_rdonly1;
         Internals::SvREADONLY(   $_item  , 1) if $_rdonly2;
      }
   }

   return $_item;
}

sub _share2 {

   if ($_[0] eq 'HASH') {
      MCE::Shared::Hash::_share({}, $_all->{ $_[1] }->{ $_[2] });
   } elsif ($_[0] eq 'ARRAY') {
      MCE::Shared::Array::_share({}, $_all->{ $_[1] }->{ $_[2] });
   } elsif ($_[0] eq 'SCALAR' || $_[0] eq 'REF') {
      MCE::Shared::Scalar::_share({}, $_all->{ $_[1] }->{ $_[2] });
   } else {
      MCE::Shared::_unsupported($_[0]);
   }
}

sub _untie2 {

   if ($_[0] eq 'HASH') {
      untie %{ $_all->{ $_[1] }->{ $_[2] } };
   } elsif ($_[0] eq 'ARRAY') {
      untie @{ $_all->{ $_[1] }->{ $_[2] } };
   } elsif ($_[0] eq 'SCALAR' || $_[0] eq 'REF') {
      untie ${ $_all->{ $_[1] }->{ $_[2] } };
   }
}

###############################################################################
## ----------------------------------------------------------------------------
## Output routines for the manager process.
##
###############################################################################

{
   my ($_MCE, $_DAU_R_SOCK_REF, $_DAU_R_SOCK, $_id, $_key, $_len, $_ret, $_wa);

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
 
         if (my $_r = reftype($_all->{ $_id }->{ $_key })) {
            _untie2($_r, $_id, $_key);
         }

         if ($_len > 0) {
            read $_DAU_R_SOCK, (my $_buf), $_len;
            $_all->{ $_id }->{ $_key } =
               (chop $_buf) ? $_MCE->{thaw}($_buf) : $_buf;
         }
         else {
            $_all->{ $_id }->{ $_key } = undef;
         }

         if (my $_r = reftype($_all->{ $_id }->{ $_key })) {
            _share2($_r, $_id, $_key);
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

         if (my $_r = reftype($_all->{ $_id }->{ $_key })) {
            if ($_r eq 'HASH') {
               my %_buf; %_buf = %{ $_all->{ $_id }->{ $_key } } if ($_wa);
               untie %{ $_all->{ $_id }->{ $_key } };
               delete $_all->{ $_id }->{ $_key };
               $_cb_ret->(\%_buf) if ($_wa);
               return;
            } elsif ($_r eq 'ARRAY') {
               my @_buf; @_buf = @{ $_all->{ $_id }->{ $_key } } if ($_wa);
               untie @{ $_all->{ $_id }->{ $_key } };
               delete $_all->{ $_id }->{ $_key };
               $_cb_ret->(\@_buf) if ($_wa);
               return;
            } elsif ($_r eq 'SCALAR' || $_r eq 'REF') {
               my $_buf; $_buf = ${ $_all->{ $_id }->{ $_key } } if ($_wa);
               untie ${ $_all->{ $_id }->{ $_key } };
               delete $_all->{ $_id }->{ $_key };
               $_cb_ret->(\$_buf) if ($_wa);
               return;
            }
         }

         if ($_wa) {
            my $_buf = delete $_all->{ $_id }->{ $_key };
            $_cb_ret->($_buf);
         } else {
            delete $_all->{ $_id }->{ $_key };
         }

         return;
      },

      SHR_H_CLR.$LF => sub {                      ## Hash CLEAR
         $_DAU_R_SOCK = ${ $_DAU_R_SOCK_REF };

         my $_id; chomp($_id = <$_DAU_R_SOCK>);

         for my $_k (keys %{ $_all->{ $_id } }) {
            if (my $_r = reftype($_all->{ $_id }->{ $_k })) {
               _untie2($_r, $_id, $_k);
            }
         }

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
my ($_len, $_ret, $_wa);

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
## Hash tie package.
##
###############################################################################

package MCE::Shared::Hash::Tie;

no warnings 'threads';
no warnings 'recursion';
no warnings 'uninitialized';

use Scalar::Util qw( looks_like_number reftype );
use bytes;

our @CARP_NOT = qw( MCE::Shared MCE );

use constant {
   SHR_H_STO => 'H~STO',   ## STORE
   SHR_H_FCH => 'H~FCH',   ## FETCH
   SHR_H_FST => 'H~FST',   ## FIRSTKEY / NEXTKEY
   SHR_H_EXI => 'H~EXI',   ## EXISTS
   SHR_H_DEL => 'H~DEL',   ## DELETE
   SHR_H_CLR => 'H~CLR',   ## CLEAR
   SHR_H_SCA => 'H~SCA',   ## SCALAR

   WA_UNDEF  => 0,         ## Wants nothing
   WA_SCALAR => 2,         ## Wants scalar
};

sub TIEHASH { bless $_[1] => $_[0]; }

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

   for my $_k (keys %{ $_all->{ $_id } }) {
      if (my $_r = reftype($_all->{ $_id }->{ $_k })) {
         MCE::Shared::Hash::_untie2($_r, $_id, $_k);
      }
   }

   delete $_all->{ $_id };

   return;
}

## ----------------------------------------------------------------------------

sub STORE {                                       ## Hash STORE
   my $_id = ${ $_[0] };

   unless ($_MCE->{_wid}) {
      if (my $_r = reftype($_all->{ $_id }->{ $_[1] })) {
         MCE::Shared::Hash::_untie2($_r, $_id, $_[1]);
      }
      $_all->{ $_id }->{ $_[1] } = $_[2];

      if (my $_r = reftype($_all->{ $_id }->{ $_[1] })) {
         MCE::Shared::Hash::_share2($_r, $_id, $_[1]);
      }
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

      $_dat_ex->();
      print {$_DAT_W_SOCK} SHR_H_STO . $LF . $_chn . $LF;
      print {$_DAU_W_SOCK} $_buf;
      $_dat_un->();

      return;
   }
}

sub FETCH {                                       ## Hash FETCH
   my $_id = ${ $_[0] };

   unless ($_MCE->{_wid}) {
      $_all->{ $_id }->{ $_[1] };
   }
   else {
      local $\ = undef if (defined $\);

      $_dat_ex->();
      print {$_DAT_W_SOCK} SHR_H_FCH . $LF . $_chn . $LF;
      print {$_DAU_W_SOCK} $_id . $LF . length($_[1]) . $LF . $_[1];

      $_do_ret->();
   }
}

sub FIRSTKEY {                                    ## Hash FIRSTKEY
   my $_id = ${ $_[0] };

   unless ($_MCE->{_wid}) {
      my $_a = scalar keys %{ $_all->{ $_id } };
      each %{ $_all->{ $_id } };
   }
   else {
      local $\ = undef if (defined $\);
      local $/ = $LF if (!$/ || $/ ne $LF);

      $_dat_ex->();
      print {$_DAT_W_SOCK} SHR_H_FST . $LF . $_chn . $LF;
      print {$_DAU_W_SOCK} $_id . $LF;

      chomp($_len = <$_DAU_W_SOCK>);

      if ($_len < 0) {
         $_dat_un->();
         return undef;
      }

      read $_DAU_W_SOCK, (my $_buf), $_len;
      $_dat_un->();

      %{ $_cache->{ $_id } } = ();   ## cache keys only locally

      for my $_k (@{ $_MCE->{thaw}($_buf) }) {
         $_cache->{ $_id }->{ $_k } = 1;
      }

      my $_a = scalar keys %{ $_cache->{ $_id } };
      each %{ $_cache->{ $_id } };
   }
}

sub NEXTKEY {                                     ## Hash NEXTKEY
   my $_id = ${ $_[0] };

   unless ($_MCE->{_wid}) {
      each %{ $_all->{ $_id } };
   }
   else {
      $_ret = each %{ $_cache->{ $_id } };

      unless (defined $_ret) {
         %{ $_cache->{ $_id } } = ();
         delete $_cache->{ $_id };
      }

      return $_ret;
   }
}

sub EXISTS {                                      ## Hash EXISTS
   my $_id = ${ $_[0] };

   unless ($_MCE->{_wid}) {
      exists $_all->{ $_id }->{ $_[1] };
   }
   else {
      local $\ = undef if (defined $\);
      local $/ = $LF if (!$/ || $/ ne $LF);

      $_dat_ex->();
      print {$_DAT_W_SOCK} SHR_H_EXI . $LF . $_chn . $LF;
      print {$_DAU_W_SOCK} $_id . $LF . length($_[1]) . $LF . $_[1];

      chomp($_ret = <$_DAU_W_SOCK>);
      $_dat_un->();

      return $_ret;
   }
}

sub DELETE {                                      ## Hash DELETE
   my $_id = ${ $_[0] };

   unless ($_MCE->{_wid}) {
      my $_k = $_[1];

      if (my $_r = reftype($_all->{ $_id }->{ $_k })) {
         if ($_r eq 'HASH') {
            my %_buf = %{ $_all->{ $_id }->{ $_k } };
            untie %{ $_all->{ $_id }->{ $_k } };
            delete $_all->{ $_id }->{ $_k };
            return \%_buf;
         } elsif ($_r eq 'ARRAY') {
            my @_buf = @{ $_all->{ $_id }->{ $_k } };
            untie @{ $_all->{ $_id }->{ $_k } };
            delete $_all->{ $_id }->{ $_k };
            return \@_buf;
         } elsif ($_r eq 'SCALAR' || $_r eq 'REF') {
            my $_buf = ${ $_all->{ $_id }->{ $_k } };
            untie ${ $_all->{ $_id }->{ $_k } };
            delete $_all->{ $_id }->{ $_k };
            return \$_buf;
         }
      }

      delete $_all->{ $_id }->{ $_k };
   }
   else {
      $_wa = (!defined wantarray) ? WA_UNDEF : WA_SCALAR;
      local $\ = undef if (defined $\);

      $_dat_ex->();
      print {$_DAT_W_SOCK} SHR_H_DEL . $LF . $_chn . $LF;
      print {$_DAU_W_SOCK} $_id . $LF . $_wa . $LF . length($_[1]).$LF . $_[1];

      unless ($_wa) {
         $_dat_un->();
      } else {
         $_do_ret->();
      }
   }
}

sub CLEAR {                                       ## Hash CLEAR
   my $_id = ${ $_[0] };

   unless ($_MCE->{_wid}) {
      for my $_k (keys %{ $_all->{ $_id } }) {
         if (my $_r = reftype($_all->{ $_id }->{ $_k })) {
            MCE::Shared::Hash::_untie2($_r, $_id, $_k);
         }
      }
      %{ $_all->{ $_id } } = ();
   }
   else {
      local $\ = undef if (defined $\);

      $_dat_ex->();
      print {$_DAT_W_SOCK} SHR_H_CLR . $LF . $_chn . $LF;
      print {$_DAU_W_SOCK} $_id . $LF;
      $_dat_un->();

      return;
   }
}

sub SCALAR {                                      ## Hash SCALAR
   my $_id = ${ $_[0] };

   unless ($_MCE->{_wid}) {
      scalar %{ $_all->{ $_id } };
   }
   else {
      local $\ = undef if (defined $\);
      local $/ = $LF if (!$/ || $/ ne $LF);

      $_dat_ex->();
      print {$_DAT_W_SOCK} SHR_H_SCA . $LF . $_chn . $LF;
      print {$_DAU_W_SOCK} $_id . $LF;

      chomp($_ret = <$_DAU_W_SOCK>);
      $_dat_un->();

      return $_ret;
   }
}

1;

