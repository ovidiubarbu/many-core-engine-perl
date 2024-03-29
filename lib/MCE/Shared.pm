###############################################################################
## ----------------------------------------------------------------------------
## MCE::Shared - MCE extension for sharing data structures between workers.
##
###############################################################################

package MCE::Shared;

use strict;
use warnings;

## no critic (Subroutines::ProhibitExplicitReturnUndef)
## no critic (Subroutines::ProhibitSubroutinePrototypes)
## no critic (TestingAndDebugging::ProhibitNoStrict)

use MCE::Shared::Hash;
use MCE::Shared::Array;
use MCE::Shared::Scalar;

use Scalar::Util qw( refaddr reftype );
use bytes;

our @CARP_NOT = qw(
   MCE::Shared::Scalar::Tie
   MCE::Shared::Array::Tie
   MCE::Shared::Hash::Tie
);

our $VERSION = '1.699';

## Method of complaint about references we cannot clone or untie.
## undef (croak), 1 (carp), 0 (silent)

$MCE::Shared::clone_warn = undef;
$MCE::Shared::untie_warn = undef;

###############################################################################
## ----------------------------------------------------------------------------
## Import and new routines.
##
###############################################################################

my $_loaded;

sub import {

   my $_class = shift; return if ($_loaded++);

   unless (defined $MCE::VERSION) {
      $\ = undef; require Carp;
      Carp::croak(
         "MCE::Shared requires MCE. Please see the MCE::Shared documentation\n".
         "for more information.\n\n"
      );
   }

   no strict 'refs'; no warnings 'redefine';
   *{ caller().'::mce_share' } = \&share;

   return;
}

sub new {

   my ($_class, %_argv) = @_;

   MCE::_croak('Method (new) is not allowed by the worker process')
      if (MCE->wid);

   for my $_p (keys %_argv) {
      Carp::croak("MCE::Shared::new: ($_p) is not a valid constructor argument")
         if ($_p ne 'type' && $_p ne 'locking');
   }

   my $_type = defined $_argv{type} ? $_argv{type} || 'hash' : 'hash';
   my $_shr;  $_type = lc $_type;

   if ($_type eq 'hash') {
      $_shr = MCE::Shared::Hash::_share(\%_argv, {});
   }
   elsif ($_type eq 'array') {
      $_shr = MCE::Shared::Array::_share(\%_argv, []);
   }
   elsif ($_type eq 'scalar') {
      my $_scalar_ref = \do{ my $scalar = undef };
      $_shr = MCE::Shared::Scalar::_share(\%_argv, $_scalar_ref);
   }
   else {
      Carp::croak("MCE::Shared::new: type ($_type) is not valid");
   }

   return $_shr;
}

###############################################################################
## ----------------------------------------------------------------------------
## Share routine.
##
###############################################################################

sub share (\[$@%]@) {

   MCE::_croak('Method (share) is not allowed by the worker process')
      if (MCE->wid);

   my $_ref_type = reftype($_[0]);

   if ($_ref_type eq 'SCALAR' || $_ref_type eq 'REF') {
      Carp::croak('Too many arguments in scalar assignment')
         if (scalar @_ > 2);

      ## Scalar special handling to prevent double tie'ing $_[0] and $_[1].
      if (scalar @_ == 2 && (my $_r = reftype($_[1]))) {
         my $_item; my $_scalar_ref = $_[0];

         if ($_r eq 'SCALAR' || $_r eq 'REF') {
            $_item = MCE::Shared::Scalar::_share({}, $_[1]);
         } elsif ($_r eq 'ARRAY') {
            $_item = MCE::Shared::Array::_share({}, $_[1]);
         } elsif ($_r eq 'HASH') {
            $_item = MCE::Shared::Hash::_share({}, $_[1]);
         } else {
            return _unsupported($_r);
         }

         ${ $_scalar_ref } = $_item;
      }

      ## Scalar normal handling.
      else {
         MCE::Shared::Scalar::_share({}, @_);
      }
   }
   elsif ($_ref_type eq 'ARRAY') {
      MCE::Shared::Array::_share({}, @_);
   }
   elsif ($_ref_type eq 'HASH') {
      Carp::carp('Odd number of elements in hash assignment')
         if (scalar @_ > 1 && (scalar @_ - 1) % 2);

      MCE::Shared::Hash::_share({}, @_);
   }
   else {
      return _unsupported($_ref_type);
   }

   return;
}

###############################################################################
## ----------------------------------------------------------------------------
## Private methods inspired by threads::shared.
##
###############################################################################

sub _copy {

   my ($_cloned, $_item) = @_;

   ## Return the item if not a ref.
   return $_item unless reftype($_item);

   ## Return the cloned ref if already cloned.
   my $_id = refaddr($_item);

   if (exists $_cloned->{ $_id }) {
      return $_cloned->{ $_id };
   }

   ## Make copies of hash, array, and scalar refs and refs of refs.
   my $_copy; my $_ref_type = reftype($_item);

   if ($_ref_type eq 'HASH') {
      $_copy = MCE::Shared::Hash::_share($_cloned, $_item);
   }
   elsif ($_ref_type eq 'ARRAY') {
      $_copy = MCE::Shared::Array::_share($_cloned, $_item);
   }
   elsif ($_ref_type eq 'SCALAR' || $_ref_type eq 'REF') {
      $_copy = MCE::Shared::Scalar::_share($_cloned, $_item);
   }
   else {
      return _unsupported($_ref_type);
   }

   return $_copy;
}

sub _on {

   MCE::_croak('Method (->on) is not allowed by the worker process')
      if (MCE->wid);

   my $_id = ${ (shift) }; my ($_pcb, $_def, $_name, $_code, $_value) = @_;

   @_ = ();

   Carp::croak("MCE::Shared (->on): (name and/or code) are not defined")
      if (!defined $_name || !defined $_code);
   Carp::croak("MCE::Shared (->on): ($_name) is not supported")
      if ($_name ne 'put' && $_name ne 'store');
   Carp::croak("MCE::Shared (->on): (code) is not a CODE ref")
      if (ref $_code ne 'CODE');

   $_def->{ $_id } = (defined $_value) ? $_value : undef;
   $_pcb->{ $_id } = $_code;

   return;
}

sub _unsupported {

   Carp::croak('Unsupported ref type: ', $_[0])
      if (!defined($MCE::Shared::clone_warn));

   Carp::carp('Unsupported ref type: ', $_[0])
      if ($MCE::Shared::clone_warn);

   return undef;
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

This document describes MCE::Shared version 1.699

=head1 SYNOPSIS

   use feature 'say';

   use MCE::Flow;
   use MCE::Shared;

   mce_share my $sca => 'initial value';
   mce_share my @arr => qw(a list of values);
   mce_share my %has => (key1 => 'value', key2 => 'value');

   mce_share my $cnt;    ## or mce_share $cnt; (defined elsewhere)
   mce_share my @foo;
   mce_share my %bar;

   my $m1 = MCE::Mutex->new;

   mce_flow {
      max_workers => 4
   },
   sub {
      my ($mce) = @_;
      my ($pid, $wid) = (MCE->pid, MCE->wid);

      ## Locking is required when many workers update the same element.
      ## This requires 2 trips to the manager process (fetch and store).

      $m1->synchronize( sub {
         $cnt += 1;
      });

      ## Locking is not necessary when updating unique elements.

      $foo[ $wid - 1 ] = $pid;
      $bar{ $pid }     = $wid;

      return;
   };

   say "scalar : $cnt";
   say " array : $_" for (@foo);
   say "  hash : $_ => $bar{$_}" for (sort keys %bar);

   -- Output

   scalar : 8
    array : 18911
    array : 18912
    array : 18913
    array : 18914
     hash : 18911 => 1
     hash : 18912 => 2
     hash : 18913 => 3
     hash : 18914 => 4

=head1 DESCRIPTION

This module provides data sharing for MCE supporting threads and processes.

=head1 ACKNOWNLEDGEMENTS

Not having to backslash variables was inspired by Leon Timmermans's
L<Const::Fast|Const::Fast> module.

=head1 INDEX

L<MCE|MCE>

=head1 AUTHOR

Mario E. Roy, S<E<lt>marioeroy AT gmail DOT comE<gt>>

=cut

