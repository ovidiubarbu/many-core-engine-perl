###############################################################################
## ----------------------------------------------------------------------------
## MCE::Map
## -- Provides a parallel map implementation using Many-Core Engine.
##
###############################################################################

package MCE::Map;

use strict;
use warnings;

use Scalar::Util qw( looks_like_number );

use MCE 1.499;
use MCE::Util;

our $VERSION = '1.499_001'; $VERSION = eval $VERSION;

###############################################################################
## ----------------------------------------------------------------------------
## Import routine.
##
###############################################################################

our $MAX_WORKERS = 'auto';
our $CHUNK_SIZE  = 'auto';

my ($_MCE, $_MCE_P, $_loaded); my ($_params, $_prev_c);
my $_p_funcs = {}; my $_tag = 'MCE::Map';

sub import {

   my $_class = shift; return if ($_loaded++);

   ## Process module arguments.
   while (my $_arg = shift) {

      $MAX_WORKERS  = shift and next if ( $_arg =~ /^max_workers$/i );
      $CHUNK_SIZE   = shift and next if ( $_arg =~ /^chunk_size$/i );
      $MCE::TMP_DIR = shift and next if ( $_arg =~ /^tmp_dir$/i );
      $MCE::FREEZE  = shift and next if ( $_arg =~ /^freeze$/i );
      $MCE::THAW    = shift and next if ( $_arg =~ /^thaw$/i );

      if ( $_arg =~ /^sereal$/i ) {
         if (shift) {
            local $@; eval 'use Sereal qw(encode_sereal decode_sereal)';
            unless ($@) {
               $MCE::FREEZE = \&encode_sereal;
               $MCE::THAW   = \&decode_sereal;
            }
         }
         next;
      }

      _croak("$_tag::import: '$_arg' is not a valid module argument");
   }

   $MAX_WORKERS = MCE::Util::_parse_max_workers($MAX_WORKERS);
   _validate_number($MAX_WORKERS, 'MAX_WORKERS');

   _validate_number($CHUNK_SIZE, 'CHUNK_SIZE')
      unless ($CHUNK_SIZE eq 'auto');

   ## Import functions.
   no strict 'refs'; no warnings 'redefine';
   my $_package = caller();

   *{ $_package . '::mce_mapp' } = \&mce_mapp;
   *{ $_package . '::mce_map'  } = \&mce_map;

   return;
}

END {
   MCE::Map::finish();
}

###############################################################################
## ----------------------------------------------------------------------------
## Gather callback for storing by chunk_id => chunk_ref into a hash.
##
###############################################################################

my ($_total_chunks, %_tmp);

sub _gather {

   $_tmp{$_[1]} = $_[0];
   $_total_chunks++;

   return;
}

###############################################################################
## ----------------------------------------------------------------------------
## Init and finish routines.
##
###############################################################################

sub initp (@) {

   if (MCE->wid) {
      @_ = (); _croak(
         "$_tag: function cannot be called by the worker process"
      );
   }

   _croak("$_tag: 'argument' is not a HASH reference")
      unless (ref $_[0] eq 'HASH');

   my $_hash_ref = shift;

   MCE::_save_state;

   $_MCE_P->shutdown() if (defined $_MCE_P);
   $_p_funcs = $_hash_ref;
   $_MCE_P->spawn();

   MCE::_restore_state;

   return;
}

sub init (@) {

   if (MCE->wid) {
      @_ = (); _croak(
         "$_tag: function cannot be called by the worker process"
      );
   }

   _croak("$_tag: 'argument' is not a HASH reference")
      unless (ref $_[0] eq 'HASH');

   MCE::Map::finish();
   $_params = shift;

   return;
}

sub finish () {

   if (defined $_MCE_P) {
      MCE::_save_state; $_MCE_P->shutdown(); MCE::_restore_state;
   }
   if (defined $_MCE) {
      MCE::_save_state; $_MCE->shutdown(); MCE::_restore_state;
   }

   $_prev_c = $_total_chunks = undef; undef %_tmp;

   return;
}

###############################################################################
## ----------------------------------------------------------------------------
## Parallel map with MCE.
##
###############################################################################

sub mce_map (&@) {

   if (MCE->wid) {
      @_ = (); _croak(
         "$_tag: function cannot be called by the worker process"
      );
   }

   my $_code = shift;

   if (ref $_[0] eq 'HASH') {
      $_params = {} unless defined $_params;
      $_params->{$_} = $_[0]->{$_} foreach (keys %{ $_[0] });

      shift;
   }

   $_total_chunks = 0; undef %_tmp;

   ## -------------------------------------------------------------------------

   my ($_chunk_size, $_max_workers) = ($CHUNK_SIZE, $MAX_WORKERS);
   my $_r = ref $_[0];

   my $_input_data = shift
      if ($_r eq 'ARRAY' || $_r eq 'GLOB' || $_r eq 'SCALAR');

   if (defined $_params) {
      my $_p = $_params;

      $_chunk_size = $_p->{chunk_size} if (exists $_p->{chunk_size});
      delete $_p->{user_func} if (exists $_p->{user_func});
      delete $_p->{gather} if (exists $_p->{gather});

      $_max_workers = MCE::Util::_parse_max_workers($_p->{max_workers})
         if (exists $_p->{max_workers} && ref $_p->{max_workers} ne 'ARRAY');

      $_input_data = $_p->{input_data}
         if (!defined $_input_data && exists $_p->{input_data});
   }

   if ($_chunk_size eq 'auto') {
      my $_size = (defined $_input_data && ref $_input_data eq 'ARRAY')
         ? scalar @{ $_input_data } : scalar @_;

      $_chunk_size = int($_size / $_max_workers + 0.5);
      $_chunk_size = 8000 if $_chunk_size > 8000;
      $_chunk_size = 1 if $_chunk_size < 1;

      $_chunk_size = 800
         if (defined $_params && exists $_params->{sequence});
   }

   ## -------------------------------------------------------------------------

   MCE::_save_state;

   if (!defined $_prev_c || $_prev_c != $_code) {
      $_MCE->shutdown() if (defined $_MCE);
      $_prev_c = $_code;

      $_MCE = MCE->new(
         use_threads => 0, max_workers => $_max_workers, task_name => $_tag,
         gather => \&_gather, user_func => sub {

            my @_a; my ($_mce, $_chunk_ref, $_chunk_id) = @_;

          # push @_a, &$_code foreach (@$_chunk_ref);
            push @_a, map { &$_code } @$_chunk_ref;

            MCE->gather(\@_a, $_chunk_id);
         }
      );

      if (defined $_params) {
         $_MCE->{$_} = $_params->{$_} foreach (keys %{ $_params });
      }
   }

   if (defined $_input_data) {
      @_ = (); $_MCE->process({ chunk_size => $_chunk_size }, $_input_data);
   }
   elsif (scalar @_) {
      $_MCE->process({ chunk_size => $_chunk_size }, \@_);
   }
   else {
      $_MCE->run({ chunk_size => $_chunk_size }, 0)
         if (defined $_params && exists $_params->{sequence});
   }

   MCE::_restore_state;

   return map { @{ $_ } } delete @_tmp{ 1 .. $_total_chunks };
}

###############################################################################
## ----------------------------------------------------------------------------
## Parallel map with MCE -- workers persist after running.
##
###############################################################################

sub mce_mapp (@) {

   if (MCE->wid) {
      @_ = (); _croak(
         "$_tag: function cannot be called by the worker process"
      );
   }

   my $_func = shift;

   if (!defined $_func || !exists $_p_funcs->{$_func}) {
      @_ = (); _croak("$_tag: '$_func' function does not exist in hash");
   }

   if (ref $_[0] eq 'HASH') {
      $_params = {} unless defined $_params;
      $_params->{$_} = $_[0]->{$_} foreach (keys %{ $_[0] });

      shift;
   }

   $_total_chunks = 0; undef %_tmp;

   ## -------------------------------------------------------------------------

   my ($_chunk_size, $_max_workers) = ($CHUNK_SIZE, $MAX_WORKERS);
   my $_r = ref $_[0];

   my $_input_data = shift
      if ($_r eq 'ARRAY' || $_r eq 'GLOB' || $_r eq 'SCALAR');

   if (defined $_params) {
      my $_p = $_params;

      $_chunk_size = $_p->{chunk_size} if (exists $_p->{chunk_size});
      delete $_p->{user_func} if (exists $_p->{user_func});
      delete $_p->{gather} if (exists $_p->{gather});

      $_max_workers = MCE::Util::_parse_max_workers($_p->{max_workers})
         if (exists $_p->{max_workers} && ref $_p->{max_workers} ne 'ARRAY');

      $_input_data = $_p->{input_data}
         if (!defined $_input_data && exists $_p->{input_data});
   }

   if ($_chunk_size eq 'auto') {
      my $_size = (defined $_input_data && ref $_input_data eq 'ARRAY')
         ? scalar @{ $_input_data } : scalar @_;

      $_chunk_size = int($_size / $_max_workers + 0.5);
      $_chunk_size = 8000 if $_chunk_size > 8000;
      $_chunk_size = 1 if $_chunk_size < 1;

      $_chunk_size = 800
         if (defined $_params && exists $_params->{sequence});
   }

   ## -------------------------------------------------------------------------

   MCE::_save_state;

   if (!defined $_MCE_P) {
      $_MCE_P = MCE->new(
         use_threads => 0, max_workers => $_max_workers, task_name => $_tag,
         gather => \&_gather, user_func => sub {

            my @_a; my ($_mce, $_chunk_ref, $_chunk_id) = @_;
            my $_code = $_p_funcs->{ $_mce->user_args };

          # push @_a, &$_code foreach (@$_chunk_ref);
            push @_a, map { &$_code } @$_chunk_ref;

            MCE->gather(\@_a, $_chunk_id);
         }
      );

      if (defined $_params) {
         $_MCE_P->{$_} = $_params->{$_} foreach (keys %{ $_params });
      }
   }

   my $_options = { chunk_size => $_chunk_size, user_args => $_func };

   if (defined $_input_data) {
      @_ = (); $_MCE_P->process($_options, $_input_data);
   }
   elsif (scalar @_) {
      $_MCE_P->process($_options, \@_);
   }
   else {
      $_MCE_P->run($_options, 0)
         if (defined $_params && exists $_params->{sequence});
   }

   MCE::_restore_state;

   return map { @{ $_ } } delete @_tmp{ 1 .. $_total_chunks };
}

###############################################################################
## ----------------------------------------------------------------------------
## Private methods.
##
###############################################################################

sub _croak {

   goto &MCE::_croak;
}

sub _validate_number {

   my $_n = $_[0]; my $_key = $_[1];

   _croak("$_tag: '$_key' is not valid")
      if (!looks_like_number($_n) || int($_n) != $_n || $_n < 1);

   return;
}

1;

__END__

=head1 NAME

MCE::Map - Provides a parallel map implementation using Many-Core Engine.

=head1 VERSION

This document describes MCE::Map version 1.499_001

=head1 SYNOPSIS

   use MCE::Map;
   use MCE::Grep;

   my @s = mce_map { $_ * $_ } mce_grep { $_ % 5 == 0 } 1..10000;

=head1 DESCRIPTION

TODO ...

=head1 API

=over

=item mce_map

   ## mce_map is imported into the calling script.

   my @a = mce_map { ... } 1..100;

=item init

   MCE::Map::init {

      ## This form is available for configuring MCE options
      ## before running.

      user_begin => sub {
         print "## ", MCE->wid, "\n";
      }
      user_end => sub {
         ...
      }
   };

=item finish

   MCE::Map::finish();   ## This is called automatically.

=back

=head1 PERSISTENT WORKERS

=over

=item initp

TODO ...

=item mce_mapp

TODO ...

=back

=head1 SEE ALSO

L<MCE::Flow>, L<MCE::Grep>, L<MCE::Loop>, L<MCE::Stream>,
L<MCE::Queue>, L<MCE>

=head1 AUTHOR

Mario E. Roy, S<E<lt>marioeroy AT gmail DOT comE<gt>>

=head1 LICENSE

This program is free software; you can redistribute it and/or modify it
under the terms of either: the GNU General Public License as published
by the Free Software Foundation; or the Artistic License.

See L<http://dev.perl.org/licenses/> for more information.

=cut

