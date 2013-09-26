###############################################################################
## ----------------------------------------------------------------------------
## MCE::Map - Parallel map model similar to the native map function.
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

my ($_MCE, $_loaded); my ($_params, $_prev_c); my $_tag = 'MCE::Map';

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

   *{ $_package . '::mce_map_f' } = \&mce_map_f;
   *{ $_package . '::mce_map_s' } = \&mce_map_s;
   *{ $_package . '::mce_map'   } = \&mce_map;

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

sub init (@) {

   if (MCE->wid) {
      @_ = (); _croak(
         "$_tag: function cannot be called by the worker process"
      );
   }

   _croak("$_tag: 'argument' is not a HASH reference")
      unless (ref $_[0] eq 'HASH');

   MCE::Map::finish(); $_params = shift;

   return;
}

sub finish () {

   if (defined $_MCE) {
      MCE::_save_state; $_MCE->shutdown(); MCE::_restore_state;
   }

   $_prev_c = $_total_chunks = undef; undef %_tmp;

   return;
}

###############################################################################
## ----------------------------------------------------------------------------
## Parallel map with MCE -- file.
##
###############################################################################

sub mce_map_f (&@) {

   my $_code = shift; my $_file = shift;

   if (defined $_params) {
      delete $_params->{input_data} if (exists $_params->{input_data});
      delete $_params->{sequence}   if (exists $_params->{sequence});
   }
   else {
      $_params = {};
   }

   if (defined $_file && ref $_file eq "" && $_file ne "") {
      _croak("$_tag: '$_file' does not exist") unless (-e $_file);
      _croak("$_tag: '$_file' is not readable") unless (-r $_file);
      _croak("$_tag: '$_file' is not a plain file") unless (-f $_file);
      $_params->{_file} = $_file;
   }
   elsif (ref $_file eq 'GLOB' || ref $_file eq 'SCALAR') {
      $_params->{_file} = $_file;
   }
   else {
      _croak("$_tag: 'file' is not specified or valid");
   }

   @_ = ();

   return mce_map($_code);
}

###############################################################################
## ----------------------------------------------------------------------------
## Parallel map with MCE -- sequence.
##
###############################################################################

sub mce_map_s (&@) {

   my $_code = shift;

   if (defined $_params) {
      delete $_params->{input_data} if (exists $_params->{input_data});
      delete $_params->{_file}      if (exists $_params->{_file});
   }
   else {
      $_params = {};
   }

   my ($_begin, $_end);

   if (ref $_[0] eq 'HASH') {
      $_begin = $_[0]->{begin}; $_end = $_[0]->{end};
      $_params->{sequence} = $_[0];
   }
   elsif (ref $_[0] eq 'ARRAY') {
      $_begin = $_[0]->[0]; $_end = $_[0]->[1];
      $_params->{sequence} = $_[0];
   }
   elsif (ref $_[0] eq "") {
      $_begin = $_[0]; $_end = $_[1];
      $_params->{sequence} = [ @_ ];
   }
   else {
      _croak("$_tag: 'sequence' is not specified or valid");
   }

   _croak("$_tag: 'begin' is not specified for sequence")
      unless (defined $_begin);

   _croak("$_tag: 'end' is not specified for sequence")
      unless (defined $_end);

   @_ = ();

   return mce_map($_code);
}

###############################################################################
## ----------------------------------------------------------------------------
## Parallel map with MCE.
##
###############################################################################

sub mce_map (&@) {

   $_total_chunks = 0; undef %_tmp;

   if (MCE->wid) {
      @_ = (); _croak(
         "$_tag: function cannot be called by the worker process"
      );
   }

   my $_code = shift; my $_max_workers = $MAX_WORKERS; my $_r = ref $_[0];

   my $_input_data = shift
      if ($_r eq 'ARRAY' || $_r eq 'GLOB' || $_r eq 'SCALAR');

   if (defined $_params) { my $_p = $_params;
      $_max_workers = MCE::Util::_parse_max_workers($_p->{max_workers})
         if (exists $_p->{max_workers});

      delete $_p->{user_func}  if (exists $_p->{user_func});
      delete $_p->{user_tasks} if (exists $_p->{user_tasks});

      delete $_p->{gather}     if (exists $_p->{gather});
   }

   my $_chunk_size = MCE::Util::_parse_chunk_size(
      $CHUNK_SIZE, $_max_workers, $_params, $_input_data, scalar @_
   );

   $_input_data = $_params->{input_data}
      if (defined $_params && exists $_params->{input_data});

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
         my $_p = $_params; foreach (keys %{ $_p }) {
            next if ($_ eq 'input_data');
            $_MCE->{$_} = $_p->{$_};
         }
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

###############################################################################
## ----------------------------------------------------------------------------
## Module usage.
##
###############################################################################

=head1 NAME

MCE::Map - Parallel map model similar to the native map function

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

=item mce_map_f

TODO ...

=item mce_map_s

TODO ...

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

=head1 SEE ALSO

L<MCE>, L<MCE::Flow>, L<MCE::Grep>, L<MCE::Loop>, L<MCE::Queue>,
L<MCE::Signal>, L<MCE::Stream>, L<MCE::Subs>, L<MCE::Util>

=head1 AUTHOR

Mario E. Roy, S<E<lt>marioeroy AT gmail DOT comE<gt>>

=head1 LICENSE

This program is free software; you can redistribute it and/or modify it
under the terms of either: the GNU General Public License as published
by the Free Software Foundation; or the Artistic License.

See L<http://dev.perl.org/licenses/> for more information.

=cut

