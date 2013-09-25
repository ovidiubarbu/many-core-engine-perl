###############################################################################
## ----------------------------------------------------------------------------
## MCE::Loop - Parallel loop model for building creative loops.
##
###############################################################################

package MCE::Loop;

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

my ($_MCE, $_loaded); my ($_params, $_prev_c); my $_tag = 'MCE::Loop';

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

   *{ $_package . '::mce_loopfile' } = \&mce_loopfile;
   *{ $_package . '::mce_loopseq'  } = \&mce_loopseq;
   *{ $_package . '::mce_loop'     } = \&mce_loop;

   return;
}

END {
   MCE::Loop::finish();
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

   MCE::Loop::finish(); $_params = shift;

   return;
}

sub finish () {

   if (defined $_MCE) {
      MCE::_save_state; $_MCE->shutdown(); MCE::_restore_state;
   }

   $_prev_c = undef;

   return;
}

###############################################################################
## ----------------------------------------------------------------------------
## Parallel loop with MCE -- file.
##
###############################################################################

sub mce_loopfile (&@) {

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
      _croak("$_tag: 'file' is not specified or a valid type");
   }

   @_ = ();

   return mce_loop($_code);
}

###############################################################################
## ----------------------------------------------------------------------------
## Parallel loop with MCE -- sequence.
##
###############################################################################

sub mce_loopseq (&@) {

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

   return mce_loop($_code);
}

###############################################################################
## ----------------------------------------------------------------------------
## Parallel loop with MCE.
##
###############################################################################

sub mce_loop (&@) {

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

      delete $_p->{user_func} if (exists $_p->{user_func});
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
         max_workers => $_max_workers, task_name => $_tag,
         user_func => $_code
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

   return;
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

MCE::Loop - Parallel loop model for building creative loops

=head1 VERSION

This document describes MCE::Loop version 1.499_001

=head1 SYNOPSIS

TODO ...

=head1 DESCRIPTION

TODO ...

=head1 API

=over

=item mce_loop

   ## mce_loop is imported into the calling script.

   mce_loop { ... } 1..100;

=item mce_loopfile

TODO ...

=item mce_loopseq

TODO ...

=item init

   MCE::Loop::init {

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

   MCE::Loop::finish();   ## This is called automatically.

=back

=head1 SEE ALSO

L<MCE>, L<MCE::Flow>, L<MCE::Grep>, L<MCE::Map>, L<MCE::Queue>,
L<MCE::Signal>, L<MCE::Stream>, L<MCE::Subs>, L<MCE::Util>

=head1 AUTHOR

Mario E. Roy, S<E<lt>marioeroy AT gmail DOT comE<gt>>

=head1 LICENSE

This program is free software; you can redistribute it and/or modify it
under the terms of either: the GNU General Public License as published
by the Free Software Foundation; or the Artistic License.

See L<http://dev.perl.org/licenses/> for more information.

=cut
