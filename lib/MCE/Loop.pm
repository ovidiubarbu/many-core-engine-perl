###############################################################################
## ----------------------------------------------------------------------------
## MCE::Loop - Parallel loop model for building creative loops.
##
###############################################################################

package MCE::Loop;

use strict;
use warnings;

use Scalar::Util qw( looks_like_number );

use MCE;
use MCE::Util;

our $VERSION = '1.499_003'; $VERSION = eval $VERSION;

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
         if (shift eq '1') {
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

   *{ $_package . '::mce_loop_f' } = \&mce_loop_f;
   *{ $_package . '::mce_loop_s' } = \&mce_loop_s;
   *{ $_package . '::mce_loop'   } = \&mce_loop;

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

sub mce_loop_f (&@) {

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

   return mce_loop($_code);
}

###############################################################################
## ----------------------------------------------------------------------------
## Parallel loop with MCE -- sequence.
##
###############################################################################

sub mce_loop_s (&@) {

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

   my $_code = shift;

   if (MCE->wid) {
      @_ = (); _croak(
         "$_tag: function cannot be called by the worker process"
      );
   }

   my $_input_data; my $_max_workers = $MAX_WORKERS; my $_r = ref $_[0];

   if ($_r eq 'ARRAY' || $_r eq 'GLOB' || $_r eq 'SCALAR') {
      $_input_data = shift;
   }

   if (defined $_params) { my $_p = $_params;
      $_max_workers = MCE::Util::_parse_max_workers($_p->{max_workers})
         if (exists $_p->{max_workers});

      delete $_p->{user_func}  if (exists $_p->{user_func});
      delete $_p->{user_tasks} if (exists $_p->{user_tasks});
   }

   my $_chunk_size = MCE::Util::_parse_chunk_size(
      $CHUNK_SIZE, $_max_workers, $_params, $_input_data, scalar @_
   );

   if (defined $_params) {
      $_input_data = $_params->{input_data} if (exists $_params->{input_data});
      $_input_data = $_params->{_file} if (exists $_params->{_file});
   }

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
         my $_p = $_params; foreach (keys %{ $_p }) {
            next if ($_ eq 'input_data');
            $_MCE->{$_} = $_p->{$_};
         }
      }
   }

   my @_a; my $_wa = wantarray; $_MCE->{gather} = \@_a if (defined $_wa);

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

   if (defined $_params) {
      delete $_params->{input_data}; delete $_params->{_file};
      delete $_params->{sequence};
   }

   delete $_MCE->{gather} if (defined $_wa);

   MCE::_restore_state;

   return ((defined $_wa) ? @_a : ());
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

This document describes MCE::Loop version 1.499_003

=head1 SYNOPSIS CHUNK_SIZE == 1

All models in MCE default to 'auto' for chunk_size. MCE::Loop is not MCE::Map.
The code block is configured as user_func for MCE. Therefore, a chunk_size > 1
means having to loop through each item in the chunk, similar to using MCE and
creating a function for the user_func option and chunking through the data.

Think of MCE::Loop as a quick way to spun up a MCE instance with user_func set
to your code block. You have the option of disabling/enabling chunking simply
by setting chunk_size as shown below.

   ## Exports mce_loop, mce_loop_f, and mce_loop_s
   use MCE::Loop;

   MCE::Loop::init {
      chunk_size => 1
   };

   ## Array or array_ref
   mce_loop { do_work($_) } 1..10000;
   mce_loop { do_work($_) } [ 1..10000 ];

   ## File_path, glob_ref, or scalar_ref
   mce_loop_f { chomp; do_work($_) } "/path/to/file";
   mce_loop_f { chomp; do_work($_) } $file_handle;
   mce_loop_f { chomp; do_work($_) } \$scalar;

   ## Sequence of numbers (begin, end [, step, format])
   mce_loop_s { do_work($_) } 1, 10000, 5;
   mce_loop_s { do_work($_) } [ 1, 10000, 5 ];

   mce_loop_s { do_work($_) } {
      begin => 1, end => 10000, step => 5, format => undef
   };

=head1 SYNOPSIS CHUNK_SIZE > 1

Use this synopsis when chunk_size is set to 'auto' or greater than 1.

   use MCE::Loop;

   MCE::Loop::init {          ## Chunk_size defaults to 'auto' when
      chunk_size => 'auto'    ## not specified. Therefore, the init
   };                         ## function is not needed.

   ## Syntax is shown for mce_loop for demonstration purposes.
   ## Chunking also applies to mce_loop_f and mce_loop_s.
   mce_loop { do_work($_) for (@{ $_ }) } 1..10000;

   ## Same as above. This resembles "core" MCE code.
   mce_loop {
      my ($mce, $chunk_ref, $chunk_id) = @_;
      for (@{ $chunk_ref }) {
         do_work($_);
      }
   } 1..10000;

=head1 DESCRIPTION

TODO

=head1 OVERRIDING DEFAULTS

The following list 5 options which may be overridden when loading the module.

   use Sereal qw(encode_sereal decode_sereal);

   use MCE::Loop
         max_workers => 4,                    ## Default 'auto'
         chunk_size  => 100,                  ## Default 'auto'
         tmp_dir     => "/path/to/app/tmp",   ## $MCE::Signal::tmp_dir
         freeze      => \&encode_sereal,      ## \&Storable::freeze
         thaw        => \&decode_sereal       ## \&Storable::thaw
   ;

There is a simplier way to enable Sereal with MCE 1.5. The following will
attempt to use Sereal if available, otherwise will default back to using
Storable for serialization.

   use MCE::Loop Sereal => 1;

   MCE::Loop::init {
      chunk_size => 1
   };

   ## Serialization is through Sereal if available.
   my %answer = mce_loop { MCE->gather( $_, sqrt $_ ) } 1..10000;

=head1 CUSTOMIZING MCE

=over 2

=item init

The init function takes a hash of MCE options.

   use MCE::Loop;

   MCE::Loop::init {
      chunk_size => 1, max_workers => 4,

      user_begin => sub {
         print "## ", MCE->wid, " started\n";
      },

      user_end => sub {
         print "## ", MCE->wid, " completed\n";
      }
   };

   my %a = mce_loop { MCE->gather($_, $_ * $_) } 1..100;

   print "\n", "@a{1..100}", "\n";

   -- output

   ## 3 started
   ## 1 started
   ## 2 started
   ## 4 started
   ## 1 completed
   ## 2 completed
   ## 3 completed
   ## 4 completed

   1 4 9 16 25 36 49 64 81 100 121 144 169 196 225 256 289 324 361
   400 441 484 529 576 625 676 729 784 841 900 961 1024 1089 1156
   1225 1296 1369 1444 1521 1600 1681 1764 1849 1936 2025 2116 2209
   2304 2401 2500 2601 2704 2809 2916 3025 3136 3249 3364 3481 3600
   3721 3844 3969 4096 4225 4356 4489 4624 4761 4900 5041 5184 5329
   5476 5625 5776 5929 6084 6241 6400 6561 6724 6889 7056 7225 7396
   7569 7744 7921 8100 8281 8464 8649 8836 9025 9216 9409 9604 9801
   10000

=back

=head1 API USAGE

TODO

=head1 MANUAL SHUTDOWN

=over 2

=item finish

MCE workers remain persistent as much as possible after running. Shutdown
occurs when the script exits. One can manually shutdown MCE by simply calling
finish after running. This resets the MCE instance.

   use MCE::Loop;

   MCE::Loop::init {
      chunk_size => 20, max_workers => 'auto'
   };

   mce_loop { ... } 1..100;

   MCE::Loop::finish;

=back

=head1 INDEX

L<MCE>

=head1 AUTHOR

Mario E. Roy, S<E<lt>marioeroy AT gmail DOT comE<gt>>

=head1 LICENSE

This program is free software; you can redistribute it and/or modify it
under the terms of either: the GNU General Public License as published
by the Free Software Foundation; or the Artistic License.

See L<http://dev.perl.org/licenses/> for more information.

=cut

