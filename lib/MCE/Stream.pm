###############################################################################
## ----------------------------------------------------------------------------
## MCE::Stream - Parallel stream model for chaining multiple maps and greps.
##
###############################################################################

package MCE::Stream;

use strict;
use warnings;

use Scalar::Util qw( looks_like_number );

use MCE::Core;
use MCE::Util;

use MCE::Queue;

our $VERSION = '1.499_002'; $VERSION = eval $VERSION;

###############################################################################
## ----------------------------------------------------------------------------
## Import routine.
##
###############################################################################

our $DEFAULT_MODE = 'map';
our $MAX_WORKERS  = 'auto';
our $CHUNK_SIZE   = 'auto';

my ($_params, @_prev_c, @_prev_m, @_prev_n, @_prev_w, @_queue, @_user_tasks);
my ($_MCE, $_loaded); my $_tag = 'MCE::Stream';

sub import {

   my $_class = shift; return if ($_loaded++);

   ## Process module arguments.
   while (my $_arg = shift) {

      $DEFAULT_MODE = shift and next if ( $_arg =~ /^default_mode$/i );
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

   _croak("$_tag: 'DEFAULT_MODE' is not valid")
      if ($DEFAULT_MODE ne 'grep' && $DEFAULT_MODE ne 'map');

   $MAX_WORKERS = MCE::Util::_parse_max_workers($MAX_WORKERS);
   _validate_number($MAX_WORKERS, 'MAX_WORKERS');

   _validate_number($CHUNK_SIZE, 'CHUNK_SIZE')
      unless ($CHUNK_SIZE eq 'auto');

   ## Import functions.
   no strict 'refs'; no warnings 'redefine';
   my $_package = caller();

   *{ $_package . '::mce_stream_f' } = \&mce_stream_f;
   *{ $_package . '::mce_stream_s' } = \&mce_stream_s;
   *{ $_package . '::mce_stream'   } = \&mce_stream;

   return;
}

END {
   MCE::Stream::finish();
}

###############################################################################
## ----------------------------------------------------------------------------
## Gather callback to ensure chunk order is preserved during gathering.
## Also, the task end callback for when a task completes.
##
###############################################################################

my ($_gather_ref, $_order_id, %_tmp);

sub _preserve_order {

   $_tmp{$_[1]} = $_[0];

   if (defined $_gather_ref) {
      while (1) {
         last unless exists $_tmp{$_order_id};
         push @{ $_gather_ref }, @{ $_tmp{$_order_id} };
         delete $_tmp{$_order_id++};
      }
   }
   else {
      $_order_id++;
   }

   return;
}

sub _task_end {

   my ($_mce, $_task_id, $_task_name) = @_;

   if (defined $_mce->{user_tasks}->[$_task_id + 1]) {
      my $N_workers = $_mce->{user_tasks}->[$_task_id + 1]->{max_workers};
      my $_queue_id = @_queue - $_task_id - 1;

      $_queue[$_queue_id]->enqueue((undef) x $N_workers);
   }

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

   MCE::Stream::finish(); $_params = shift;

   return;
}

sub finish () {

   if (defined $_MCE) {
      MCE::_save_state; $_MCE->shutdown(); MCE::_restore_state;
   }

   $_gather_ref = $_order_id = undef; undef %_tmp; @_user_tasks = ();
   @_prev_w = (); @_prev_n = (); @_prev_m = (); @_prev_c = ();

   $_->DESTROY() foreach (@_queue); @_queue = ();

   return;
}

###############################################################################
## ----------------------------------------------------------------------------
## Parallel stream with MCE -- file.
##
###############################################################################

sub mce_stream_f (@) {

   my ($_file, $_pos); my $_start_pos = (ref $_[0] eq 'HASH') ? 2 : 1;

   for ($_start_pos .. @_ - 1) {
      my $_ref = ref $_[$_];
      if ($_ref eq "" || $_ref eq 'GLOB' || $_ref eq 'SCALAR') {
         $_file = $_[$_]; $_pos = $_;
         last;
      }
   }

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

   if (defined $_pos) {
      pop @_ for ($_pos .. @_ - 1);
   }

   return mce_stream(@_);
}

###############################################################################
## ----------------------------------------------------------------------------
## Parallel stream with MCE -- sequence.
##
###############################################################################

sub mce_stream_s (@) {

   my ($_begin, $_end, $_pos); my $_start_pos = (ref $_[0] eq 'HASH') ? 2 : 1;

   delete $_params->{sequence}
      if (exists $_params->{sequence});

   for ($_start_pos .. @_ - 1) {
      my $_ref = ref $_[$_];

      if ($_ref eq "" || $_ref eq 'ARRAY' || $_ref eq 'HASH') {
         $_pos = $_;

         if (ref $_[$_pos] eq "") {
            $_begin = $_[$_pos]; $_end = $_[$_pos + 1];
            $_params->{sequence} = [
               $_[$_pos], $_[$_pos + 1], $_[$_pos + 2], $_[$_pos + 3]
            ];
         }
         elsif (ref $_[$_pos] eq 'HASH') {
            $_begin = $_[$_pos]->{begin}; $_end = $_[$_pos]->{end};
            $_params->{sequence} = $_[0];
         }
         elsif (ref $_[$_pos] eq 'ARRAY') {
            $_begin = $_[$_pos]->[0]; $_end = $_[$_pos]->[1];
            $_params->{sequence} = $_[0];
         }

         last;
      }
   }

   if (defined $_params) {
      delete $_params->{input_data} if (exists $_params->{input_data});
      delete $_params->{_file}      if (exists $_params->{_file});
   }
   else {
      $_params = {};
   }

   _croak("$_tag: 'sequence' is not specified or valid")
      unless (exists $_params->{sequence});

   _croak("$_tag: 'begin' is not specified for sequence")
      unless (defined $_begin);

   _croak("$_tag: 'end' is not specified for sequence")
      unless (defined $_end);

   if (defined $_pos) {
      pop @_ for ($_pos .. @_ - 1);
   }

   return mce_stream(@_);
}

###############################################################################
## ----------------------------------------------------------------------------
## Parallel stream with MCE.
##
###############################################################################

sub mce_stream (@) {

   if (MCE->wid) {
      @_ = (); _croak(
         "$_tag: function cannot be called by the worker process"
      );
   }

   if (ref $_[0] eq 'HASH' && !exists $_[0]->{code}) {
      $_params = {} unless defined $_params;
      $_params->{$_} = $_[0]->{$_} foreach (keys %{ $_[0] });

      shift;
   }

   my $_aref = shift if (ref $_[0] eq 'ARRAY');

   $_order_id = 1; undef %_tmp;

   if (defined $_aref) {
      $_gather_ref = $_aref; @$_aref = ();
   }

   ## -------------------------------------------------------------------------

   my (@_code, @_mode, @_name, @_wrks); my $_init_mce = 0; my $_pos = 0;

   while (ref $_[0] eq 'CODE' || ref $_[0] eq 'HASH') {
      if (ref $_[0] eq 'CODE') {
         push @_code, $_[0];
         push @_mode, $DEFAULT_MODE;
      }
      else {
         push @_code, exists $_[0]->{code} ? $_[0]->{code} : undef;
         push @_mode, exists $_[0]->{mode} ? $_[0]->{mode} : $DEFAULT_MODE;

         unless (ref $_code[-1] eq 'CODE') {
            @_ = (); _croak("$_tag: 'code' is not valid");
         }
         if ($_mode[-1] ne 'grep' && $_mode[-1] ne 'map') {
            @_ = (); _croak("$_tag: 'mode' is not valid");
         }
      }

      push @_name, (defined $_params && ref $_params->{task_name} eq 'ARRAY')
         ? $_params->{task_name}->[$_pos] : undef;
      push @_wrks, (defined $_params && ref $_params->{max_workers} eq 'ARRAY')
         ? $_params->{max_workers}->[$_pos] : undef;

      $_init_mce = 1
         if (!defined $_prev_c[$_pos] || $_prev_c[$_pos] != $_code[$_pos]);
      $_init_mce = 1
         if (!defined $_prev_m[$_pos] || $_prev_m[$_pos] ne $_mode[$_pos]);

      {
         no warnings;
         $_init_mce = 1 if ($_prev_n[$_pos] ne $_name[$_pos]);
         $_init_mce = 1 if ($_prev_w[$_pos] ne $_wrks[$_pos]);
      }

      $_prev_c[$_pos] = $_code[$_pos];
      $_prev_m[$_pos] = $_mode[$_pos];
      $_prev_n[$_pos] = $_name[$_pos];
      $_prev_w[$_pos] = $_wrks[$_pos];

      shift; $_pos++;
   }

   if (defined $_prev_c[$_pos]) {
      pop @_prev_c for ($_pos .. @_prev_c - 1);
      pop @_prev_m for ($_pos .. @_prev_m - 1);
      pop @_prev_n for ($_pos .. @_prev_n - 1);
      pop @_prev_w for ($_pos .. @_prev_w - 1);

      $_init_mce = 1;
   }

   return unless (scalar @_code);

   ## -------------------------------------------------------------------------

   my $_input_data; my $_max_workers = $MAX_WORKERS; my $_r = ref $_[0];

   if ($_r eq 'ARRAY' || $_r eq 'GLOB' || $_r eq 'SCALAR') {
      $_input_data = shift;
   }

   if (defined $_params) { my $_p = $_params;
      $_max_workers = MCE::Util::_parse_max_workers($_p->{max_workers})
         if (exists $_p->{max_workers} && ref $_p->{max_workers} ne 'ARRAY');

      delete $_p->{user_func}  if (exists $_p->{user_func});
      delete $_p->{user_tasks} if (exists $_p->{user_tasks});
      delete $_p->{task_end}   if (exists $_p->{task_end});
      delete $_p->{gather}     if (exists $_p->{gather});
   }
   $_max_workers = int($_max_workers / @_code + 0.5) + 1
      if (@_code > 1);

   my $_chunk_size = MCE::Util::_parse_chunk_size(
      $CHUNK_SIZE, $_max_workers, $_params, $_input_data, scalar @_
   );

   if (!defined $_params || !exists $_params->{_file}) {
      $_chunk_size = int($_chunk_size / @_code + 0.5) + 1
         if (@_code > 1);
   }

   if (defined $_params) {
      $_input_data = $_params->{input_data} if (exists $_params->{input_data});
      $_input_data = $_params->{_file} if (exists $_params->{_file});
   }

   ## -------------------------------------------------------------------------

   MCE::_save_state;

   if ($_init_mce) {
      $_MCE->shutdown() if (defined $_MCE);

      pop( @_queue )->DESTROY for (@_code .. @_queue);
      push @_queue, MCE::Queue->new for (@_queue .. @_code - 2);

      _gen_user_tasks(\@_queue, \@_code, \@_mode, \@_name, \@_wrks);

      $_MCE = MCE->new(
         use_threads => 0, max_workers => $_max_workers, task_name => $_tag,
         user_tasks => \@_user_tasks, task_end => \&_task_end
      );

      if (defined $_params) {
         my $_p = $_params; foreach (keys %{ $_p }) {
            next if ($_ eq 'max_workers' && ref $_p->{max_workers} eq 'ARRAY');
            next if ($_ eq 'task_name' && ref $_p->{task_name} eq 'ARRAY');
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

   if (defined $_params) {
      delete $_params->{input_data}; delete $_params->{_file};
      delete $_params->{sequence};
   }

   MCE::_restore_state;

   return map { @{ $_ } } delete @_tmp{ 1 .. $_order_id - 1 }
      unless (defined $_aref);

   $_gather_ref = undef;

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

sub _gen_user_tasks {

   my ($_queue_ref, $_code_ref, $_mode_ref, $_name_ref, $_wrks_ref) = @_;

   @_user_tasks = ();

   ## For the code block farthest to the right.

   push @_user_tasks, {
      task_name => $_name_ref->[-1],
      max_workers => $_wrks_ref->[-1],

      gather => (@{ $_code_ref } > 1)
         ? $_queue_ref->[-1] : \&_preserve_order,

      user_func => sub {
         my ($_mce, $_chunk_ref, $_chunk_id) = @_;
         my @_a; my $_code = $_code_ref->[-1];

         push @_a, ($_mode_ref->[-1] eq 'map')
            ?  map { &$_code } @{ $_chunk_ref }
            : grep { &$_code } @{ $_chunk_ref };

         MCE->gather( (@{ $_code_ref } > 1)
            ? MCE->freeze([ \@_a, $_chunk_id ])
            : (\@_a, $_chunk_id)
         );
      }
   };

   ## For in-between code blocks (processed from right to left).

   for (my $_i = @{ $_code_ref } - 2; $_i > 0; $_i--) {
      my $_pos = $_i;

      push @_user_tasks, {
         task_name => $_name_ref->[$_pos],
         max_workers => $_wrks_ref->[$_pos],
         gather => $_queue_ref->[$_pos - 1],

         user_func => sub {
            my $_q = $_queue_ref->[$_pos];

            while (1) {
               my $_chunk = $_q->dequeue;
               last unless (defined $_chunk);

               my @_a; my $_code = $_code_ref->[$_pos];
               $_chunk = MCE->thaw($_chunk);

               push @_a, ($_mode_ref->[$_pos] eq 'map')
                  ?  map { &$_code } @{ $_chunk->[0] }
                  : grep { &$_code } @{ $_chunk->[0] };

               MCE->gather(MCE->freeze([ \@_a, $_chunk->[1] ]));
            }

            return;
         }
      };
   }

   ## For the left-most code block.

   if (@{ $_code_ref } > 1) {

      push @_user_tasks, {
         task_name => $_name_ref->[0],
         max_workers => $_wrks_ref->[0],
         gather => \&_preserve_order,

         user_func => sub {
            my $_q = $_queue_ref->[0];

            while (1) {
               my $_chunk = $_q->dequeue;
               last unless (defined $_chunk);

               my @_a; my $_code = $_code_ref->[0];
               $_chunk = MCE->thaw($_chunk);

               push @_a, ($_mode_ref->[0] eq 'map')
                  ?  map { &$_code } @{ $_chunk->[0] }
                  : grep { &$_code } @{ $_chunk->[0] };

               MCE->gather(\@_a, $_chunk->[1]);
            }

            return;
         }
      };
   }

   return;
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

MCE::Stream - Parallel stream model for chaining multiple maps and greps

=head1 VERSION

This document describes MCE::Stream version 1.499_002

=head1 SYNOPSIS

   use MCE::Stream;

   my (@a, $b);

   @a = mce_stream sub { $_ * 3 }, sub { $_ * 2 }, 1..10000;
   mce_stream \@b, sub { $_ * 3 }, sub { $_ * 2 }, 1..10000;

   ## Native Perl
   my @s = mce_map { $_ * $_ } mce_grep { $_ % 5 == 0 } 1..10000;

   ## Multiple maps and greps running in parallel (right-to-left)
   mce_stream \@s,
      { mode => 'map',  code => sub { $_ * $_ } },
      { mode => 'grep', code => sub { $_ % 5 == 0 } }, 1..10000;

=head1 DESCRIPTION

TODO ...

=head1 API

=over

=item mce_stream

   ## mce_stream is imported into the calling script.

   my @a = mce_stream sub { ... }, sub { ... }, 1..100;

=item mce_stream_f

TODO ...

=item mce_stream_s

TODO ...

=item init

   MCE::Stream::init {

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

   MCE::Stream::finish();   ## This is called automatically.

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

