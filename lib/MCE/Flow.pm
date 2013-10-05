###############################################################################
## ----------------------------------------------------------------------------
## MCE::Flow - Parallel flow model for building creative applications.
##
###############################################################################

package MCE::Flow;

use strict;
use warnings;

use Scalar::Util qw( looks_like_number );

use MCE;
use MCE::Util;

our $VERSION = '1.499_005'; $VERSION = eval $VERSION;

###############################################################################
## ----------------------------------------------------------------------------
## Import routine.
##
###############################################################################

our $MAX_WORKERS  = 'auto';
our $CHUNK_SIZE   = 'auto';

my ($_params, @_prev_c, @_prev_n, @_prev_w, @_user_tasks);
my ($_MCE, $_loaded); my $_tag = 'MCE::Flow';

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

   *{ $_package . '::mce_flow_f' } = \&mce_flow_f;
   *{ $_package . '::mce_flow_s' } = \&mce_flow_s;
   *{ $_package . '::mce_flow'   } = \&mce_flow;

   return;
}

END {
   MCE::Flow::finish();
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

   MCE::Flow::finish(); $_params = shift;

   return;
}

sub finish () {

   if (defined $_MCE) {
      MCE::_save_state; $_MCE->shutdown(); MCE::_restore_state;
   }

   @_user_tasks = (); @_prev_w = (); @_prev_n = (); @_prev_c = ();

   return;
}

###############################################################################
## ----------------------------------------------------------------------------
## Parallel flow with MCE -- file.
##
###############################################################################

sub mce_flow_f (@) {

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

   return mce_flow(@_);
}

###############################################################################
## ----------------------------------------------------------------------------
## Parallel flow with MCE -- sequence.
##
###############################################################################

sub mce_flow_s (@) {

   my ($_begin, $_end, $_pos); my $_start_pos = (ref $_[0] eq 'HASH') ? 2 : 1;

   delete $_params->{sequence}
      if (exists $_params->{sequence});

   for ($_start_pos .. @_ - 1) {
      my $_ref = ref $_[$_];

      if ($_ref eq "" || $_ref eq 'HASH' || $_ref eq 'ARRAY') {
         $_pos = $_;

         if ($_ref eq "") {
            $_begin = $_[$_pos]; $_end = $_[$_pos + 1];
            $_params->{sequence} = [
               $_[$_pos], $_[$_pos + 1], $_[$_pos + 2], $_[$_pos + 3]
            ];
         }
         elsif ($_ref eq 'HASH') {
            $_begin = $_[$_pos]->{begin}; $_end = $_[$_pos]->{end};
            $_params->{sequence} = $_[0];
         }
         elsif ($_ref eq 'ARRAY') {
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

   return mce_flow(@_);
}

###############################################################################
## ----------------------------------------------------------------------------
## Parallel flow with MCE.
##
###############################################################################

sub mce_flow (@) {

   if (MCE->wid) {
      @_ = (); _croak(
         "$_tag: function cannot be called by the worker process"
      );
   }

   if (ref $_[0] eq 'HASH') {
      $_params = {} unless defined $_params;
      $_params->{$_} = $_[0]->{$_} foreach (keys %{ $_[0] });

      shift;
   }

   ## -------------------------------------------------------------------------

   my (@_code, @_name, @_wrks); my $_init_mce = 0; my $_pos = 0;

   while (ref $_[0] eq 'CODE') {
      push @_code, $_[0];

      push @_name, (defined $_params && ref $_params->{task_name} eq 'ARRAY')
         ? $_params->{task_name}->[$_pos] : undef;
      push @_wrks, (defined $_params && ref $_params->{max_workers} eq 'ARRAY')
         ? $_params->{max_workers}->[$_pos] : undef;

      $_init_mce = 1
         if (!defined $_prev_c[$_pos] || $_prev_c[$_pos] != $_code[$_pos]);

      {
         no warnings;
         $_init_mce = 1 if ($_prev_n[$_pos] ne $_name[$_pos]);
         $_init_mce = 1 if ($_prev_w[$_pos] ne $_wrks[$_pos]);
      }

      $_prev_c[$_pos] = $_code[$_pos];
      $_prev_n[$_pos] = $_name[$_pos];
      $_prev_w[$_pos] = $_wrks[$_pos];

      shift; $_pos++;
   }

   if (defined $_prev_c[$_pos]) {
      pop @_prev_c for ($_pos .. @_prev_c - 1);
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
      _gen_user_tasks(\@_code, \@_name, \@_wrks);

      $_MCE = MCE->new(
         max_workers => $_max_workers, task_name => $_tag,
         user_tasks => \@_user_tasks
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

   my @_a; my $_wa = wantarray; $_MCE->{gather} = \@_a if (defined $_wa);

   if (defined $_input_data) {
      @_ = (); $_MCE->process({ chunk_size => $_chunk_size }, $_input_data);
   }
   elsif (scalar @_) {
      $_MCE->process({ chunk_size => $_chunk_size }, \@_);
   }
   else {
      $_MCE->run({ chunk_size => $_chunk_size }, 0);
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

sub _gen_user_tasks {

   my ($_code_ref, $_name_ref, $_wrks_ref) = @_;

   @_user_tasks = ();

   for (my $_i = 0; $_i < @{ $_code_ref }; $_i++) {
      push @_user_tasks, {
         task_name   => $_name_ref->[$_i],
         max_workers => $_wrks_ref->[$_i],
         user_func   => $_code_ref->[$_i]
      }
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

MCE::Flow - Parallel flow model for building creative applications

=head1 VERSION

This document describes MCE::Flow version 1.499_005

=head1 DESCRIPTION

MCE::Flow is great for writing custom apps to maximize on all available cores.
Obviously, it's trivial to parallelize with mce_stream as seen below. However,
let's have MCE::Flow compute the same in parallel.

   ## Native map function.
   my @a = map { $_ * 4 } map { $_ * 3 } map { $_ * 2 } 1..10000;

   ## Same with MCE::Stream (from right to left).
   @a = mce_stream
        sub { $_ * 4 }, sub { $_ * 3 }, sub { $_ * 2 }, 1..10000;
     
   mce_stream \@a,
        sub { $_ * 4 }, sub { $_ * 3 }, sub { $_ * 2 }, 1..10000;

Ok, let's have some fun. This utilizes MCE::Queue. MCE::Flow makes it easy
to harness user_tasks within MCE.

   use MCE::Flow;
   use MCE::Queue;

This calls for preserving output order. Always remember to set $_order_id to 1
before running.

   my ($_gather_ref, $_order_id, %_tmp); 

   sub _preserve_order {

      $_tmp{$_[1]} = $_[0];

      while (1) {
         last unless exists $_tmp{$_order_id};
         push @{ $_gather_ref }, @{ $_tmp{$_order_id} };
         delete $_tmp{$_order_id++};
      }

      return;
   }

We need 2 queues for flow of data between the 3 tasks. Notice the task_end
function and how it checks the $task_name value for determining which task
has ended.

   my $b = MCE::Queue->new;
   my $c = MCE::Queue->new;

   sub task_end {
      my ($mce, $task_id, $task_name) = @_;

      if (defined $mce->{user_tasks}->[$task_id + 1]) {
         my $N_workers = $mce->{user_tasks}->[$task_id + 1]->{max_workers};

         if ($task_name eq 'a') {
            $b->enqueue((undef) x $N_workers);
         }
         elsif ($task_name eq 'b') {
            $c->enqueue((undef) x $N_workers);
         }
      }

      return;
   }

Now on with the 3 sub tasks. The first one reads input and begins the flow.
The 2nd dequeues, performs the calculation, and enqueues into the next.
Finally, the last sub task calls the gather method. Gather can be called
as often as needed. Data is frozen in-between tasks to save from double
serialization. This is the fastest approach in MCE with the least overhead.

   sub task_a {
      my @ans; my ($mce, $chunk_ref, $chunk_id) = @_;

      push @ans, map { $_ * 2 } @{ $chunk_ref };
      $b->enqueue(MCE->freeze([ \@ans, $chunk_id ]));
   }

   sub task_b {
      my ($mce) = @_;

      while (1) {
         my $chunk = $b->dequeue; last unless defined $chunk;
         my @ans; $chunk = MCE->thaw($chunk);

         push @ans, map { $_ * 3 } @{ $chunk->[0] };
         $c->enqueue(MCE->freeze([ \@ans, $chunk->[1] ]));
      }

      return;
   }

   sub task_c {
      my ($mce) = @_;

      while (1) {
         my $chunk = $c->dequeue; last unless defined $chunk;
         my @ans; $chunk = MCE->thaw($chunk);

         push @ans, map { $_ * 5 } @{ $chunk->[0] };
         MCE->gather(\@ans, $chunk->[1]);
      }

      return;
   }

To put it all together, MCE::Flow builds out a MCE instance behind the scene
automatically for you and runs. Task_name can take an array reference. The
same is true for max_workers (not shown below).

   my @a; $_gather_ref = \@a; $_order_id = 1;

   mce_flow {
      gather => \&_preserve_order, task_name => [ 'a', 'b', 'c' ],
      task_end => \&task_end

   }, \&task_a, \&task_b, \&task_c, 1..10000;

   print "@a" . "\n";

What if speed is not a concern and wanting to rid of all the MCE->freeze and
MCE->thaw in the code. Simply enqueue and dequeue 2 items at a time.

   $b->enqueue(\@ans, $chunk_id)

   ...

   my ($chunk_ref, $chunk_id) = $b->dequeue(2);
   last unless defined $chunk_ref;

   ...

The task_end must be updated as well due to workers dequeuing 2 items at a
time. Therefore, we must double the number of undefs into the queue.

   if ($task_name eq 'a') {
      $b->enqueue((undef) x ($N_workers * 2));
   }
   elsif ($task_name eq 'b') {
      $c->enqueue((undef) x ($N_workers * 2));
   }

For faster data serialization behind the scene, MCE::Flow can make use of
Sereal if available on the system. It silently falls back to Storable if
not available. Both chunk_size and max_workers default to auto.

   use MCE::Flow Sereal => 1, chunk_size => 'auto', max_workers => 'auto';

=head1 API DOCUMENTATION

=over

=item mce_flow

   ## mce_flow is imported into the calling script.

   mce_flow {
      ## Both this (mce_options) and input data are optional.
   },
   sub { ... }, sub { ... }, sub { ... }, 1..10000;

=item mce_flow_f

TODO ...

=item mce_flow_s

TODO ...

=item init

   MCE::Flow::init {

      ## This form is available for configuring MCE options
      ## before running.

      user_begin => sub {
         print "## ", MCE->task_name, ": ", MCE->wid, "\n";
      }
      user_end => sub {
         ...
      }
   };

=item finish

   MCE::Flow::finish();   ## This is called automatically.

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

