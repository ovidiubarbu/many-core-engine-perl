###############################################################################
## ----------------------------------------------------------------------------
## MCE::Step - Parallel step model for building creative steps.
##
###############################################################################

package MCE::Step;

use strict;
use warnings;

## no critic (BuiltinFunctions::ProhibitStringyEval)
## no critic (Subroutines::ProhibitSubroutinePrototypes)
## no critic (TestingAndDebugging::ProhibitNoStrict)

use Scalar::Util qw( looks_like_number );

use MCE;
use MCE::Queue;

our $VERSION  = '1.699';

our @CARP_NOT = qw( MCE );

###############################################################################
## ----------------------------------------------------------------------------
## Import routine.
##
###############################################################################

my $MAX_WORKERS = 'auto';
my $CHUNK_SIZE  = 'auto';
my $FAST        = 0;

my ($_params, @_prev_c, @_prev_n, @_prev_t, @_prev_w, @_user_tasks, @_queue);
my ($_MCE, $_loaded, $_last_task_id, %_lkup); my $_tag = 'MCE::Step';

sub import {

   my $_class = shift; return if ($_loaded++);

   ## Process module arguments.
   while (my $_argument = shift) {
      my $_arg = lc $_argument;

      $MAX_WORKERS = shift and next if ( $_arg eq 'max_workers' );
      $CHUNK_SIZE  = shift and next if ( $_arg eq 'chunk_size' );

      $MCE::FREEZE = $MCE::MCE->{freeze} = shift and next
         if ( $_arg eq 'freeze' );
      $MCE::THAW = $MCE::MCE->{thaw} = shift and next
         if ( $_arg eq 'thaw' );

      if ( $_arg eq 'sereal' ) {
         if (shift eq '1') {
            local $@; eval 'use Sereal qw(encode_sereal decode_sereal)';
            unless ($@) {
               $MCE::FREEZE = $MCE::MCE->{freeze} = \&encode_sereal;
               $MCE::THAW = $MCE::MCE->{thaw} = \&decode_sereal;
            }
         }
         next;
      }

      if ( $_arg eq 'tmp_dir' ) {
         $MCE::TMP_DIR = $MCE::MCE->{tmp_dir} = shift;
         my $_e1 = 'is not a directory or does not exist';
         my $_e2 = 'is not writeable';
         _croak($_tag."::import: ($MCE::TMP_DIR) $_e1") unless -d $MCE::TMP_DIR;
         _croak($_tag."::import: ($MCE::TMP_DIR) $_e2") unless -w $MCE::TMP_DIR;
         next;
      }

      if ( $_arg eq 'fast' ) {
         $FAST = 1 if (shift eq '1');
         next;
      }

      _croak($_tag."::import: ($_argument) is not a valid module argument");
   }

   $MAX_WORKERS = MCE::Util::_parse_max_workers($MAX_WORKERS);
   _validate_number($MAX_WORKERS, 'MAX_WORKERS');

   _validate_number($CHUNK_SIZE, 'CHUNK_SIZE')
      unless ($CHUNK_SIZE eq 'auto');

   ## Import functions.
   no strict 'refs'; no warnings 'redefine';
   my $_pkg = caller;

   *{ $_pkg.'::mce_step_f' } = \&run_file;
   *{ $_pkg.'::mce_step_s' } = \&run_seq;
   *{ $_pkg.'::mce_step'   } = \&run;

   return;
}

END {
   return if (defined $_MCE && $_MCE->wid);

   finish();
}

###############################################################################
## ----------------------------------------------------------------------------
## The task end callback for when a task completes.
##
###############################################################################

sub _task_end {

   my ($_mce, $_task_id, $_task_name) = @_;

   if (defined $_mce->{user_tasks}->[$_task_id + 1]) {
      my $n_workers = $_mce->{user_tasks}->[$_task_id + 1]->{max_workers};
      $_queue[$_task_id]->enqueue((undef) x $n_workers);
   }

   $_params->{task_end}->($_mce, $_task_id, $_task_name)
      if (exists $_params->{task_end} && ref $_params->{task_end} eq 'CODE');

   return;
}

###############################################################################
## ----------------------------------------------------------------------------
## Methods for MCE; step, enq, enqp, await.
##
###############################################################################

{
   no warnings 'redefine';

   sub MCE::step {

      my $x = shift; my $self = ref($x) ? $x : $_MCE;

      _croak('MCE::step: method cannot be called by the manager process')
         unless ($self->{_wid});

      my $_task_id = $self->{_task_id};

      if ($_task_id < $_last_task_id) {
         if (scalar @_ > 1 || ref $_[0] || !defined $_[0]) {
            $_queue[$_task_id]->enqueue($self->freeze([ @_ ]).'1');
         } else {
            $_queue[$_task_id]->enqueue($_[0].'0');
         }
      }
      else {
         _croak('MCE::step: method cannot be called by the last task');
      }

      return;
   }

   ############################################################################

   sub MCE::enq {

      my $x = shift; my $self = ref($x) ? $x : $_MCE; my $_name = shift;

      _croak('MCE::enq: method cannot be called by the manager process')
         unless ($self->{_wid});
      _croak('MCE::enq: (task_name) is not specified or valid')
         if (!defined $_name || !exists $_lkup{$_name});
      _croak('MCE::enq: stepping to same task or backwards is not allowed')
         if ($_lkup{$_name} <= $self->{_task_id});

      my $_task_id = $_lkup{$_name} - 1;

      if ($_task_id < $_last_task_id) {
         if (scalar @_ > 1) {
            my @_items = map {
               (ref $_ || !defined $_) ? $self->freeze([ $_ ]).'1' : $_.'0';
            } @_;
            $_queue[$_task_id]->enqueue(@_items);
         }
         elsif (!defined $_[0] || ref $_[0]) {
            $_queue[$_task_id]->enqueue($self->freeze([ @_ ]).'1');
         }
         else {
            $_queue[$_task_id]->enqueue($_[0].'0');
         }
      }
      else {
         _croak('MCE::enq: method cannot be called by the last task');
      }

      return;
   }

   ############################################################################

   sub MCE::enqp {

      my $x = shift; my $self = ref($x) ? $x : $_MCE;
      my ($_name, $_p) = (shift, shift);

      _croak('MCE::enqp: method cannot be called by the manager process')
         unless ($self->{_wid});
      _croak('MCE::enqp: (task_name) is not specified or valid')
         if (!defined $_name || !exists $_lkup{$_name});
      _croak('MCE::enqp: stepping to same task or backwards is not allowed')
         if ($_lkup{$_name} <= $self->{_task_id});
      _croak('MCE::enqp: (priority) is not an integer')
         if (!looks_like_number($_p) || int($_p) != $_p);

      my $_task_id = $_lkup{$_name} - 1;

      if ($_task_id < $_last_task_id) {
         if (scalar @_ > 1) {
            my @_items = map {
               (ref $_ || !defined $_) ? $self->freeze([ $_ ]).'1' : $_.'0';
            } @_;
            $_queue[$_task_id]->enqueuep($_p, @_items);
         }
         elsif (!defined $_[0] || ref $_[0]) {
            $_queue[$_task_id]->enqueuep($_p, $self->freeze([ @_ ]).'1');
         }
         else {
            $_queue[$_task_id]->enqueuep($_p, $_[0].'0');
         }
      }
      else {
         _croak('MCE::enqp: method cannot be called by the last task');
      }

      return;
   }

   ############################################################################

   sub MCE::await {

      my $x = shift; my $self = ref($x) ? $x : $_MCE; my $_name = shift;

      _croak('MCE::await: method cannot be called by the manager process')
         unless ($self->{_wid});
      _croak('MCE::await: (task_name) is not specified or valid')
         if (!defined $_name || !exists $_lkup{$_name});
      _croak('MCE::await: awaiting from same task or backwards is not allowed')
         if ($_lkup{$_name} <= $self->{_task_id});

      my $_task_id = $_lkup{$_name} - 1;  my $_t = shift || 0;

      _croak('MCE::await: (threshold) is not an integer')
         if (!looks_like_number($_t) || int($_t) != $_t);

      if ($_task_id < $_last_task_id) {
         $_queue[$_task_id]->await($_t);
      } else {
         _croak('MCE::await: method cannot be called by the last task');
      }

      return;
   }

}

###############################################################################
## ----------------------------------------------------------------------------
## Init and finish routines.
##
###############################################################################

sub init (@) {

   shift if (defined $_[0] && $_[0] eq 'MCE::Step');

   if (MCE->wid) {
      @_ = (); _croak(
         "$_tag: function cannot be called by the worker process"
      );
   }

   finish(); $_params = (ref $_[0] eq 'HASH') ? shift : { @_ };

   @_ = ();

   return;
}

sub finish () {

   if (defined $_MCE && $_MCE->{_spawned}) {
      MCE::_save_state; $_MCE->shutdown(); MCE::_restore_state;
   }

   $_->DESTROY() for (@_queue); @_queue = ();

   @_prev_c = (); @_prev_n = (); @_prev_t = (); @_prev_w = ();

   @_user_tasks = (); %_lkup = ();

   return;
}

###############################################################################
## ----------------------------------------------------------------------------
## Parallel step with MCE -- file.
##
###############################################################################

sub run_file (@) {

   shift if (defined $_[0] && $_[0] eq 'MCE::Step');

   my ($_file, $_pos); my $_start_pos = (ref $_[0] eq 'HASH') ? 2 : 1;

   if (defined $_params) {
      delete $_params->{input_data} if (exists $_params->{input_data});
      delete $_params->{sequence}   if (exists $_params->{sequence});
   }
   else {
      $_params = {};
   }

   for my $_i ($_start_pos .. @_ - 1) {
      my $_r = ref $_[$_i];
      if ($_r eq '' || $_r eq 'GLOB' || $_r eq 'SCALAR' || $_r =~ /^IO::/) {
         $_file = $_[$_i]; $_pos = $_i;
         last;
      }
   }

   if (defined $_file && ref $_file eq '' && $_file ne '') {
      _croak("$_tag: ($_file) does not exist") unless (-e $_file);
      _croak("$_tag: ($_file) is not readable") unless (-r $_file);
      _croak("$_tag: ($_file) is not a plain file") unless (-f $_file);
      $_params->{_file} = $_file;
   }
   elsif (ref $_file eq 'GLOB' || ref $_file eq 'SCALAR' || ref($_file) =~ /^IO::/) {
      $_params->{_file} = $_file;
   }
   else {
      _croak("$_tag: (file) is not specified or valid");
   }

   if (defined $_pos) {
      pop @_ for ($_pos .. @_ - 1);
   }

   return run(@_);
}

###############################################################################
## ----------------------------------------------------------------------------
## Parallel step with MCE -- sequence.
##
###############################################################################

sub run_seq (@) {

   shift if (defined $_[0] && $_[0] eq 'MCE::Step');

   my ($_begin, $_end, $_pos); my $_start_pos = (ref $_[0] eq 'HASH') ? 2 : 1;

   if (defined $_params) {
      delete $_params->{sequence}   if (exists $_params->{sequence});
      delete $_params->{input_data} if (exists $_params->{input_data});
      delete $_params->{_file}      if (exists $_params->{_file});
   }
   else {
      $_params = {};
   }

   for my $_i ($_start_pos .. @_ - 1) {
      my $_ref = ref $_[$_i];

      if ($_ref eq '' || $_ref eq 'HASH' || $_ref eq 'ARRAY') {
         $_pos = $_i;

         if ($_ref eq '') {
            $_begin = $_[$_pos]; $_end = $_[$_pos + 1];
            $_params->{sequence} = [
               $_[$_pos], $_[$_pos + 1], $_[$_pos + 2], $_[$_pos + 3]
            ];
         }
         elsif ($_ref eq 'HASH') {
            $_begin = $_[$_pos]->{begin}; $_end = $_[$_pos]->{end};
            $_params->{sequence} = $_[$_pos];
         }
         elsif ($_ref eq 'ARRAY') {
            $_begin = $_[$_pos]->[0]; $_end = $_[$_pos]->[1];
            $_params->{sequence} = $_[$_pos];
         }

         last;
      }
   }

   _croak("$_tag: (sequence) is not specified or valid")
      unless (exists $_params->{sequence});

   _croak("$_tag: (begin) is not specified for sequence")
      unless (defined $_begin);

   _croak("$_tag: (end) is not specified for sequence")
      unless (defined $_end);

   $_params->{sequence_run} = 1;

   if (defined $_pos) {
      pop @_ for ($_pos .. @_ - 1);
   }

   return run(@_);
}

###############################################################################
## ----------------------------------------------------------------------------
## Parallel step with MCE.
##
###############################################################################

sub run (@) {

   shift if (defined $_[0] && $_[0] eq 'MCE::Step');

   if (MCE->wid) {
      @_ = (); _croak(
         "$_tag: function cannot be called by the worker process"
      );
   }

   if (ref $_[0] eq 'HASH') {
      $_params = {} unless defined $_params;
      for my $_p (keys %{ $_[0] }) {
         $_params->{$_p} = $_[0]->{$_p};
      }

      shift;
   }

   ## -------------------------------------------------------------------------

   my (@_code, @_name, @_thrs, @_wrks); my $_init_mce = 0; my $_pos = 0;

   %_lkup = ();

   while (ref $_[0] eq 'CODE') {
      push @_code, $_[0];

      push @_name, (defined $_params && ref $_params->{task_name} eq 'ARRAY')
         ? $_params->{task_name}->[$_pos] : undef;
      push @_thrs, (defined $_params && ref $_params->{use_threads} eq 'ARRAY')
         ? $_params->{use_threads}->[$_pos] : undef;
      push @_wrks, (defined $_params && ref $_params->{max_workers} eq 'ARRAY')
         ? $_params->{max_workers}->[$_pos] : undef;

      $_lkup{ $_name[ $_pos ] } = $_pos if (defined $_name[ $_pos ]);

      if (!defined $_prev_c[$_pos] || $_prev_c[$_pos] != $_code[$_pos]) {
         $_init_mce = 1;
      }

      {
         no warnings;
         $_init_mce = 1 if ($_prev_n[$_pos] ne $_name[$_pos]);
         $_init_mce = 1 if ($_prev_t[$_pos] ne $_thrs[$_pos]);
         $_init_mce = 1 if ($_prev_w[$_pos] ne $_wrks[$_pos]);
      }

      $_prev_c[$_pos] = $_code[$_pos];
      $_prev_n[$_pos] = $_name[$_pos];
      $_prev_t[$_pos] = $_thrs[$_pos];
      $_prev_w[$_pos] = $_wrks[$_pos];

      shift; $_pos++;
   }

   if (defined $_prev_c[$_pos]) {
      pop @_prev_c for ($_pos .. @_prev_c - 1);
      pop @_prev_n for ($_pos .. @_prev_n - 1);
      pop @_prev_t for ($_pos .. @_prev_t - 1);
      pop @_prev_w for ($_pos .. @_prev_w - 1);

      $_init_mce = 1;
   }

   return unless (scalar @_code);

   ## -------------------------------------------------------------------------

   my $_input_data; my $_max_workers = $MAX_WORKERS; my $_r = ref $_[0];

   if ($_r eq 'ARRAY' || $_r eq 'GLOB' || $_r eq 'SCALAR' || $_r =~ /^IO::/) {
      $_input_data = shift;
   }

   if (defined $_params) { my $_p = $_params;
      $_max_workers = MCE::Util::_parse_max_workers($_p->{max_workers})
         if (exists $_p->{max_workers} && ref $_p->{max_workers} ne 'ARRAY');

      delete $_p->{sequence}   if (defined $_input_data || scalar @_);
      delete $_p->{user_func}  if (exists $_p->{user_func});
      delete $_p->{user_tasks} if (exists $_p->{user_tasks});
   }

   if (@_code > 1 && $_max_workers > 1) {
      $_max_workers = int($_max_workers / @_code + 0.5) + 1;
   }

   my $_chunk_size = MCE::Util::_parse_chunk_size(
      $CHUNK_SIZE, $_max_workers, $_params, $_input_data, scalar @_
   );

   if (defined $_params) {
      if (exists $_params->{_file}) {
         $_input_data = delete $_params->{_file};
      }
      else {
         $_input_data = $_params->{input_data} if exists $_params->{input_data};
      }
   }

   MCE::_save_state;

   ## -------------------------------------------------------------------------

   if ($_init_mce) {
      $_MCE->shutdown() if (defined $_MCE);

      pop( @_queue )->DESTROY for (@_code .. @_queue);

      push @_queue, MCE::Queue->new(fast => $FAST, await => 1)
         for (@_queue .. @_code - 2);

      _gen_user_tasks(\@_queue, \@_code, \@_name, \@_thrs, \@_wrks, $_chunk_size);
      $_last_task_id = @_code - 1;

      my %_options = (
         max_workers => $_max_workers, task_name => $_tag,
         user_tasks  => \@_user_tasks, task_end  => \&_task_end,
      );

      if (defined $_params) {
         local $_; my $_p = $_params;

         for (keys %{ $_p }) {
            next if ($_ eq 'max_workers' && ref $_p->{max_workers} eq 'ARRAY');
            next if ($_ eq 'task_name'   && ref $_p->{task_name}   eq 'ARRAY');
            next if ($_ eq 'use_threads' && ref $_p->{use_threads} eq 'ARRAY');

            next if ($_ eq 'chunk_size');
            next if ($_ eq 'input_data');
            next if ($_ eq 'sequence_run');
            next if ($_ eq 'task_end');

            _croak("MCE::Step: ($_) is not a valid constructor argument")
               unless (exists $MCE::_valid_fields_new{$_});

            $_options{$_} = $_p->{$_};
         }
      }

      $_MCE = MCE->new(%_options);
   }
   else {
      ## Workers may persist after running. Thus, updating the MCE instance.
      ## These options do not require respawning.
      if (defined $_params) {
         for my $_p (qw(
            RS interval stderr_file stdout_file user_error user_output
            job_delay submit_delay on_post_exit on_post_run user_args
            flush_file flush_stderr flush_stdout gather
         )) {
            $_MCE->{$_p} = $_params->{$_p} if (exists $_params->{$_p});
         }
      }
   }

   ## -------------------------------------------------------------------------

   my @_a; my $_wa = wantarray; $_MCE->{gather} = \@_a if (defined $_wa);

   if (defined $_input_data) {
      @_ = ();
      $_MCE->process({ chunk_size => $_chunk_size }, $_input_data);
      delete $_MCE->{input_data};
   }
   elsif (scalar @_) {
      $_MCE->process({ chunk_size => $_chunk_size }, \@_);
      delete $_MCE->{input_data};
   }
   else {
      if (defined $_params && exists $_params->{sequence}) {
         $_MCE->run({
            chunk_size => $_chunk_size, sequence => $_params->{sequence}
         }, 0);
         if (exists $_params->{sequence_run}) {
            delete $_params->{sequence_run};
            delete $_params->{sequence};
         }
         delete $_MCE->{sequence};
      }
      else {
         $_MCE->run({ chunk_size => $_chunk_size }, 0);
      }
   }

   delete $_MCE->{gather} if (defined $_wa);

   MCE::_restore_state;

   if (exists $_MCE->{_rla_return}) {
      $MCE::MCE->{_rla_return} = delete $_MCE->{_rla_return};
   }

   finish() if ($^S);   ## shutdown if in eval state

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

sub _gen_user_func {

   my ($_queue_ref, $_code_ref, $_chunk_size, $_pos) = @_;

   my $_q_in  = $_queue_ref->[$_pos - 1];
   my $_c_ref = $_code_ref->[$_pos];

   return sub {
      my ($_mce) = @_;

      $_mce->{_next_jmp} = sub { goto _MCE_STEP__NEXT; };
      $_mce->{_last_jmp} = sub { goto _MCE_STEP__LAST; };

      _MCE_STEP__NEXT:

      while (defined (local $_ = $_q_in->dequeue())) {
         if (chop $_) {
            my $_args = $_mce->thaw($_);  $_ = $_args->[0];
            $_c_ref->($_mce, @{ $_args });
         } else {
            $_c_ref->($_mce, $_);
         }
      }

      _MCE_STEP__LAST:

      return;
   };
}

sub _gen_user_tasks {

   my (
      $_queue_ref, $_code_ref, $_name_ref, $_thrs_ref, $_wrks_ref, $_chunk_size
   ) = @_;

   @_user_tasks = ();

   push @_user_tasks, {
      task_name   => $_name_ref->[0],
      use_threads => $_thrs_ref->[0],
      max_workers => $_wrks_ref->[0],
      user_func   => sub { $_code_ref->[0]->(@_); return; }
   };

   for my $_pos (1 .. @{ $_code_ref } - 1) {
      push @_user_tasks, {
         task_name   => $_name_ref->[$_pos],
         use_threads => $_thrs_ref->[$_pos],
         max_workers => $_wrks_ref->[$_pos],
         user_func   => _gen_user_func(
            $_queue_ref, $_code_ref, $_chunk_size, $_pos
         )
      };
   }

   return;
}

sub _validate_number {

   my ($_n, $_key) = @_;

   _croak("$_tag: ($_key) is not valid") if (!defined $_n);

   $_n =~ s/K\z//i; $_n =~ s/M\z//i;

   if (!looks_like_number($_n) || int($_n) != $_n || $_n < 1) {
      _croak("$_tag: ($_key) is not valid");
   }

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

MCE::Step - Parallel step model for building creative steps

=head1 VERSION

This document describes MCE::Step version 1.699

=head1 DESCRIPTION

MCE::Step is similar to L<MCE::Flow|MCE::Flow> for writing custom apps.
The main difference comes from the transparent use of queues between
sub-tasks.

It is trivial to parallelize with mce_stream shown below.

   ## Native map function
   my @a = map { $_ * 4 } map { $_ * 3 } map { $_ * 2 } 1..10000;

   ## Same as with MCE::Stream (processing from right to left)
   @a = mce_stream
        sub { $_ * 4 }, sub { $_ * 3 }, sub { $_ * 2 }, 1..10000;

   ## Pass an array reference to have writes occur simultaneously
   mce_stream \@a,
        sub { $_ * 4 }, sub { $_ * 3 }, sub { $_ * 2 }, 1..10000;

However, let's have MCE::Step compute the same in parallel. Unlike the example
in L<MCE::Flow|MCE::Flow>, the use of MCE::Queue is totally transparent.

   use MCE::Step;

This calls for preserving output order.

   sub preserve_order {
      my %tmp; my $order_id = 1; my $gather_ref = $_[0];
      @{ $gather_ref } = ();  ## clear the array (optional)

      return sub {
         my ($data_ref, $chunk_id) = @_;
         $tmp{$chunk_id} = $data_ref;

         while (1) {
            last unless exists $tmp{$order_id};
            push @{ $gather_ref }, @{ delete $tmp{$order_id++} };
         }

         return;
      };
   }

Next are the 3 sub-tasks. Compare these 3 sub-tasks with the same as described
in L<MCE::Flow|MCE::Flow>. The call to MCE->step simplifies the passing of data
into the next sub-task.

   sub task_a {
      my @ans; my ($mce, $chunk_ref, $chunk_id) = @_;
      push @ans, map { $_ * 2 } @{ $chunk_ref };
      MCE->step(\@ans, $chunk_id);
   }

   sub task_b {
      my @ans; my ($mce, $chunk_ref, $chunk_id) = @_;
      push @ans, map { $_ * 3 } @{ $chunk_ref };
      MCE->step(\@ans, $chunk_id);
   }

   sub task_c {
      my @ans; my ($mce, $chunk_ref, $chunk_id) = @_;
      push @ans, map { $_ * 4 } @{ $chunk_ref };
      MCE->gather(\@ans, $chunk_id);
   }

In summary, MCE::Step builds out a MCE instance behind the scene and starts
running. The task_name (shown), max_workers, and use_threads options can take
an anonymous array for specifying the values uniquely for each sub-task.

   my @a;

   mce_step {
      task_name => [ 'a', 'b', 'c' ],
      gather => preserve_order(\@a)

   }, \&task_a, \&task_b, \&task_c, 1..10000;

   print "@a\n";

=head1 STEP DEMO

In the demonstration below, one may call ->gather or ->step any number of times
although ->step is not allowed in the last sub-block. Data is gathered to @arr
which may likely be out-of-order. Gathering data is optional. All sub-blocks
receive $mce as the first argument.

First, defining 3 sub-tasks.

   use MCE::Step;

   sub task_a {
      my ($mce, $chunk_ref, $chunk_id) = @_;

      if ($_ % 2 == 0) {
         MCE->gather($_);
       # MCE->gather($_ * 4);        ## Ok to gather multiple times
      }
      else {
         MCE->print("a step: $_, $_ * $_\n");
         MCE->step($_, $_ * $_);
       # MCE->step($_, $_ * 4 );     ## Ok to step multiple times
      }
   }

   sub task_b {
      my ($mce, $arg1, $arg2) = @_;

      MCE->print("b args: $arg1, $arg2\n");

      if ($_ % 3 == 0) {             ## $_ is the same as $arg1
         MCE->gather($_);
      }
      else {
         MCE->print("b step: $_ * $_\n");
         MCE->step($_ * $_);
      }
   }

   sub task_c {
      my ($mce, $arg1) = @_;

      MCE->print("c: $_\n");
      MCE->gather($_);
   }

Next, pass MCE options, using chunk_size 1, and run all 3 tasks in parallel.
Notice how max_workers and use_threads can take an anonymous array, similarly
to task_name.

   my @arr = mce_step {
      task_name   => [ 'a', 'b', 'c' ],
      max_workers => [  2,   2,   2  ],
      use_threads => [  0,   0,   0  ],
      chunk_size  => 1

   }, \&task_a, \&task_b, \&task_c, 1..10;

Finally, sort the array and display its contents.

   @arr = sort { $a <=> $b } @arr;

   print "\n@arr\n\n";

   -- Output

   a step: 1, 1 * 1
   a step: 3, 3 * 3
   a step: 5, 5 * 5
   a step: 7, 7 * 7
   a step: 9, 9 * 9
   b args: 1, 1
   b step: 1 * 1
   b args: 3, 9
   b args: 7, 49
   b step: 7 * 7
   b args: 5, 25
   b step: 5 * 5
   b args: 9, 81
   c: 1
   c: 49
   c: 25

   1 2 3 4 6 8 9 10 25 49

=head1 SYNOPSIS when CHUNK_SIZE EQUALS 1

Although L<MCE::Loop|MCE::Loop> may be preferred for running using a single
code block, the text below also applies to this module, particularly for the
first block.

All models in MCE default to 'auto' for chunk_size. The arguments for the block
are the same as writing a user_func block using the Core API.

Beginning with MCE 1.5, the next input item is placed into the input scalar
variable $_ when chunk_size equals 1. Otherwise, $_ points to $chunk_ref
containing many items. Basically, line 2 below may be omitted from your code
when using $_. One can call MCE->chunk_id to obtain the current chunk id.

   line 1:  user_func => sub {
   line 2:     my ($mce, $chunk_ref, $chunk_id) = @_;
   line 3:
   line 4:     $_ points to $chunk_ref->[0]
   line 5:        in MCE 1.5 when chunk_size == 1
   line 6:
   line 7:     $_ points to $chunk_ref
   line 8:        in MCE 1.5 when chunk_size  > 1
   line 9:  }

Follow this synopsis when chunk_size equals one. Looping is not required from
inside the first block. Hence, the block is called once per each item.

   ## Exports mce_step, mce_step_f, and mce_step_s
   use MCE::Step;

   MCE::Step::init {
      chunk_size => 1
   };

   ## Array or array_ref
   mce_step sub { do_work($_) }, 1..10000;
   mce_step sub { do_work($_) }, [ 1..10000 ];

   ## File_path, glob_ref, or scalar_ref
   mce_step_f sub { chomp; do_work($_) }, "/path/to/file";
   mce_step_f sub { chomp; do_work($_) }, $file_handle;
   mce_step_f sub { chomp; do_work($_) }, \$scalar;

   ## Sequence of numbers (begin, end [, step, format])
   mce_step_s sub { do_work($_) }, 1, 10000, 5;
   mce_step_s sub { do_work($_) }, [ 1, 10000, 5 ];

   mce_step_s sub { do_work($_) }, {
      begin => 1, end => 10000, step => 5, format => undef
   };

=head1 SYNOPSIS when CHUNK_SIZE is GREATER THAN 1

Follow this synopsis when chunk_size equals 'auto' or greater than 1.
This means having to loop through the chunk from inside the first block.

   use MCE::Step;

   MCE::Step::init {          ## Chunk_size defaults to 'auto' when
      chunk_size => 'auto'    ## not specified. Therefore, the init
   };                         ## function may be omitted.

   ## Syntax is shown for mce_step for demonstration purposes.
   ## Looping inside the block is the same for mce_step_f and
   ## mce_step_s.

   mce_step sub { do_work($_) for (@{ $_ }) }, 1..10000;

   ## Same as above, resembles code using the Core API.

   mce_step sub {
      my ($mce, $chunk_ref, $chunk_id) = @_;

      for (@{ $chunk_ref }) {
         do_work($_);
      }

   }, 1..10000;

Chunking reduces the number of IPC calls behind the scene. Think in terms of
chunks whenever processing a large amount of data. For relatively small data,
choosing 1 for chunk_size is fine.

=head1 OVERRIDING DEFAULTS

The following list 6 options which may be overridden when loading the module.

   use Sereal qw( encode_sereal decode_sereal );
   use CBOR::XS qw( encode_cbor decode_cbor );
   use JSON::XS qw( encode_json decode_json );

   use MCE::Step
         max_workers => 8,               ## Default 'auto'
         chunk_size => 500,              ## Default 'auto'
         fast => 1,                      ## Default 0 (fast queue?)
         tmp_dir => "/path/to/app/tmp",  ## $MCE::Signal::tmp_dir
         freeze => \&encode_sereal,      ## \&Storable::freeze
         thaw => \&decode_sereal         ## \&Storable::thaw
   ;

There is a simpler way to enable Sereal with MCE 1.5. The following will
attempt to use Sereal if available, otherwise defaults to Storable for
serialization.

   use MCE::Step Sereal => 1;

   MCE::Step::init {
      chunk_size => 1
   };

   ## Serialization is by the Sereal module if available.
   my %answer = mce_step sub { MCE->gather( $_, sqrt $_ ) }, 1..10000;

=head1 CUSTOMIZING MCE

=over 3

=item MCE::Step->init ( options )

=item MCE::Step::init { options }

The init function accepts a hash of MCE options. Unlike with MCE::Stream,
both gather and bounds_only options may be specified when calling init
(not shown below).

   use MCE::Step;

   MCE::Step::init {
      chunk_size => 1, max_workers => 4,

      user_begin => sub {
         print "## ", MCE->wid, " started\n";
      },

      user_end => sub {
         print "## ", MCE->wid, " completed\n";
      }
   };

   my %a = mce_step sub { MCE->gather($_, $_ * $_) }, 1..100;

   print "\n", "@a{1..100}", "\n";

   -- Output

   ## 3 started
   ## 1 started
   ## 4 started
   ## 2 started
   ## 3 completed
   ## 4 completed
   ## 1 completed
   ## 2 completed

   1 4 9 16 25 36 49 64 81 100 121 144 169 196 225 256 289 324 361
   400 441 484 529 576 625 676 729 784 841 900 961 1024 1089 1156
   1225 1296 1369 1444 1521 1600 1681 1764 1849 1936 2025 2116 2209
   2304 2401 2500 2601 2704 2809 2916 3025 3136 3249 3364 3481 3600
   3721 3844 3969 4096 4225 4356 4489 4624 4761 4900 5041 5184 5329
   5476 5625 5776 5929 6084 6241 6400 6561 6724 6889 7056 7225 7396
   7569 7744 7921 8100 8281 8464 8649 8836 9025 9216 9409 9604 9801
   10000

=back

Like with MCE::Step::init above, MCE options may be specified using an
anonymous hash for the first argument. Notice how task_name, max_workers,
and use_threads can take an anonymous array for setting uniquely for
each code block.

Unlike MCE::Stream which processes from right-to-left, MCE::Step begins
with the first code block, thus processing from left-to-right.

The following takes 9 seconds to complete. The 9 seconds is from having
only 2 workers assigned for the last sub-task and waiting 1 or 2 seconds
initially before calling MCE->step.

Removing both calls to MCE->step will cause the script to complete in just
1 second. The reason is due to the 2nd and subsequent sub-tasks awaiting
data from an internal queue. Workers terminate upon receiving an undef.

   use threads;
   use MCE::Step;

   my @a = mce_step {
      task_name   => [ 'a', 'b', 'c' ],
      max_workers => [  3,   4,   2, ],
      use_threads => [  1,   0,   0, ],

      user_end => sub {
         my ($mce, $task_id, $task_name) = @_;
         MCE->print("$task_id - $task_name completed\n");
      },

      task_end => sub {
         my ($mce, $task_id, $task_name) = @_;
         MCE->print("$task_id - $task_name ended\n");
      }
   },
   sub { sleep 1; MCE->step(""); },   ## 3 workers, named a
   sub { sleep 2; MCE->step(""); },   ## 4 workers, named b
   sub { sleep 3;                };   ## 2 workers, named c

   -- Output

   0 - a completed
   0 - a completed
   0 - a completed
   0 - a ended
   1 - b completed
   1 - b completed
   1 - b completed
   1 - b completed
   1 - b ended
   2 - c completed
   2 - c completed
   2 - c ended

=head1 API DOCUMENTATION

Although input data is optional for MCE::Step, the following assumes chunk_size
equals 1 in order to demonstrate all the possibilities of passing input data
into the code block.

=over 3

=item MCE::Step->run ( { input_data => iterator }, sub { code } )

=item mce_step { input_data => iterator }, sub { code }

An iterator reference can by specified for input_data. The only other way
is to specify input_data via MCE::Step::init. This prevents MCE::Step from
configuring the iterator reference as another user task which will not work.

Iterators are described under "SYNTAX for INPUT_DATA" at L<MCE::Core|MCE::Core>.

   MCE::Step::init {
      input_data => iterator
   };

   mce_step sub { $_ };

=item MCE::Step->run ( sub { code }, list )

=item mce_step sub { code }, list

Input data can be defined using a list.

   mce_step sub { $_ }, 1..1000;
   mce_step sub { $_ }, [ 1..1000 ];

=item MCE::Step->run_file ( sub { code }, file )

=item mce_step_f sub { code }, file

The fastest of these is the /path/to/file. Workers communicate the next offset
position among themselves without any interaction from the manager process.

   mce_step_f sub { $_ }, "/path/to/file";
   mce_step_f sub { $_ }, $file_handle;
   mce_step_f sub { $_ }, \$scalar;

=item MCE::Step->run_seq ( sub { code }, $beg, $end [, $step, $fmt ] )

=item mce_step_s sub { code }, $beg, $end [, $step, $fmt ]

Sequence can be defined as a list, an array reference, or a hash reference.
The functions require both begin and end values to run. Step and format are
optional. The format is passed to sprintf (% may be omitted below).

   my ($beg, $end, $step, $fmt) = (10, 20, 0.1, "%4.1f");

   mce_step_s sub { $_ }, $beg, $end, $step, $fmt;
   mce_step_s sub { $_ }, [ $beg, $end, $step, $fmt ];

   mce_step_s sub { $_ }, {
      begin => $beg, end => $end, step => $step, format => $fmt
   };

=back

The sequence engine can compute 'begin' and 'end' items only, for the chunk,
and not the items in between (hence boundaries only). This option applies
to sequence only and has no effect when chunk_size equals 1.

The time to run is 0.006s below. This becomes 0.827s without the bounds_only
option due to computing all items in between, thus creating a very large
array. Basically, specify bounds_only => 1 when boundaries is all you need
for looping inside the block; e.g. Monte Carlo simulations.

Time was measured using 1 worker to emphasize the difference.

   use MCE::Step;

   MCE::Step::init {
      max_workers => 1, chunk_size => 1_250_000,
      bounds_only => 1
   };

   ## For sequence, the input scalar $_ points to $chunk_ref
   ## when chunk_size > 1, otherwise $chunk_ref->[0].
   ##
   ## mce_step_s sub {
   ##    my $begin = $_->[0]; my $end = $_->[-1];
   ##
   ##    for ($begin .. $end) {
   ##       ...
   ##    }
   ##
   ## }, 1, 10_000_000;

   mce_step_s sub {
      my ($mce, $chunk_ref, $chunk_id) = @_;
      ## $chunk_ref contains 2 items, not 1_250_000

      my $begin = $chunk_ref->[ 0];
      my $end   = $chunk_ref->[-1];   ## or $chunk_ref->[1]

      MCE->printf("%7d .. %8d\n", $begin, $end);

   }, 1, 10_000_000;

   -- Output

         1 ..  1250000
   1250001 ..  2500000
   2500001 ..  3750000
   3750001 ..  5000000
   5000001 ..  6250000
   6250001 ..  7500000
   7500001 ..  8750000
   8750001 .. 10000000

=head1 GATHERING DATA

Unlike MCE::Map where gather and output order are done for you automatically,
the gather method is used to have results sent back to the manager process.

   use MCE::Step chunk_size => 1;

   ## Output order is not guaranteed.
   my @a = mce_step sub { MCE->gather($_ * 2) }, 1..100;
   print "@a\n\n";

   ## Outputs to a hash instead (key, value).
   my %h1 = mce_step sub { MCE->gather($_, $_ * 2) }, 1..100;
   print "@h1{1..100}\n\n";

   ## This does the same thing due to chunk_id starting at one.
   my %h2 = mce_step sub { MCE->gather(MCE->chunk_id, $_ * 2) }, 1..100;
   print "@h2{1..100}\n\n";

The gather method can be called multiple times within the block unlike return
which would leave the block. Therefore, think of gather as yielding results
immediately to the manager process without actually leaving the block.

   use MCE::Step chunk_size => 1, max_workers => 3;

   my @hosts = qw(
      hosta hostb hostc hostd hoste
   );

   my %h3 = mce_step sub {
      my ($output, $error, $status); my $host = $_;

      ## Do something with $host;
      $output = "Worker ". MCE->wid .": Hello from $host";

      if (MCE->chunk_id % 3 == 0) {
         ## Simulating an error condition
         local $? = 1; $status = $?;
         $error = "Error from $host"
      }
      else {
         $status = 0;
      }

      ## Ensure unique keys (key, value) when gathering to
      ## a hash.
      MCE->gather("$host.out", $output);
      MCE->gather("$host.err", $error) if (defined $error);
      MCE->gather("$host.sta", $status);

   }, @hosts;

   foreach my $host (@hosts) {
      print $h3{"$host.out"}, "\n";
      print $h3{"$host.err"}, "\n" if (exists $h3{"$host.err"});
      print "Exit status: ", $h3{"$host.sta"}, "\n\n";
   }

   -- Output

   Worker 3: Hello from hosta
   Exit status: 0

   Worker 2: Hello from hostb
   Exit status: 0

   Worker 1: Hello from hostc
   Error from hostc
   Exit status: 1

   Worker 3: Hello from hostd
   Exit status: 0

   Worker 2: Hello from hoste
   Exit status: 0

The following uses an anonymous array containing 3 elements when gathering
data. Serialization is automatic behind the scene.

   my %h3 = mce_step sub {
      ...

      MCE->gather($host, [$output, $error, $status]);

   }, @hosts;

   foreach my $host (@hosts) {
      print $h3{$host}->[0], "\n";
      print $h3{$host}->[1], "\n" if (defined $h3{$host}->[1]);
      print "Exit status: ", $h3{$host}->[2], "\n\n";
   }

Although MCE::Map comes to mind, one may want additional control when
gathering data such as retaining output order.

   use MCE::Step;

   sub preserve_order {
      my %tmp; my $order_id = 1; my $gather_ref = $_[0];

      return sub {
         $tmp{ (shift) } = \@_;

         while (1) {
            last unless exists $tmp{$order_id};
            push @{ $gather_ref }, @{ delete $tmp{$order_id++} };
         }

         return;
      };
   }

   ## Workers persist for the most part after running. Though, not always
   ## the case and depends on Perl. Pass a reference to a subroutine if
   ## workers must persist; e.g. mce_step { ... }, \&foo, 1..100000.

   MCE::Step::init {
      chunk_size => 'auto', max_workers => 'auto'
   };

   for (1..2) {
      my @m2;

      mce_step {
         gather => preserve_order(\@m2)
      },
      sub {
         my @a; my ($mce, $chunk_ref, $chunk_id) = @_;

         ## Compute the entire chunk data at once.
         push @a, map { $_ * 2 } @{ $chunk_ref };

         ## Afterwards, invoke the gather feature, which
         ## will direct the data to the callback function.
         MCE->gather(MCE->chunk_id, @a);

      }, 1..100000;

      print scalar @m2, "\n";
   }

   MCE::Step::finish;

All 6 models support 'auto' for chunk_size unlike the Core API. Think of the
models as the basis for providing JIT for MCE. They create the instance, tune
max_workers, and tune chunk_size automatically regardless of the hardware.

The following does the same thing using the Core API. Workers persist after
running.

   use MCE;

   sub preserve_order {
      ...
   }

   my $mce = MCE->new(
      max_workers => 'auto', chunk_size => 8000,

      user_func => sub {
         my @a; my ($mce, $chunk_ref, $chunk_id) = @_;

         ## Compute the entire chunk data at once.
         push @a, map { $_ * 2 } @{ $chunk_ref };

         ## Afterwards, invoke the gather feature, which
         ## will direct the data to the callback function.
         MCE->gather(MCE->chunk_id, @a);
      }
   );

   for (1..2) {
      my @m2;

      $mce->process({ gather => preserve_order(\@m2) }, [1..100000]);

      print scalar @m2, "\n";
   }

   $mce->shutdown;

=head1 MANUAL SHUTDOWN

=over 3

=item MCE::Step->finish

=item MCE::Step::finish

Workers remain persistent as much as possible after running. Shutdown occurs
automatically when the script terminates. Call finish when workers are no
longer needed.

   use MCE::Step;

   MCE::Step::init {
      chunk_size => 20, max_workers => 'auto'
   };

   mce_step sub { ... }, 1..100;

   MCE::Step::finish;

=back

=head1 INDEX

L<MCE|MCE>

=head1 AUTHOR

Mario E. Roy, S<E<lt>marioeroy AT gmail DOT comE<gt>>

=cut

