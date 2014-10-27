###############################################################################
## ----------------------------------------------------------------------------
## MCE::Stream - Parallel stream model for chaining multiple maps and greps.
##
###############################################################################

package MCE::Stream;

use strict;
use warnings;

use Scalar::Util qw( looks_like_number );

use MCE;
use MCE::Util;

use MCE::Queue;

our $VERSION = '1.519'; $VERSION = eval $VERSION;

###############################################################################
## ----------------------------------------------------------------------------
## Import routine.
##
###############################################################################

our $DEFAULT_MODE = 'map';
our $MAX_WORKERS  = 'auto';
our $CHUNK_SIZE   = 'auto';

my ($_params, @_prev_c, @_prev_m, @_prev_n, @_prev_w, @_user_tasks, @_queue);
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
   return if (defined $_MCE && $_MCE->wid);

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

   $_params->{task_end}->($_mce, $_task_id, $_task_name)
      if (exists $_params->{task_end} && ref $_params->{task_end} eq 'CODE');

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
      my $_r = ref $_[$_];
      if ($_r eq "" || $_r eq 'GLOB' || $_r eq 'SCALAR' || $_r =~ /^IO::/) {
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
   elsif (ref $_file eq 'GLOB' || ref $_file eq 'SCALAR' || ref($_file) =~ /^IO::/) {
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
            $_params->{sequence} = $_[$_pos];
         }
         elsif ($_ref eq 'ARRAY') {
            $_begin = $_[$_pos]->[0]; $_end = $_[$_pos]->[1];
            $_params->{sequence} = $_[$_pos];
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

   if ($_r eq 'ARRAY' || $_r eq 'GLOB' || $_r eq 'SCALAR' || $_r =~ /^IO::/) {
      $_input_data = shift;
   }

   if (defined $_params) { my $_p = $_params;
      $_max_workers = MCE::Util::_parse_max_workers($_p->{max_workers})
         if (exists $_p->{max_workers} && ref $_p->{max_workers} ne 'ARRAY');

      delete $_p->{sequence}    if (defined $_input_data || scalar @_);
      delete $_p->{user_func}   if (exists $_p->{user_func});
      delete $_p->{user_tasks}  if (exists $_p->{user_tasks});
      delete $_p->{use_slurpio} if (exists $_p->{use_slurpio});
      delete $_p->{bounds_only} if (exists $_p->{bounds_only});
      delete $_p->{gather}      if (exists $_p->{gather});
   }

   $_max_workers = int($_max_workers / @_code + 0.5) + 1
      if (@_code > 1);

   my $_chunk_size = MCE::Util::_parse_chunk_size(
      $CHUNK_SIZE, $_max_workers, $_params, $_input_data, scalar @_
   );

   if (defined $_params) {
      $_input_data = $_params->{input_data} if (exists $_params->{input_data});

      if (exists $_params->{_file}) {
         $_input_data = $_params->{_file}; delete $_params->{_file};
      }
   }

   MCE::_save_state;

   ## -------------------------------------------------------------------------

   if ($_init_mce) {
      $_MCE->shutdown() if (defined $_MCE);

      pop( @_queue )->DESTROY for (@_code .. @_queue);
      push @_queue, MCE::Queue->new for (@_queue .. @_code - 2);

      _gen_user_tasks(\@_queue, \@_code, \@_mode, \@_name, \@_wrks);

      my %_options = (
         use_threads => 0, max_workers => $_max_workers, task_name => $_tag,
         user_tasks => \@_user_tasks, task_end => \&_task_end,
         use_slurpio => 0
      );

      if (defined $_params) {
         my $_p = $_params;

         foreach (keys %{ $_p }) {
            next if ($_ eq 'max_workers' && ref $_p->{max_workers} eq 'ARRAY');
            next if ($_ eq 'task_name' && ref $_p->{task_name} eq 'ARRAY');
            next if ($_ eq 'input_data');
            next if ($_ eq 'chunk_size');
            next if ($_ eq 'task_end');

            _croak("MCE::Stream: '$_' is not a valid constructor argument")
               unless (exists $MCE::_valid_fields_new{$_});

            $_options{$_} = $_p->{$_};
         }
      }

      $_MCE = MCE->new(%_options);
   }
   else {
      ## Workers may persist after running. MCE::Stream allows options
      ## to be passed using an anonymous hash. Therefore, lets update
      ## the MCE instance. These options do not require respawning.

      for (qw(
         RS interval stderr_file stdout_file user_error user_output
         job_delay submit_delay on_post_exit on_post_run user_args
         flush_file flush_stderr flush_stdout
      )) {
         $_MCE->{$_} = $_params->{$_} if (exists $_params->{$_});
      }
   }

   ## -------------------------------------------------------------------------

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

         if (ref $_chunk_ref) {
            push @_a, ($_mode_ref->[-1] eq 'map')
               ?  map { &$_code } @{ $_chunk_ref }
               : grep { &$_code } @{ $_chunk_ref };
         }
         else {
            push @_a, ($_mode_ref->[-1] eq 'map')
               ?  map { &$_code } $_chunk_ref
               : grep { &$_code } $_chunk_ref;
         }

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

   $_n =~ s/K\z//i; $_n =~ s/M\z//i;

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

This document describes MCE::Stream version 1.519

=head1 SYNOPSIS

   ## Exports mce_stream, mce_stream_f, mce_stream_s
   use MCE::Stream;

   my (@m1, @m2, @m3);

   ## Default mode is map and processed from right-to-left
   @m1 = mce_stream sub { $_ * 3 }, sub { $_ * 2 }, 1..10000;
   mce_stream \@m2, sub { $_ * 3 }, sub { $_ * 2 }, 1..10000;

   ## Native Perl
   @m3 = map { $_ * $_ } grep { $_ % 5 == 0 } 1..10000;

   ## Streaming grep and map in parallel
   mce_stream \@m3,
      { mode => 'map',  code => sub { $_ * $_ } },
      { mode => 'grep', code => sub { $_ % 5 == 0 } }, 1..10000;

   ## Array or array_ref
   my @a = mce_stream sub { $_ * $_ }, 1..10000;
   my @b = mce_stream sub { $_ * $_ }, [ 1..10000 ];

   ## File_path, glob_ref, or scalar_ref
   my @c = mce_stream_f sub { chomp; $_ }, "/path/to/file";
   my @d = mce_stream_f sub { chomp; $_ }, $file_handle;
   my @e = mce_stream_f sub { chomp; $_ }, \$scalar;

   ## Sequence of numbers (begin, end [, step, format])
   my @f = mce_stream_s sub { $_ * $_ }, 1, 10000, 5;
   my @g = mce_stream_s sub { $_ * $_ }, [ 1, 10000, 5 ];

   my @h = mce_stream_s sub { $_ * $_ }, {
      begin => 1, end => 10000, step => 5, format => undef
   };

=head1 DESCRIPTION

This module allows one to stream multiple map and/or grep operations in
parallel. Code blocks run simultaneously from right-to-left. Chunk data are
sent immediately to the next code block during processing. The results are
appended immediately as well when passing the reference to the array.

   ## Appends are serialized, even out-of-order ok, but immediately.
   ## Out-of-order chunks are held temporarily until ordered chunks
   ## arrive before appending.

   mce_stream \@a, sub { $_ }, sub { $_ }, sub { $_ }, 1..10000;

   ##                                                    input
   ##                                        chunk1      input
   ##                            chunk3      chunk2      input
   ##                chunk2      chunk2      chunk3      input
   ##   append1      chunk3      chunk1      chunk4      input
   ##   append2      chunk1      chunk5      chunk5      input
   ##   append3      chunk5      chunk4      chunk6      ...
   ##   append4      chunk4      chunk6      ...
   ##   append5      chunk6      ...
   ##   append6      ...
   ##   ...
   ##

MCE incurs a small overhead due to passing of data. Therefore, a fast code
block will likely run faster when chaining multiple map functions natively in
Perl. However, the overhead quickly diminishes as the complexity of the code
increases.

   ## 0.542 secs -- baseline using the native map function
   my @m1 = map { $_ * 4 } map { $_ * 3 } map { $_ * 2 } 1..1000000;

   ## 0.765 secs -- this is quite amazing considering data passing
   my @m2 = mce_stream
         sub { $_ * 4 }, sub { $_ * 3 }, sub { $_ * 2 }, 1..1000000;

   ## 0.592 secs -- appends to @m3 are immediate
   my @m3; mce_stream \@m3,
         sub { $_ * 4 }, sub { $_ * 3 }, sub { $_ * 2 }, 1..1000000;

The mce_stream_s function will provide better times, useful when the input data
is simply a range of numbers. Workers generate sequences mathematically among
themselves without any interaction from the manager process. Two arguments are
required for mce_stream_s (begin, end). Step defaults to 1 if begin is smaller
than end, otherwise -1.

   ## 0.447 secs -- numbers are generated mathematically via sequence
   my @m4; mce_stream_s \@m4,
         sub { $_ * 4 }, sub { $_ * 3 }, sub { $_ * 2 }, 1, 1000000;

=head1 OVERRIDING DEFAULTS

The following list 6 options which may be overridden when loading the module.

   use Sereal qw(encode_sereal decode_sereal);

   use MCE::Stream
         default_mode => 'grep',               ## Default 'map'
         max_workers  => 8,                    ## Default 'auto'
         chunk_size   => 500,                  ## Default 'auto'
         tmp_dir      => "/path/to/app/tmp",   ## $MCE::Signal::tmp_dir
         freeze       => \&encode_sereal,      ## \&Storable::freeze
         thaw         => \&decode_sereal       ## \&Storable::thaw
   ;

There is a simpler way to enable Sereal with MCE 1.5. The following will
attempt to use Sereal if available, otherwise will default back to using
Storable for serialization.

   use MCE::Stream Sereal => 1;

   ## Serialization is through Sereal if available.
   my @m2 = mce_stream sub { $_ * $_ }, 1..10000;

=head1 CUSTOMIZING MCE

=over 3

=item init

The init function accepts a hash of MCE options. The gather and bounds_only
options, if specified, will be ignored due to being used internally by the
module (not shown below).

   use MCE::Stream;

   MCE::Stream::init {
      chunk_size => 1, max_workers => 4,

      user_begin => sub {
         print "## ", MCE->wid, " started\n";
      },

      user_end => sub {
         print "## ", MCE->wid, " completed\n";
      }
   };

   my @a = mce_stream sub { $_ * $_ }, 1..100;

   print "\n", "@a", "\n";

   -- Output

   ## 1 started
   ## 2 started
   ## 3 started
   ## 4 started
   ## 3 completed
   ## 1 completed
   ## 2 completed
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

Like with MCE::Stream::init above, MCE options may be specified using an
anonymous hash for the first argument. Notice how both max_workers and
task_name can take an anonymous array for setting values individually
for each code block. Remember that MCE::Stream processes from
right-to-left when setting the individual values.

   use MCE::Stream;

   my @a = mce_stream {
      max_workers => [ 2, 4, 3, ], task_name => [ 'c', 'b', 'a' ],

      user_end => sub {
         my ($task_id, $task_name) = (MCE->task_id, MCE->task_name);
         print "$task_id - $task_name completed\n";
      },

      task_end => sub {
         my ($mce, $task_id, $task_name) = @_;
         MCE->print("$task_id - $task_name ended\n");
      }
   },
   sub { $_ * 4 },             ## 2 workers, named c
   sub { $_ * 3 },             ## 4 workers, named b
   sub { $_ * 2 }, 1..10000;   ## 3 workers, named a

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

Note that the anonymous hash, for specifying options, also comes first when
passing the array reference.

   my @a; mce_stream {
      ...
   }, \@a, sub { ... }, sub { ... }, 1..10000;

=head1 API DOCUMENTATION

Scripts using MCE::Stream can be written using the long or short form. The long
form becomes relavant when mixing modes. Processing occurs from right-to-left.

   my @m3 = mce_stream
      { mode => 'map',  code => sub { $_ * $_ } },
      { mode => 'grep', code => sub { $_ % 5 == 0 } }, 1..10000;

   my @m4; mce_stream \@m4,
      { mode => 'map',  code => sub { $_ * $_ } },
      { mode => 'grep', code => sub { $_ % 5 == 0 } }, 1..10000;

For multiple grep blocks, the short form can be used. Simply specify the
default mode for the module. The two valid values for default_mode is 'grep'
and 'map'.

   use MCE::Stream default_mode => 'grep';

   my @f = mce_stream_f sub { /ending$/ }, sub { /^starting/ }, $file;

The following assumes 'map' for default_mode in order to demonstrate all the
possibilities of passing input data into the code block.

=over 3

=item mce_stream sub { code }, list

Input data can be defined using a list or passing a reference to an array.

   my @a = mce_stream sub { $_ * 2 }, 1..1000;
   my @b = mce_stream sub { $_ * 2 }, [ 1..1000 ];

=item mce_stream_f sub { code }, file

The fastest of these is the /path/to/file. Workers communicate the next offset
position among themselves without any interaction from the manager process.

   my @c = mce_stream_f sub { chomp; $_ . "\r\n" }, "/path/to/file";
   my @d = mce_stream_f sub { chomp; $_ . "\r\n" }, $file_handle;
   my @e = mce_stream_f sub { chomp; $_ . "\r\n" }, \$scalar;

=item mce_stream_s sub { code }, sequence

Sequence can be defined as a list, an array reference, or a hash reference.
The functions require both begin and end values to run. Step and format are
optional. The format is passed to sprintf (% may be omitted below).

   my ($beg, $end, $step, $fmt) = (10, 20, 0.1, "%4.1f");

   my @f = mce_stream_s sub { $_ }, $beg, $end, $step, $fmt;
   my @g = mce_stream_s sub { $_ }, [ $beg, $end, $step, $fmt ];

   my @h = mce_stream_s sub { $_ }, {
      begin => $beg, end => $end, step => $step, format => $fmt
   };

=item mce_stream { input_data => iterator }, sub { code }

An iterator reference can by specified for input data. Notice the anonymous
hash as the first argument to mce_stream. The only other way is to specify
input_data via MCE::Stream::init. This prevents MCE::Stream from configuring
the iterator reference as another user task which will not work.

Iterators are described under "SYNTAX for INPUT_DATA" at L<MCE::Core>.

   MCE::Stream::init {
      input_data => iterator
   };

   my @a = mce_stream sub { $_ * 3 }, sub { $_ * 2 };

=back

=head1 MANUAL SHUTDOWN

=over 3

=item finish

MCE workers remain persistent as much as possible after running. Shutdown
occurs when the script exits. One can manually shutdown MCE by simply calling
finish after running. This resets the MCE instance.

   use MCE::Stream;

   MCE::Stream::init {
      chunk_size => 20, max_workers => 'auto'
   };

   my @a = mce_stream { ... } 1..100;

   MCE::Stream::finish;

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

