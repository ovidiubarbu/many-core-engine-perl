###############################################################################
## ----------------------------------------------------------------------------
## Many-Core Engine (MCE) for Perl. Provides parallel processing capabilities.
##
###############################################################################

package MCE;

use strict;
use warnings;

use Fcntl qw( :flock O_CREAT O_TRUNC O_RDWR O_RDONLY );
use Storable 2.04 qw( store retrieve freeze thaw );
use Socket qw( :DEFAULT :crlf );

use MCE::Signal;

my ($_que_template, $_que_read_size);
my ($_has_threads, $_max_files, $_max_procs);

BEGIN {
   ## Configure template to use for pack/unpack for writing to and reading from
   ## the queue. Each entry contains 2 positive numbers: chunk_id & msg_id.
   ## Attempt 64-bit size, otherwize fall back to host machine's word length.
   {
      local $@; local $SIG{__DIE__} = \&_NOOP;
      eval { $_que_read_size = length(pack('Q2', 0, 0)); };
      $_que_template  = ($@) ? 'I2' : 'Q2';
      $_que_read_size = length(pack($_que_template, 0, 0));
   }

   ## Determine the underlying maximum number of files.
   unless ($^O eq 'MSWin32') {
      my $_bash = (-x '/bin/bash') ? '/bin/bash' : undef;
         $_bash = '/usr/bin/bash' if (!$_bash && -x '/usr/bin/bash');

      if ($_bash) {
         my $_res = `$_bash -c 'ulimit -n; ulimit -u'`;  ## max files, procs

         $_res =~ /^(\S+)\s(\S+)/m;
         $_max_files = $1 || 256;
         $_max_procs = $2 || 256;

         $_max_files = ($_max_files =~ /\A\d+\z/) ? $_max_files : 256;
         $_max_procs = ($_max_procs =~ /\A\d+\z/) ? $_max_procs : 256;
      }
      else {
         $_max_files = $_max_procs = 256;
      }
   }
}

###############################################################################
## ----------------------------------------------------------------------------
## This module does not load the threads module. Please include your threading
## library of choice prir to including MCE library. This is only a requirement
## if you're wanting to use threads versus forking.
##
###############################################################################

my ($_COM_LOCK, $_DAT_LOCK, $_SYN_LOCK);
our $_MCE_LOCK : shared = 1;

sub import {
   unless (defined $_has_threads) {
      if (defined $threads::VERSION) {
         unless (defined $threads::shared::VERSION) {
            local $@; local $SIG{__DIE__} = \&_NOOP;
            eval 'use threads::shared; threads::shared::share($_MCE_LOCK)';
         }
         $_has_threads = 1;
      }
      $_has_threads = $_has_threads || 0;
   }
}

our $VERSION = '1.406';
$VERSION = eval $VERSION;

## PDL + MCE (spawning as threads) is not stable. Thanks goes to David Mertens
## for reporting on how he fixed it for his PDL::Parallel::threads module. The
## same fix is also needed here in order for PDL + MCE threads to not crash
## during exiting.

{
   no warnings 'redefine';
   sub PDL::CLONE_SKIP { 1 }
}

###############################################################################
## ----------------------------------------------------------------------------
## Define constants & variables.
##
###############################################################################

use constant {
   MAX_CHUNK_SIZE  => 24 * 1024 * 1024,  ## Set max constraints
   MAX_OPEN_FILES  => $_max_files || 256,
   MAX_USER_PROCS  => $_max_procs || 256,

   MAX_RECS_SIZE   => 8192,              ## Read # of records if <= value
                                         ## Read # of bytes   if >  value

   QUE_TEMPLATE    => $_que_template,    ## Pack template for queue socket
   QUE_READ_SIZE   => $_que_read_size,   ## Read size

   OUTPUT_B_SYN    => ':B~SYN',          ## Worker barrier sync - begin
   OUTPUT_E_SYN    => ':E~SYN',          ## Worker barrier sync - end

   OUTPUT_W_ABT    => ':W~ABT',          ## Worker has aborted
   OUTPUT_W_DNE    => ':W~DNE',          ## Worker has completed
   OUTPUT_W_EXT    => ':W~EXT',          ## Worker has exited
   OUTPUT_A_ARY    => ':A~ARY',          ## Array  << Array
   OUTPUT_S_GLB    => ':S~GLB',          ## Scalar << Glob FH
   OUTPUT_A_CBK    => ':A~CBK',          ## Callback (/w arguments)
   OUTPUT_N_CBK    => ':N~CBK',          ## Callback (no arguments)
   OUTPUT_S_CBK    => ':S~CBK',          ## Callback (1 scalar arg)
   OUTPUT_S_OUT    => ':S~OUT',          ## Scalar >> STDOUT
   OUTPUT_S_ERR    => ':S~ERR',          ## Scalar >> STDERR
   OUTPUT_S_FLE    => ':S~FLE',          ## Scalar >> File

   READ_FILE       => 0,                 ## Worker reads file handle
   READ_MEMORY     => 1,                 ## Worker reads memory handle

   REQUEST_ARRAY   => 0,                 ## Worker requests next array chunk
   REQUEST_GLOB    => 1,                 ## Worker requests next glob chunk

   SENDTO_FILEV1   => 0,                 ## Worker sends to 'file', $a, '/path'
   SENDTO_FILEV2   => 1,                 ## Worker sends to 'file:/path', $a
   SENDTO_STDOUT   => 2,                 ## Worker sends to STDOUT
   SENDTO_STDERR   => 3,                 ## Worker sends to STDERR

   WANTS_UNDEFINE  => 0,                 ## Callee wants nothing
   WANTS_ARRAY     => 1,                 ## Callee wants list
   WANTS_SCALAR    => 2,                 ## Callee wants scalar
   WANTS_REFERENCE => 3                  ## Callee wants H/A/S ref
};

undef $_max_files; undef $_max_procs;
undef $_que_template; undef $_que_read_size;

my %_valid_fields = map { $_ => 1 } qw(
   max_workers tmp_dir use_threads user_tasks task_end

   chunk_size input_data job_delay spawn_delay submit_delay use_slurpio RS
   flush_file flush_stderr flush_stdout stderr_file stdout_file on_post_exit
   sequence user_begin user_end user_func user_error user_output on_post_run
   user_args

   _abort_msg _mce_sid _mce_tid _pids _run_mode _single_dim _thrs _tids _wid
   _com_r_sock _com_w_sock _dat_r_sock _dat_w_sock _out_r_sock _out_w_sock
   _que_r_sock _que_w_sock _sess_dir _spawned _state _status _task _task_id
   _exiting _exit_pid _total_exited _total_running _total_workers _task_wid
   _send_cnt

);

my %_params_allowed_args = map { $_ => 1 } qw(
   chunk_size input_data job_delay spawn_delay submit_delay use_slurpio RS
   flush_file flush_stderr flush_stdout stderr_file stdout_file on_post_exit
   sequence user_begin user_end user_func user_error user_output on_post_run
   user_args
);

my $_is_cygwin    = ($^O eq 'cygwin');
my $_is_winperl   = ($^O eq 'MSWin32');
my $_mce_tmp_dir  = $MCE::Signal::tmp_dir;
my %_mce_sess_dir = ();
my %_mce_spawned  = ();
my $_mce_count    = 0;

$MCE::Signal::mce_sess_dir_ref = \%_mce_sess_dir;
$MCE::Signal::mce_spawned_ref  = \%_mce_spawned;

## Warnings are disabled to minimize bits of noise when user or OS signals
## the script to exit. e.g. MCE_script.pl < infile | head

no warnings 'threads'; no warnings 'uninitialized';

sub DESTROY { }

###############################################################################
## ----------------------------------------------------------------------------
## New instance instantiation.
##
###############################################################################

sub new {

   my ($class, %argv) = @_;

   @_ = ();

   my $self = {}; bless($self, ref($class) || $class);

   ## Public options.
   $self->{tmp_dir}      = $argv{tmp_dir}      || $_mce_tmp_dir;
   $self->{input_data}   = $argv{input_data}   || undef;
   $self->{chunk_size}   = $argv{chunk_size}   || 1;
   $self->{max_workers}  = $argv{max_workers}  || 1;
   $self->{use_slurpio}  = $argv{use_slurpio}  || 0;

   if (exists $argv{use_threads}) {
      $self->{use_threads} = $argv{use_threads};
      if (!$_has_threads && $argv{use_threads} ne '0') {
         my $_msg  = "\n";
            $_msg .= "## Please include threads support prior to loading MCE\n";
            $_msg .= "## when specifying use_threads => $argv{use_threads}\n";
            $_msg .= "\n";
            
         _croak($_msg);
      }
   }
   else {
      $self->{use_threads} = ($_has_threads) ? 1 : 0;
   }

   $MCE::Signal::has_threads = 1
      if ($self->{use_threads} && !$MCE::Signal::has_threads);

   $self->{job_delay}    = $argv{job_delay}    || undef;
   $self->{spawn_delay}  = $argv{spawn_delay}  || undef;
   $self->{submit_delay} = $argv{submit_delay} || undef;
   $self->{on_post_exit} = $argv{on_post_exit} || undef;
   $self->{on_post_run}  = $argv{on_post_run}  || undef;
   $self->{sequence}     = $argv{sequence}     || undef;
   $self->{user_args}    = $argv{user_args}    || undef;
   $self->{user_begin}   = $argv{user_begin}   || undef;
   $self->{user_func}    = $argv{user_func}    || undef;
   $self->{user_end}     = $argv{user_end}     || undef;
   $self->{user_error}   = $argv{user_error}   || undef;
   $self->{user_output}  = $argv{user_output}  || undef;
   $self->{flush_file}   = $argv{flush_file}   || 0;
   $self->{flush_stderr} = $argv{flush_stderr} || 0;
   $self->{flush_stdout} = $argv{flush_stdout} || 0;
   $self->{stderr_file}  = $argv{stderr_file}  || undef;
   $self->{stdout_file}  = $argv{stdout_file}  || undef;
   $self->{user_tasks}   = $argv{user_tasks}   || undef;
   $self->{task_end}     = $argv{task_end}     || undef;
   $self->{RS}           = $argv{RS}           || undef;

   ## Validation.
   for (keys %argv) {
      _croak("MCE::new: '$_' is not a valid constructor argument")
         unless (exists $_valid_fields{$_});
   }

   _croak("MCE::new: '$self->{tmp_dir}' is not a directory or does not exist")
      unless (-d $self->{tmp_dir});
   _croak("MCE::new: '$self->{tmp_dir}' is not writeable")
      unless (-w $self->{tmp_dir});

   if (defined $self->{user_tasks}) {
      _croak("MCE::new: 'user_tasks' is not an ARRAY reference")
         unless (ref $self->{user_tasks} eq 'ARRAY');

      for my $_task (@{ $self->{user_tasks} }) {
         $_task->{max_workers} = $self->{max_workers}
            unless (defined $_task->{max_workers});
         $_task->{use_threads} = $self->{use_threads}
            unless (defined $_task->{use_threads});
      }

      if ($_is_cygwin) {
         ## File locking fails among children and threads under Cygwin.
         ## Must be all children or all threads, not intermixed.
         my (%_values, $_value);

         for my $_task (@{ $self->{user_tasks} }) {
            $_value = (defined $_task->{use_threads})
               ? $_task->{use_threads} : $self->{use_threads};
            $_values{$_value} = '';
         }

         _croak("MCE::new: 'cannot mix' use_threads => 0/1 in Cygwin")
            if (keys %_values > 1);
      }
   }

   $self->_validate_args(); %argv = ();

   ## Private options.

   $self->{_pids}       = undef; ## Array for joining children when completed
   $self->{_thrs}       = undef; ## Array for joining threads when completed
   $self->{_tids}       = undef; ## Array for joining threads when completed
   $self->{_status}     = undef; ## Array of Hashes to hold $self->exit data
   $self->{_exit_pid}   = undef; ## Worker exit ID e.g. TID_123, PID_1234
   $self->{_mce_sid}    = undef; ## Spawn ID defined at time of spawning
   $self->{_mce_tid}    = undef; ## Thread ID when spawn was called
   $self->{_sess_dir}   = undef; ## Unique session dir when spawn was called
   $self->{_com_r_sock} = undef; ## Communication channel for MCE
   $self->{_com_w_sock} = undef; ## Communication channel for workers
   $self->{_dat_r_sock} = undef; ## Data channel for MCE
   $self->{_dat_w_sock} = undef; ## Data channel for workers
   $self->{_out_r_sock} = undef; ## For serialized reads by main thread/process
   $self->{_out_w_sock} = undef; ## Workers write to this for serialized writes
   $self->{_que_r_sock} = undef; ## Queue channel for MCE
   $self->{_que_w_sock} = undef; ## Queue channel for workers
   $self->{_state}      = undef; ## State info: task/task_id/task_wid/params
   $self->{_task}       = undef; ## Task info: total_running/total_workers

   $self->{_send_cnt}   =     0; ## Number of times data was sent via send
   $self->{_spawned}    =     0; ## Workers spawned
   $self->{_task_id}    =     0; ## Task ID        starts at 0 (array index)
   $self->{_task_wid}   =     0; ## Task Worker ID starts at 1 per task
   $self->{_wid}        =     0; ## MCE Worker ID  starts at 1 per MCE instance

   ## -------------------------------------------------------------------------

   ## Limit chunk_size and max_workers -- allow for some headroom.

   $self->{chunk_size} = MAX_CHUNK_SIZE
      if ($self->{chunk_size} > MAX_CHUNK_SIZE);

   if ($^O eq 'cygwin') {                 ## Limit to 48 threads, 24 children
      $self->{max_workers} = 48
         if ($self->{use_threads} && $self->{max_workers} > 48);
      $self->{max_workers} = 24
         if (!$self->{use_threads} && $self->{max_workers} > 24);
   }
   elsif ($^O eq 'MSWin32') {             ## Limit to 64 threads, 32 children
      $self->{max_workers} = 64
         if ($self->{use_threads} && $self->{max_workers} > 64);
      $self->{max_workers} = 32
         if (!$self->{use_threads} && $self->{max_workers} > 32);
   }
   else {
      if ($self->{use_threads}) {
         $self->{max_workers} = int(MAX_OPEN_FILES / 3) - 8
            if ($self->{max_workers} > int(MAX_OPEN_FILES / 3) - 8);
      } else {
         $self->{max_workers} = MAX_USER_PROCS - 64
            if ($self->{max_workers} > MAX_USER_PROCS - 64);
      }
   }

   return $self;
}

###############################################################################
## ----------------------------------------------------------------------------
## Spawn method.
##
###############################################################################

sub spawn {

   my MCE $self = $_[0];

   ## To avoid leaking (Scalars leaked: 1) messages (fixed in Perl 5.12.x).
   @_ = ();

   _croak("MCE::spawn: method cannot be called by the worker process")
      if ($self->{_wid});

   ## Return if workers have already been spawned.
   return $self unless ($self->{_spawned} == 0);

   lock $_MCE_LOCK if ($_has_threads);            ## Obtain MCE lock.

   my $_die_handler  = $SIG{__DIE__};  $SIG{__DIE__}  = \&_die;
   my $_warn_handler = $SIG{__WARN__}; $SIG{__WARN__} = \&_warn;

   ## Configure tid/sid for this instance here, not in the new method above.
   ## We want the actual thread id in which spawn was called under.
   unless ($self->{_mce_tid}) {
      $self->{_mce_tid} = ($_has_threads) ? threads->tid() : '';
      $self->{_mce_tid} = '' unless (defined $self->{_mce_tid});
      $self->{_mce_sid} = $$ .'.'. $self->{_mce_tid} .'.'. (++$_mce_count);
   }

   my $_mce_sid  = $self->{_mce_sid};
   my $_sess_dir = $self->{_sess_dir};
   my $_tmp_dir  = $self->{tmp_dir};

   ## Create temp dir.
   unless ($_sess_dir) {
      _croak("MCE::spawn: '$_tmp_dir' is not defined")
         if (!defined $_tmp_dir || $_tmp_dir eq '');
      _croak("MCE::spawn: '$_tmp_dir' is not a directory or does not exist")
         unless (-d $_tmp_dir);
      _croak("MCE::spawn: '$_tmp_dir' is not writeable")
         unless (-w $_tmp_dir);

      my $_cnt = 0; $_sess_dir = $self->{_sess_dir} = "$_tmp_dir/$_mce_sid";

      while ( !(mkdir $_sess_dir, 0770) ) {
         $_sess_dir = $self->{_sess_dir} = "$_tmp_dir/$_mce_sid." . (++$_cnt);
      }

      $_mce_sess_dir{$_sess_dir} = 1;
   }

   ## -------------------------------------------------------------------------

   ## Create socket pair for communication channels between MCE and workers.
   socketpair( $self->{_com_r_sock}, $self->{_com_w_sock},
      AF_UNIX, SOCK_STREAM, PF_UNSPEC ) or die "socketpair: $!\n";

   binmode  $self->{_com_r_sock};                 ## Set binary mode
   binmode  $self->{_com_w_sock};

   ## Create socket pair for data channels between MCE and workers.
   socketpair( $self->{_dat_r_sock}, $self->{_dat_w_sock},
      AF_UNIX, SOCK_STREAM, PF_UNSPEC ) or die "socketpair: $!\n";

   binmode  $self->{_dat_r_sock};                 ## Set binary mode
   binmode  $self->{_dat_w_sock};

   ## Create socket pair for serializing send requests and STDOUT output.
   socketpair( $self->{_out_r_sock}, $self->{_out_w_sock},
      AF_UNIX, SOCK_STREAM, PF_UNSPEC ) or die "socketpair: $!\n";

   binmode  $self->{_out_r_sock};                 ## Set binary mode
   binmode  $self->{_out_w_sock};
   shutdown $self->{_out_r_sock}, 1;              ## No more writing
   shutdown $self->{_out_w_sock}, 0;              ## No more reading

   ## Create socket pairs for queue channels between MCE and workers.
   socketpair( $self->{_que_r_sock}, $self->{_que_w_sock},
      AF_UNIX, SOCK_STREAM, PF_UNSPEC ) or die "socketpair: $!\n";

   binmode  $self->{_que_r_sock};                 ## Set binary mode
   binmode  $self->{_que_w_sock};
   shutdown $self->{_que_r_sock}, 1;              ## No more writing
   shutdown $self->{_que_w_sock}, 0;              ## No more reading

   ## Autoflush handles. This is done this way versus the inclusion of the
   ## large IO::Handle module just to call autoflush(1).
   my $_old_hndl = select $self->{_com_r_sock}; $| = 1;
                   select $self->{_com_w_sock}; $| = 1;
                   select $self->{_dat_r_sock}; $| = 1;
                   select $self->{_dat_w_sock}; $| = 1;
                   select $self->{_out_r_sock}; $| = 1;
                   select $self->{_out_w_sock}; $| = 1;
                   select $self->{_que_r_sock}; $| = 1;
                   select $self->{_que_w_sock}; $| = 1;

   select $_old_hndl;

   ## -------------------------------------------------------------------------

   $self->{_pids}   = (); $self->{_thrs}  = (); $self->{_tids} = ();
   $self->{_status} = (); $self->{_state} = (); $self->{_task} = ();

   my $_max_workers = $self->{max_workers};
   my $_use_threads = $self->{use_threads};

   ## Obtain lock.
   open my $_COM_LOCK, '+>> :stdio', "$_sess_dir/_com.lock";
   flock $_COM_LOCK, LOCK_EX;

   ## Spawn workers.
   $_mce_spawned{$_mce_sid} = $self;

   unless (defined $self->{user_tasks}) {
      $self->{_total_workers} = $_max_workers;

      if (defined $_use_threads && $_use_threads == 1) {
         $self->_dispatch_thread($_) for (1 .. $_max_workers);
      } else {
         $self->_dispatch_child($_) for (1 .. $_max_workers);
      }

      $self->{_task}->[0] = { _total_workers => $_max_workers };

      for (1 .. $_max_workers) {
         keys(%{ $self->{_state}->[$_] }) = 4;
         $self->{_state}->[$_] = {
            _task => undef, _task_id => undef, _task_wid => undef,
            _params => undef
         }
      }
   }
   else {
      my ($_task_id, $_wid);

      $_task_id = $_wid = $self->{_total_workers} = 0;

      $self->{_total_workers} += $_->{max_workers}
         for (@{ $self->{user_tasks} });

      for my $_task (@{ $self->{user_tasks} }) {
         my $_use_threads = $_task->{use_threads};

         if (defined $_use_threads && $_use_threads == 1) {
            $self->_dispatch_thread(++$_wid, $_task, $_task_id, $_)
               for (1 .. $_task->{max_workers});
         } else {
            $self->_dispatch_child(++$_wid, $_task, $_task_id, $_)
               for (1 .. $_task->{max_workers});
         }

         $_task_id++;
      }

      $_task_id = $_wid = 0;

      for my $_task (@{ $self->{user_tasks} }) {
         $self->{_task}->[$_task_id] = {
            _total_running => 0, _total_workers => $_task->{max_workers}
         };
         for (1 .. $_task->{max_workers}) {
            keys(%{ $self->{_state}->[++$_wid] }) = 4;
            $self->{_state}->[$_wid] = {
               _task => $_task, _task_id => $_task_id, _task_wid => $_,
               _params => undef
            }
         }

         $_task_id++;
      }
   }

   ## Await reply from the last worker spawned.
   if ($self->{_total_workers} > 0) {
      my $_COM_R_SOCK = $self->{_com_r_sock};
      local $/ = $LF; <$_COM_R_SOCK>;
   }

   $self->{_send_cnt} = 0;
   $self->{_spawned}  = 1;

   ## Release lock.
   flock $_COM_LOCK, LOCK_UN;
   close $_COM_LOCK; undef $_COM_LOCK;

   $SIG{__DIE__}  = $_die_handler;
   $SIG{__WARN__} = $_warn_handler;

   return $self;
}

###############################################################################
## ----------------------------------------------------------------------------
## Forchunk method.
##
###############################################################################

sub forchunk {

   my MCE $self = $_[0]; my $_input_data = $_[1];

   _croak("MCE::forchunk: method cannot be called by the worker process")
      if ($self->{_wid});
   _croak("MCE::forchunk: method cannot be called while processing")
      if ($self->{_send_cnt});
   _croak("MCE::forchunk: method cannot be called while running")
      if ($self->{_total_running});

   my ($_user_func, $_params_ref);

   if (ref $_[2] eq 'HASH') {
      $_user_func = $_[3]; $_params_ref = $_[2];
   } else {
      $_user_func = $_[2]; $_params_ref = {};
   }

   @_ = ();

   _croak("MCE::forchunk: 'input_data' is not specified")
      unless (defined $_input_data);
   _croak("MCE::forchunk: 'code_block' is not specified")
      unless (defined $_user_func);

   $_params_ref->{input_data} = $_input_data;
   $_params_ref->{user_func}  = $_user_func;

   $self->run(1, $_params_ref);

   return $self;
}

###############################################################################
## ----------------------------------------------------------------------------
## Foreach method.
##
###############################################################################

sub foreach {

   my MCE $self = $_[0]; my $_input_data = $_[1];

   _croak("MCE::foreach: method cannot be called by the worker process")
      if ($self->{_wid});
   _croak("MCE::foreach: method cannot be called while processing")
      if ($self->{_send_cnt});
   _croak("MCE::foreach: method cannot be called while running")
      if ($self->{_total_running});

   my ($_user_func, $_params_ref);

   if (ref $_[2] eq 'HASH') {
      $_user_func = $_[3]; $_params_ref = $_[2];
   } else {
      $_user_func = $_[2]; $_params_ref = {};
   }

   @_ = ();

   _croak("MCE::foreach: 'input_data' is not specified")
      unless (defined $_input_data);
   _croak("MCE::foreach: 'code_block' is not specified")
      unless (defined $_user_func);

   $_params_ref->{chunk_size} = 1;
   $_params_ref->{input_data} = $_input_data;
   $_params_ref->{user_func}  = $_user_func;

   $self->run(1, $_params_ref);

   return $self;
}

###############################################################################
## ----------------------------------------------------------------------------
## Forseq method.
##
###############################################################################

sub forseq {

   my MCE $self = $_[0]; my $_sequence = $_[1];

   _croak("MCE::forseq: method cannot be called by the worker process")
      if ($self->{_wid});
   _croak("MCE::forseq: method cannot be called while processing")
      if ($self->{_send_cnt});
   _croak("MCE::forseq: method cannot be called while running")
      if ($self->{_total_running});

   my ($_user_func, $_params_ref);

   if (ref $_[2] eq 'HASH') {
      $_user_func = $_[3]; $_params_ref = $_[2];
   } else {
      $_user_func = $_[2]; $_params_ref = {};
   }

   @_ = ();

   _croak("MCE::forseq: 'sequence' is not specified")
      unless (defined $_sequence);
   _croak("MCE::forseq: 'code_block' is not specified")
      unless (defined $_user_func);

   $_params_ref->{chunk_size} = 1;
   $_params_ref->{sequence}   = $_sequence;
   $_params_ref->{user_func}  = $_user_func;

   $self->run(1, $_params_ref);

   return $self;
}

###############################################################################
## ----------------------------------------------------------------------------
## Process method.
##
###############################################################################

sub process {

   my MCE $self = $_[0]; my $_input_data = $_[1]; my $_params_ref = $_[2];

   @_ = ();

   _croak("MCE::process: method cannot be called by the worker process")
      if ($self->{_wid});
   _croak("MCE::process: method cannot be called while processing")
      if ($self->{_send_cnt});
   _croak("MCE::process: method cannot be called while running")
      if ($self->{_total_running});

   ## Set input data.
   if (defined $_input_data) {
      $_params_ref->{input_data} = $_input_data;
   } else {
      _croak("MCE::process: 'input_data' is not specified")
   }

   ## Pass 0 to "not" auto-shutdown after processing.
   $self->run(0, $_params_ref);

   return $self;
}

###############################################################################
## ----------------------------------------------------------------------------
## Restart worker method.
##
###############################################################################

sub restart_worker {

   my MCE $self = $_[0]; my $_wid = $_[1];

   _croak("MCE::restart_worker: method cannot be called by the worker process")
      if ($self->{_wid});

   @_ = ();

   _croak("MCE::restart_worker: 'wid' is not specified")
      unless (defined $_wid);
   _croak("MCE::restart_worker: 'wid' is not valid")
      unless (defined $self->{_state}->[$_wid]);

   my $_params   = $self->{_state}->[$_wid]->{_params};
   my $_task_wid = $self->{_state}->[$_wid]->{_task_wid};
   my $_task_id  = $self->{_state}->[$_wid]->{_task_id};
   my $_task     = $self->{_state}->[$_wid]->{_task};

   my $_use_threads = (defined $_task_id)
      ? $_task->{use_threads} : $self->{use_threads};

   $self->{_task}->[$_task_id]->{_total_running} += 1 if (defined $_task_id);
   $self->{_task}->[$_task_id]->{_total_workers} += 1 if (defined $_task_id);

   $self->{_total_running} += 1;
   $self->{_total_workers} += 1;

   if (defined $_use_threads && $_use_threads == 1) {
      $self->_dispatch_thread($_wid, $_task, $_task_id, $_task_wid, $_params);
   } else {
      $self->_dispatch_child($_wid, $_task, $_task_id, $_task_wid, $_params);
   }

   return;
}

###############################################################################
## ----------------------------------------------------------------------------
## Run method.
##
###############################################################################

sub run {

   my MCE $self = $_[0];

   _croak("MCE::run: method cannot be called by the worker process")
      if ($self->{_wid});

   ## Parse args.
   my ($_auto_shutdown, $_params_ref);

   if (ref $_[1] eq 'HASH') {
      $_auto_shutdown = (defined $_[2]) ? $_[2] : 1;
      $_params_ref    = $_[1];
   }
   else {
      $_auto_shutdown = (defined $_[1]) ? $_[1] : 1;
      $_params_ref    = $_[2];
   }

   @_ = ();

   my $_requires_shutdown = 0;

   ## Unset params if workers have been sent user_data via send.
   $_params_ref = undef if ($self->{_send_cnt});

   ## Set user_func to NOOP if not specified.
   $self->{user_func} = \&_NOOP
      if (!defined $self->{user_func} && !defined $_params_ref->{user_func});

   ## Set user specified params if specified.
   if (defined $_params_ref && ref $_params_ref eq 'HASH') {
      $_requires_shutdown = _sync_params($self, $_params_ref);
      $self->_validate_args();
   }

   ## Shutdown workers if determined by _sync_params or if processing a
   ## scalar reference. Workers need to be restarted in order to pick up
   ## on the new code blocks and/or scalar reference.
   $self->shutdown()
      if ($_requires_shutdown || ref $self->{input_data} eq 'SCALAR');

   ## -------------------------------------------------------------------------

   ## Spawn workers.
   $self->spawn() if ($self->{_spawned} == 0);
   return $self   if ($self->{_total_workers} == 0);

   local $SIG{__DIE__}  = \&_die;
   local $SIG{__WARN__} = \&_warn;

   my ($_input_data, $_input_file, $_input_glob, $_seq);
   my ($_abort_msg, $_first_msg, $_run_mode, $_single_dim);

   $_seq = (defined $self->{user_tasks} && $self->{user_tasks}->[0]->{sequence})
      ? $self->{user_tasks}->[0]->{sequence}
      : $self->{sequence};

   ## Determine run mode for workers.
   if (defined $_seq) {
      my ($_begin, $_end, $_step, $_fmt) = (ref $_seq eq 'ARRAY')
         ? @{ $_seq } : ($_seq->{begin}, $_seq->{end}, $_seq->{step});
      $_run_mode  = 'sequence';
      $_abort_msg = int(($_end - $_begin) / $_step / $self->{chunk_size}) + 1;
      $_first_msg = 0;
   }
   elsif (defined $self->{input_data}) {
      if (ref $self->{input_data} eq 'ARRAY') {      ## Array mode.
         $_run_mode   = 'array';
         $_input_data = $self->{input_data};
         $_input_file = $_input_glob = undef;
         $_single_dim = 1 if (ref $_input_data->[0] eq '');
         $_abort_msg  = 0; ## Flag: Has Data: No
         $_first_msg  = 1; ## Flag: Has Data: Yes
         if (@$_input_data == 0) {
            return $self->shutdown() if ($_auto_shutdown == 1);
         }
      }
      elsif (ref $self->{input_data} eq 'GLOB') {    ## Glob mode.
         $_run_mode   = 'glob';
         $_input_glob = $self->{input_data};
         $_input_data = $_input_file = undef;
         $_abort_msg  = 0; ## Flag: Has Data: No
         $_first_msg  = 1; ## Flag: Has Data: Yes
      }
      elsif (ref $self->{input_data} eq '') {        ## File mode.
         $_run_mode   = 'file';
         $_input_file = $self->{input_data};
         $_input_data = $_input_glob = undef;
         $_abort_msg  = (-s $_input_file) + 1;
         $_first_msg  = 0; ## Begin at offset position
         if ((-s $_input_file) == 0) {
            return $self->shutdown() if ($_auto_shutdown == 1);
         }
      }
      elsif (ref $self->{input_data} eq 'SCALAR') {  ## Memory mode.
         $_run_mode   = 'memory';
         $_input_data = $_input_file = $_input_glob = undef;
         $_abort_msg  = length(${ $self->{input_data} }) + 1;
         $_first_msg  = 0; ## Begin at offset position
         if (length(${ $self->{input_data} }) == 0) {
            return $self->shutdown() if ($_auto_shutdown == 1);
         }
      }
      else {
         _croak("MCE::run: 'input_data' is not valid");
      }
   }
   else {                                            ## Nodata mode.
      $_run_mode  = 'nodata';
      $_abort_msg = undef;
   }

   ## -------------------------------------------------------------------------

   my $_chunk_size    = $self->{chunk_size};
   my $_sequence      = $self->{sequence};
   my $_user_args     = $self->{user_args};
   my $_use_slurpio   = $self->{use_slurpio};
   my $_sess_dir      = $self->{_sess_dir};
   my $_total_workers = $self->{_total_workers};
   my $_send_cnt      = $self->{_send_cnt};

   my $_COM_LOCK;

   ## Begin processing.
   unless ($_send_cnt) {

      my %_params = (
         '_abort_msg'  => $_abort_msg,    '_run_mode'    => $_run_mode,
         '_chunk_size' => $_chunk_size,   '_single_dim'  => $_single_dim,
         '_input_file' => $_input_file,   '_sequence'    => $_sequence,
         '_user_args'  => $_user_args,    '_use_slurpio' => $_use_slurpio
      );
      my %_params_nodata = (
         '_abort_msg'  => undef,          '_run_mode'    => 'nodata',
         '_chunk_size' => $_chunk_size,   '_single_dim'  => $_single_dim,
         '_input_file' => $_input_file,   '_sequence'    => $_sequence,
         '_user_args'  => $_user_args,    '_use_slurpio' => $_use_slurpio
      );

      local $\ = undef; local $/ = $LF;
      lock $_MCE_LOCK if ($_has_threads);            ## Obtain MCE lock.

      my ($_wid, %_task0_wids);

      my $_COM_R_SOCK     = $self->{_com_r_sock};
      my $_submit_delay   = $self->{submit_delay};
      my $_has_user_tasks = (defined $self->{user_tasks});

      my $_frozen_params  = freeze(\%_params);
      my $_frozen_nodata  = freeze(\%_params_nodata) if ($_has_user_tasks);

      if ($_has_user_tasks) { for (1 .. @{ $self->{_state} } - 1) {
         $_task0_wids{$_} = 1 unless ($self->{_state}->[$_]->{_task_id});
      }}

      ## Obtain lock 1 of 2.
      open my $_DAT_LOCK, '+>> :stdio', "$_sess_dir/_dat.lock";
      flock $_DAT_LOCK, LOCK_EX;

      ## Insert the first message into the queue if defined.
      if (defined $_first_msg) {
         local $\ = undef;
         my $_QUE_W_SOCK = $self->{_que_w_sock};
         print $_QUE_W_SOCK pack(QUE_TEMPLATE, 0, $_first_msg);
      }

      ## Submit params data to workers.
      for (1 .. $_total_workers) {
         select(undef, undef, undef, $_submit_delay)
            if ($_submit_delay && $_submit_delay > 0.0);

         print $_COM_R_SOCK $_, $LF; chomp($_wid = <$_COM_R_SOCK>);

         if (!$_has_user_tasks || exists $_task0_wids{$_wid}) {
            print $_COM_R_SOCK length($_frozen_params), $LF, $_frozen_params;
            $self->{_state}->[$_wid]->{_params} = \%_params;
         } else {
            print $_COM_R_SOCK length($_frozen_nodata), $LF, $_frozen_nodata;
            $self->{_state}->[$_wid]->{_params} = \%_params_nodata;
         }

         <$_COM_R_SOCK>;
      }

      ## Obtain lock 2 of 2.
      open $_COM_LOCK, '+>> :stdio', "$_sess_dir/_com.lock";
      flock $_COM_LOCK, LOCK_EX;

      ## Release lock 1 of 2.
      flock $_DAT_LOCK, LOCK_UN;
      select(undef, undef, undef, 0.002);
      close $_DAT_LOCK; undef $_DAT_LOCK;
   }

   ## -------------------------------------------------------------------------

   $self->{_total_exited} = 0;

   if ($_send_cnt) {
      $self->{_total_running} = $_send_cnt;
      $self->{_task}->[0]->{_total_running} = $_send_cnt;
   }
   else {
      $self->{_total_running} = $_total_workers;
      if (defined $self->{user_tasks}) {
         $_->{_total_running} = $_->{_total_workers} for (@{ $self->{_task} });
      }
   }

   ## Call the output function.
   if ($self->{_total_running} > 0) {
      $self->{_abort_msg}  = $_abort_msg;
      $self->{_run_mode}   = $_run_mode;
      $self->{_single_dim} = $_single_dim;

      $self->_output_loop($_input_data, $_input_glob);

      undef $self->{_abort_msg};
      undef $self->{_run_mode};
      undef $self->{_single_dim};
   }

   unless ($_send_cnt) {
      ## Remove the last message from the queue.
      unless ($_run_mode eq 'nodata') {
         unlink "$_sess_dir/_store.db" if ($_run_mode eq 'array');
         if (defined $self->{_que_r_sock}) {
            local $/ = $LF;
            my $_next; my $_QUE_R_SOCK = $self->{_que_r_sock};
            read $_QUE_R_SOCK, $_next, QUE_READ_SIZE;
         }
      }

      ## Release lock 2 of 2.
      flock $_COM_LOCK, LOCK_UN;
      close $_COM_LOCK; undef $_COM_LOCK;
   }

   $self->{_send_cnt} = 0;

   ## Shutdown workers (also if any workers have exited).
   $self->shutdown() if ($_auto_shutdown == 1 || $self->{_total_exited} > 0);

   return $self;
}

###############################################################################
## ----------------------------------------------------------------------------
## Send method.
##
###############################################################################

sub send {

   my MCE $self = $_[0];

   _croak("MCE::send: method cannot be called by the worker process")
      if ($self->{_wid});
   _croak("MCE::send: method cannot be called while running")
      if ($self->{_total_running});

   _croak("MCE::send: method cannot be used with input_data or sequence")
      if (defined $self->{input_data} || defined $self->{sequence});
   _croak("MCE::send: method cannot be used with user_tasks")
      if (defined $self->{user_tasks});

   my $_data_ref;

   if (ref $_[1] eq 'ARRAY' || ref $_[1] eq 'HASH' || ref $_[1] eq 'PDL') {
      $_data_ref = $_[1];
   } else {
      _croak("MCE::send: ARRAY, HASH, or a PDL reference is not specified");
   }

   $self->{_send_cnt} = 0 unless (defined $self->{_send_cnt});

   @_ = ();

   ## -------------------------------------------------------------------------

   ## Spawn workers.
   $self->spawn() if ($self->{_spawned} == 0);

   _croak("MCE::send: Sending greater than # of workers is not allowed")
      if ($self->{_send_cnt} >= $self->{_task}->[0]->{_total_workers});

   local $SIG{__DIE__}  = \&_die;
   local $SIG{__WARN__} = \&_warn;

   ## Begin data submission.
   {
      local $\ = undef; local $/ = $LF;

      my $_COM_R_SOCK   = $self->{_com_r_sock};
      my $_sess_dir     = $self->{_sess_dir};
      my $_submit_delay = $self->{submit_delay};

      my $_frozen_data  = freeze($_data_ref);

      ## Submit data to worker.
      select(undef, undef, undef, $_submit_delay)
         if ($_submit_delay && $_submit_delay > 0.0);

      print $_COM_R_SOCK "_data${LF}";
      <$_COM_R_SOCK>;

      print $_COM_R_SOCK length($_frozen_data), $LF, $_frozen_data;
      <$_COM_R_SOCK>;
   }

   $self->{_send_cnt} += 1;

   return $self;
}

###############################################################################
## ----------------------------------------------------------------------------
## Shutdown method.
##
###############################################################################

sub shutdown {

   my MCE $self = $_[0];

   @_ = ();

   _croak("MCE::shutdown: method cannot be called by the worker process")
      if ($self->{_wid});
   _croak("MCE::shutdown: method cannot be called while processing")
      if ($self->{_send_cnt});
   _croak("MCE::shutdown: method cannot be called while running")
      if ($self->{_total_running});

   ## Return if workers have not been spawned or have already been shutdown.
   return unless ($self->{_spawned});

   ## Wait for workers to complete processing before shutting down.
   $self->run(0) if ($self->{_send_cnt});

   local $SIG{__DIE__}  = \&_die;
   local $SIG{__WARN__} = \&_warn;

   lock $_MCE_LOCK if ($_has_threads);            ## Obtain MCE lock.

   my $_COM_R_SOCK    = $self->{_com_r_sock};
   my $_mce_sid       = $self->{_mce_sid};
   my $_sess_dir      = $self->{_sess_dir};
   my $_total_workers = $self->{_total_workers};

   ## Delete entry.
   delete $_mce_spawned{$_mce_sid};

   ## Notify workers to exit loop.
   local $\ = undef; local $/ = $LF;

   for (1 .. $_total_workers) {
      print $_COM_R_SOCK "_exit${LF}";
      <$_COM_R_SOCK>;
   }

   ## Reap children/threads.
   if ( $self->{_pids} && @{ $self->{_pids} } > 0 ) {
      my $_list = $self->{_pids};
      for my $i (0 .. @$_list) {
         waitpid $_list->[$i], 0 if ($_list->[$i]);
      }
   }
   if ( $self->{_thrs} && @{ $self->{_thrs} } > 0 ) {
      my $_list = $self->{_thrs};
      for my $i (0 .. @$_list) {
         ${ $_list->[$i] }->join() if ($_list->[$i]);
      }
   }

   ## Close sockets.
   shutdown $self->{_out_r_sock}, 0;              ## Output channels
   shutdown $self->{_out_w_sock}, 1;
   shutdown $self->{_que_r_sock}, 0;              ## Queue channels
   shutdown $self->{_que_w_sock}, 1;

   close $self->{_out_r_sock}; undef $self->{_out_r_sock};
   close $self->{_out_w_sock}; undef $self->{_out_w_sock};
   close $self->{_que_r_sock}; undef $self->{_que_r_sock};
   close $self->{_que_w_sock}; undef $self->{_que_w_sock};

   shutdown $self->{_com_r_sock}, 2;              ## Comm channels
   shutdown $self->{_com_w_sock}, 2;
   shutdown $self->{_dat_r_sock}, 2;              ## Data channels
   shutdown $self->{_dat_w_sock}, 2;

   close $self->{_com_r_sock}; undef $self->{_com_r_sock};
   close $self->{_com_w_sock}; undef $self->{_com_w_sock};
   close $self->{_dat_r_sock}; undef $self->{_dat_r_sock};
   close $self->{_dat_w_sock}; undef $self->{_dat_w_sock};

   ## Remove session directory.
   if (defined $_sess_dir) {
      unlink "$_sess_dir/_com.lock";
      unlink "$_sess_dir/_dat.lock";
      unlink "$_sess_dir/_syn.lock";
      rmdir  "$_sess_dir";

      delete $_mce_sess_dir{$_sess_dir};
   }

   ## Reset instance.
   $self->{_pids}   = (); $self->{_thrs}  = (); $self->{_tids} = ();
   $self->{_status} = (); $self->{_state} = (); $self->{_task} = ();

   $self->{_total_running} = $self->{_total_workers} = 0;
   $self->{_total_exited}  = 0;

   $self->{_out_r_sock} = $self->{_out_w_sock} = undef;
   $self->{_que_r_sock} = $self->{_que_w_sock} = undef;
   $self->{_com_r_sock} = $self->{_com_w_sock} = undef;
   $self->{_dat_r_sock} = $self->{_dat_w_sock} = undef;

   $self->{_mce_sid}    = $self->{_mce_tid}    = undef;
   $self->{_task_id}    = $self->{_task_wid}   = 0;
   $self->{_spawned}    = $self->{_wid}        = 0;
   $self->{_sess_dir}   = undef;
   $self->{_send_cnt}   = 0;

   return;
}

###############################################################################
## ----------------------------------------------------------------------------
## Sync method.
##
###############################################################################

sub sync {

   my MCE $self = $_[0];

   @_ = ();

   _croak("MCE::sync: method cannot be called by the manager process")
      unless ($self->{_wid});

   ## Barrier synchronization is supported for task 0 at this time.
   ## Note: Workers are assigned task_id 0 when ommitting user_tasks.

   return if ($self->{_task_id} > 0);

   my $_DAT_W_SOCK = $self->{_dat_w_sock};
   my $_OUT_W_SOCK = $self->{_out_w_sock};

   local $\ = undef; local $/ = $LF;

   ## Notify the manager process.
   flock $_DAT_LOCK, LOCK_EX;
   print $_OUT_W_SOCK OUTPUT_B_SYN, $LF;
   <$_DAT_W_SOCK>;
   flock $_DAT_LOCK, LOCK_UN;

   ## Wait here until all workers (task_id 0) have synced.
   flock $_SYN_LOCK, LOCK_SH;
   flock $_SYN_LOCK, LOCK_UN;

   ## Notify the manager process.
   print $_OUT_W_SOCK OUTPUT_E_SYN, $LF;

   return;
}

###############################################################################
## ----------------------------------------------------------------------------
## Miscellaneous methods.
##    abort exit last next sess_dir task_id task_wid wid chunk_size tmp_dir
##
###############################################################################

## Abort current job.

sub abort {

   my MCE $self = $_[0];

   @_ = ();

   my $_QUE_R_SOCK = $self->{_que_r_sock};
   my $_QUE_W_SOCK = $self->{_que_w_sock};
   my $_OUT_W_SOCK = $self->{_out_w_sock};
   my $_abort_msg  = $self->{_abort_msg};

   if (defined $_abort_msg) {
      local $\ = undef; local $/ = $LF;
      my $_next; read $_QUE_R_SOCK, $_next, QUE_READ_SIZE;
      print $_QUE_W_SOCK pack(QUE_TEMPLATE, 0, $_abort_msg);
      print $_OUT_W_SOCK OUTPUT_W_ABT, $LF if ($self->wid > 0);
   }

   return;
}

## Worker exits from MCE.

sub exit {

   my MCE $self     = $_[0];
   my $_exit_status = (defined $_[1]) ? $_[1] : $?;
   my $_exit_msg    = (defined $_[2]) ? $_[2] : '';
   my $_exit_id     = (defined $_[3]) ? $_[3] : '';

   @_ = ();

   _croak("MCE::exit: method cannot be called by the manager process")
      unless ($self->{_wid});

   delete $_mce_spawned{ $self->{_mce_sid} };

   unless ($self->{_exiting}) {
      $self->{_exiting} = 1;

      my $_DAT_W_SOCK = $self->{_dat_w_sock};
      my $_OUT_W_SOCK = $self->{_out_w_sock};
      my $_task_id    = $self->{_task_id};

      if (defined $_DAT_LOCK) {
         my $_len  =  length($_exit_msg);
         $_exit_id =~ s/[\r\n][\r\n]*/ /mg;

         local $\ = undef;
         flock $_DAT_LOCK, LOCK_EX;

         flock $_DAT_LOCK, LOCK_EX          if ($_is_cygwin);  ## Humm, needed.
         select(undef, undef, undef, 0.001) if ($_is_cygwin);  ## This as well.

         print $_OUT_W_SOCK OUTPUT_W_EXT,  $LF, $_task_id,          $LF;

         print $_DAT_W_SOCK $self->{_wid}, $LF, $self->{_exit_pid}, $LF,
                            $_exit_status, $LF, $_exit_id,          $LF,
                            $_len,         $LF, $_exit_msg;

         flock $_DAT_LOCK, LOCK_UN;
      }
   }

   ## Exit thread/child process.
   $SIG{__DIE__} = $SIG{__WARN__} = sub { };

   close $_DAT_LOCK; undef $_DAT_LOCK;
   close $_COM_LOCK; undef $_COM_LOCK;
   close $_SYN_LOCK; under $_SYN_LOCK;

   threads->exit($_exit_status) if ($_has_threads && threads->can('exit'));

   close STDERR; close STDOUT;
   kill 9, $$ unless ($_is_winperl);

   CORE::exit($_exit_status);
}

## Worker immediately exits the chunking loop.

sub last {

   my MCE $self = $_[0];

   @_ = ();

   _croak("MCE::last: method cannot be called by the manager process")
      unless ($self->{_wid});

   $self->{_last_jmp}() if (defined $self->{_last_jmp});

   return;
}

## Worker starts the next iteration of the chunking loop.

sub next {

   my MCE $self = $_[0];

   @_ = ();

   _croak("MCE::next: method cannot be called by the manager process")
      unless ($self->{_wid});

   $self->{_next_jmp}() if (defined $self->{_next_jmp});

   return;
}

## Return the (Session Dir), (Task ID), (Task Worker ID), or (MCE Worker ID).

sub sess_dir   { my MCE $self = $_[0]; @_ = (); return $self->{_sess_dir};  }
sub task_id    { my MCE $self = $_[0]; @_ = (); return $self->{_task_id};   }
sub task_wid   { my MCE $self = $_[0]; @_ = (); return $self->{_task_wid};  }
sub wid        { my MCE $self = $_[0]; @_ = (); return $self->{_wid};       }

## Return the (Chunk Size) or (Temporary Directory) MCE is using.

sub chunk_size { my MCE $self = $_[0]; @_ = (); return $self->{chunk_size}; }
sub tmp_dir    { my MCE $self = $_[0]; @_ = (); return $self->{tmp_dir};    }

###############################################################################
## ----------------------------------------------------------------------------
## Do & sendto methods for serializing data from workers to main thread.
##
###############################################################################

## Do method. Additional arguments are optional.

sub do {

   my MCE $self = shift; my $_callback = shift;

   _croak("MCE::do: method cannot be called by the manager process")
      unless ($self->{_wid});
   _croak("MCE::do: 'callback' is not specified")
      unless (defined $_callback);

   $_callback = "main::$_callback" if (index($_callback, ':') < 0);

   return _do_callback($self, $_callback, \@_);
}

## Sendto method.

{
   my %_sendto_lkup = (
      'file'   => SENDTO_FILEV1, 'FILE'   => SENDTO_FILEV1,
      'file:'  => SENDTO_FILEV2, 'FILE:'  => SENDTO_FILEV2,
      'stdout' => SENDTO_STDOUT, 'STDOUT' => SENDTO_STDOUT,
      'stderr' => SENDTO_STDERR, 'STDERR' => SENDTO_STDERR
   );

   my $_filev2_regx = qr/^([^:]+:)(.+)/;

   sub sendto {

      my MCE $self = shift; my $_to = shift;

      _croak("MCE::sendto: method cannot be called by the manager process")
         unless ($self->{_wid});

      return unless (defined $_[0]);

      my ($_file, $_id);
      $_id = (exists $_sendto_lkup{$_to}) ? $_sendto_lkup{$_to} : undef;

      if (!defined $_id) {
         if (defined $_to && $_to =~ /$_filev2_regx/o) {
            $_id = (exists $_sendto_lkup{$1}) ? $_sendto_lkup{$1} : undef;
            $_file = $2;
         }
         if (!defined $_id || ($_id == SENDTO_FILEV2 && !defined $_file)) {
            my $_msg  = "\n";
               $_msg .= "MCE::sendto: improper use of method\n";
               $_msg .= "\n";
               $_msg .= "## usage:\n";
               $_msg .= "##    ->sendto(\"stderr\", ...);\n";
               $_msg .= "##    ->sendto(\"stdout\", ...);\n";
               $_msg .= "##    ->sendto(\"file:/path/to/file\", ...);\n";
               $_msg .= "\n";

            _croak($_msg);
         }
      }

      if ($_id == SENDTO_FILEV1) {                ## sendto 'file', $a, $path
         return if (!defined $_[1] || @_ > 2);    ## Please switch to using V2
         $_file = $_[1]; delete $_[1];            ## sendto 'file:/path', $a
         $_id   = SENDTO_FILEV2;
      }

      return _do_send($self, $_id, $_file, @_);
   }
}

###############################################################################
## ----------------------------------------------------------------------------
## Private methods.
##
###############################################################################

sub _croak {

   $SIG{__DIE__}  = \&_die;
   $SIG{__WARN__} = \&_warn;

   $\ = undef; require Carp; goto &Carp::croak;

   return;
}

sub _NOOP { }

sub _die  { MCE::Signal->_die_handler(@_);  }
sub _warn { MCE::Signal->_warn_handler(@_); }

###############################################################################
## ----------------------------------------------------------------------------
## Sync & validation methods.
##
###############################################################################

sub _sync_params {

   my MCE $self = $_[0]; my $_params_ref = $_[1];

   @_ = ();

   die "Private method called" unless (caller)[0]->isa( ref($self) );

   my $_requires_shutdown = 0;

   for (qw( user_begin user_func user_end )) {
      if (defined $_params_ref->{$_}) {
         $self->{$_} = $_params_ref->{$_};
         delete $_params_ref->{$_};
         $_requires_shutdown = 1;
      }
   }

   for (keys %{ $_params_ref }) {
      _croak("MCE::_sync_params: '$_' is not a valid params argument")
         unless (exists $_params_allowed_args{$_});

      $self->{$_} = $_params_ref->{$_};
   }

   return ($self->{_spawned}) ? $_requires_shutdown : 0;
}

sub _validate_args {

   my MCE $_s = $_[0];

   @_ = ();

   die "Private method called" unless (caller)[0]->isa( ref($_s) );

   my $_tag = 'MCE::_validate_args';

   if (defined $_s->{input_data} && ref $_s->{input_data} eq '') {
      _croak("$_tag: '$_s->{input_data}' does not exist")
         unless (-e $_s->{input_data});
   }

   _croak("$_tag: 'use_slurpio' is not 0 or 1")
      if ($_s->{use_slurpio} && $_s->{use_slurpio} !~ /\A[01]\z/);
   _croak("$_tag: 'job_delay' is not valid")
      if ($_s->{job_delay} && $_s->{job_delay} !~ /\A[\d\.]+\z/);
   _croak("$_tag: 'spawn_delay' is not valid")
      if ($_s->{spawn_delay} && $_s->{spawn_delay} !~ /\A[\d\.]+\z/);
   _croak("$_tag: 'submit_delay' is not valid")
      if ($_s->{submit_delay} && $_s->{submit_delay} !~ /\A[\d\.]+\z/);

   _croak("$_tag: 'on_post_exit' is not a CODE reference")
      if ($_s->{on_post_exit} && ref $_s->{on_post_exit} ne 'CODE');
   _croak("$_tag: 'on_post_run' is not a CODE reference")
      if ($_s->{on_post_run} && ref $_s->{on_post_run} ne 'CODE');
   _croak("$_tag: 'user_error' is not a CODE reference")
      if ($_s->{user_error} && ref $_s->{user_error} ne 'CODE');
   _croak("$_tag: 'user_output' is not a CODE reference")
      if ($_s->{user_output} && ref $_s->{user_output} ne 'CODE');

   _croak("$_tag: 'flush_file' is not 0 or 1")
      if ($_s->{flush_file} && $_s->{flush_file} !~ /\A[01]\z/);
   _croak("$_tag: 'flush_stderr' is not 0 or 1")
      if ($_s->{flush_stderr} && $_s->{flush_stderr} !~ /\A[01]\z/);
   _croak("$_tag: 'flush_stdout' is not 0 or 1")
      if ($_s->{flush_stdout} && $_s->{flush_stdout} !~ /\A[01]\z/);

   $_s->_validate_args_s();

   if (defined $_s->{user_tasks}) {
      for my $_t (@{ $_s->{user_tasks} }) {
         $_s->_validate_args_s($_t);

         _croak("$_tag: 'task_end' is not a CODE reference")
            if ($_t->{task_end} && ref $_t->{task_end} ne 'CODE');
      }
   }

   return;
}

sub _validate_args_s {

   my MCE $self = $_[0];
   my $_s       = $_[1] || $self;

   @_ = ();

   die "Private method called" unless (caller)[0]->isa( ref($self) );

   my $_tag = 'MCE::_validate_args_s';

   _croak("$_tag: 'chunk_size' is not valid")
      if (defined $_s->{chunk_size} && (
         $_s->{chunk_size} !~ /\A\d+\z/ or $_s->{chunk_size} == 0
      ));
   _croak("$_tag: 'max_workers' is not valid")
      if (defined $_s->{max_workers} && (
         $_s->{max_workers} !~ /\A\d+\z/ or $_s->{max_workers} == 0
      ));

   _croak("$_tag: 'RS' is not valid")
      if ($_s->{RS} && ref $_s->{RS} ne '');
   _croak("$_tag: 'use_threads' is not 0 or 1")
      if ($_s->{use_threads} && $_s->{use_threads} !~ /\A[01]\z/);
   _croak("$_tag: 'user_begin' is not a CODE reference")
      if ($_s->{user_begin} && ref $_s->{user_begin} ne 'CODE');
   _croak("$_tag: 'user_func' is not a CODE reference")
      if ($_s->{user_func} && ref $_s->{user_func} ne 'CODE');
   _croak("$_tag: 'user_end' is not a CODE reference")
      if ($_s->{user_end} && ref $_s->{user_end} ne 'CODE');

   if (defined $_s->{sequence}) {
      my $_seq = $_s->{sequence};

      _croak("$_tag: cannot specify both 'input_data' and 'sequence'")
         if (defined $self->{input_data});

      if (ref $_seq eq 'ARRAY') {
         my ($_begin, $_end, $_step, $_fmt) = @{ $_seq };
         $_seq = {
            begin => $_begin, end => $_end, step => $_step, format => $_fmt
         };
      }
      else {
         _croak("$_tag: 'sequence' is not a HASH or ARRAY reference")
            if (ref $_seq ne 'HASH');
      }

      _croak("$_tag: 'begin' is not defined for sequence")
         unless (defined $_seq->{begin});
      _croak("$_tag: 'end' is not defined for sequence")
         unless (defined $_seq->{end});

      for (qw(begin end step)) {
         _croak("$_tag: '$_' is not valid for sequence")
            if (defined $_seq->{$_} && (
               $_seq->{$_} eq '' || $_seq->{$_} !~ /\A-?\d*\.?\d*\z/
            ));
      }

      unless (defined $_seq->{step}) {
         $_seq->{step} = ($_seq->{begin} < $_seq->{end}) ? 1 : -1;
         if (ref $_s->{sequence} eq 'ARRAY') {
            $_s->{sequence}->[2] = $_seq->{step};
         }
      }

      if ( ($_seq->{step} < 0 && $_seq->{begin} < $_seq->{end}) ||
           ($_seq->{step} > 0 && $_seq->{begin} > $_seq->{end}) ||
           ($_seq->{step} == 0)
      ) {
         _croak("$_tag: impossible 'step' size for sequence");
      }
   }

   return;
}

###############################################################################
## ----------------------------------------------------------------------------
## Internal do & send related functions for serializing data to destination.
##
###############################################################################

{
   my ($_data_ref, $_dest, $_value, $_len, $_send_init_called);
   my ($_sess_dir, $_DAT_W_SOCK, $_OUT_W_SOCK, $_want_id);

   ## Create array structure containing various send functions.
   my @_dest_function = ();

   $_dest_function[SENDTO_FILEV2] = sub {

      return unless (defined $_value);

      local $\ = undef;                           ## Content >> File
      $_len = length(${ $_[0] });

      flock $_DAT_LOCK, LOCK_EX;
      print $_OUT_W_SOCK OUTPUT_S_FLE, $LF;
      print $_DAT_W_SOCK "$_value${LF}$_len${LF}", ${ $_[0] };

      flock $_DAT_LOCK, LOCK_UN;
      return;
   };

   $_dest_function[SENDTO_STDOUT] = sub {

      local $\ = undef;                           ## Content >> STDOUT
      $_len = length(${ $_[0] });

      flock $_DAT_LOCK, LOCK_EX;
      print $_OUT_W_SOCK OUTPUT_S_OUT, $LF;
      print $_DAT_W_SOCK "$_len${LF}", ${ $_[0] };

      flock $_DAT_LOCK, LOCK_UN;
      return;
   };

   $_dest_function[SENDTO_STDERR] = sub {

      local $\ = undef;                           ## Content >> STDERR
      $_len = length(${ $_[0] });

      flock $_DAT_LOCK, LOCK_EX;
      print $_OUT_W_SOCK OUTPUT_S_ERR, $LF;
      print $_DAT_W_SOCK "$_len${LF}", ${ $_[0] };

      flock $_DAT_LOCK, LOCK_UN;
      return;
   };

   ## -------------------------------------------------------------------------

   sub _do_callback {

      my MCE $self = $_[0]; $_value = $_[1]; $_data_ref = $_[2];

      @_ = ();

      die "Improper use of function call" unless ($_send_init_called);

      local $\ = undef; my $_buffer;

      unless (defined wantarray) {
         $_want_id = WANTS_UNDEFINE;
      } elsif (wantarray) {
         $_want_id = WANTS_ARRAY;
      } else {
         $_want_id = WANTS_SCALAR;
      }

      ## Crossover: Send arguments

      if (@$_data_ref > 0) {                      ## Multiple Args >> Callback
         if (@$_data_ref > 1 || ref $_data_ref->[0]) {
            $_buffer = freeze($_data_ref); $_len = length($_buffer);

            flock $_DAT_LOCK, LOCK_EX;
            print $_OUT_W_SOCK OUTPUT_A_CBK, $LF;
            print $_DAT_W_SOCK "$_want_id${LF}$_value${LF}";
            print $_DAT_W_SOCK "$_len${LF}", $_buffer;

            undef $_buffer;
         }
         else {                                   ## Scalar >> Callback
            $_len = length($_data_ref->[0]);

            flock $_DAT_LOCK, LOCK_EX;
            print $_OUT_W_SOCK OUTPUT_S_CBK, $LF;
            print $_DAT_W_SOCK "$_want_id${LF}$_value${LF}";
            print $_DAT_W_SOCK "$_len${LF}", $_data_ref->[0];
         }
      }
      else {                                      ## No Args >> Callback
         flock $_DAT_LOCK, LOCK_EX;
         print $_OUT_W_SOCK OUTPUT_N_CBK, $LF;
         print $_DAT_W_SOCK "$_want_id${LF}$_value${LF}";
      }

      $_data_ref = '';

      ## Crossover: Receive return value

      if ($_want_id == WANTS_UNDEFINE) {
         flock $_DAT_LOCK, LOCK_UN;
         return;
      }
      elsif ($_want_id == WANTS_ARRAY) {
         local $/ = $LF;

         chomp($_len = <$_DAT_W_SOCK>);
         read $_DAT_W_SOCK, $_buffer, $_len;

         flock $_DAT_LOCK, LOCK_UN;
         return @{ thaw($_buffer) };
      }
      else {
         local $/ = $LF;

         chomp($_want_id = <$_DAT_W_SOCK>);
         chomp($_len     = <$_DAT_W_SOCK>);
         read $_DAT_W_SOCK, $_buffer, $_len;

         flock $_DAT_LOCK, LOCK_UN;
         return $_buffer if ($_want_id == WANTS_SCALAR);
         return thaw($_buffer);
      }
   }

   ## -------------------------------------------------------------------------

   sub _do_send_init {

      my MCE $self = $_[0];

      @_ = ();

      die "Private method called" unless (caller)[0]->isa( ref($self) );

      $_sess_dir   = $self->{_sess_dir};
      $_OUT_W_SOCK = $self->{_out_w_sock};
      $_DAT_W_SOCK = $self->{_dat_w_sock};

      $_send_init_called = 1;

      return;
   }

   sub _do_send {

      my MCE $self = shift; $_dest = shift; $_value = shift;

      die "Improper use of function call" unless ($_send_init_called);

      my $_buffer;

      if (@_ > 1) {
         $_buffer = join('', @_);
         return $_dest_function[$_dest](\$_buffer);
      }
      elsif (my $_ref = ref $_[0]) {
         if ($_ref eq 'SCALAR') {
            return $_dest_function[$_dest]($_[0]);
         } elsif ($_ref eq 'ARRAY') {
            $_buffer = join('', @{ $_[0] });
         } elsif ($_ref eq 'HASH') {
            $_buffer = join('', %{ $_[0] });
         } else {
            $_buffer = join('', @_);
         }
         return $_dest_function[$_dest](\$_buffer);
      }
      else {
         return $_dest_function[$_dest](\$_[0]);
      }
   }
}

###############################################################################
## ----------------------------------------------------------------------------
## Process output.
##
## Awaits data from workers. Calls user_output function if specified.
## Otherwise, processes output internally.
##
## The send related functions tag the output. The hash structure below
## is driven by a hash key.
##
###############################################################################

{
   my ($_value, $_want_id, $_input_data, $_eof_flag, $_aborted);
   my ($_user_error, $_user_output, $_flush_file, $self);
   my ($_callback, $_file, %_sendto_fhs, $_len, $_chunk_id);

   my ($_DAT_R_SOCK, $_OUT_R_SOCK, $_MCE_STDERR, $_MCE_STDOUT);
   my ($_RS, $_I_SEP, $_O_SEP, $_input_glob, $_chunk_size);
   my ($_input_size, $_offset_pos, $_single_dim, $_use_slurpio);

   my ($_has_user_tasks, $_on_post_exit, $_on_post_run, $_task_id);
   my ($_exit_wid, $_exit_pid, $_exit_status, $_exit_id, $_sync_cnt);

   ## Create hash structure containing various output functions.
   my %_output_function = (

      OUTPUT_W_ABT.$LF => sub {                   ## Worker has aborted

         $_aborted = 1;

         return;
      },

      OUTPUT_W_DNE.$LF => sub {                   ## Worker has completed
         chomp($_task_id = <$_OUT_R_SOCK>);

         $self->{_total_running} -= 1;

         ## Call on task_end if the last worker for the task.
         if ($_has_user_tasks && $_task_id >= 0) {
            $self->{_task}->[$_task_id]->{_total_running} -= 1;

            unless ($self->{_task}->[$_task_id]->{_total_running}) {
               if (defined $self->{user_tasks}->[$_task_id]->{task_end}) {
                  $self->{user_tasks}->[$_task_id]->{task_end}();
               }
            }
         }

         return;
      },

      ## ----------------------------------------------------------------------

      OUTPUT_W_EXT.$LF => sub {                   ## Worker has exited
         chomp($_task_id = <$_OUT_R_SOCK>);

         $self->{_total_exited}  += 1;
         $self->{_total_running} -= 1;
         $self->{_total_workers} -= 1;

         if ($_has_user_tasks && $_task_id >= 0) {
            $self->{_task}->[$_task_id]->{_total_running} -= 1;
            $self->{_task}->[$_task_id]->{_total_workers} -= 1;
         }

         my $_exit_msg = '';

         chomp($_exit_wid    = <$_DAT_R_SOCK>);
         chomp($_exit_pid    = <$_DAT_R_SOCK>);
         chomp($_exit_status = <$_DAT_R_SOCK>);
         chomp($_exit_id     = <$_DAT_R_SOCK>);
         chomp($_len         = <$_DAT_R_SOCK>);

         read $_DAT_R_SOCK, $_exit_msg, $_len if ($_len);

         ## Reap child/thread. Note: Win32 uses negative PIDs.
         if ($_exit_pid =~ /^PID_(-?\d+)/) {
            my $_pid = $1; my $_list = $self->{_pids};
            for my $i (0 .. @$_list) {
               if ($_list->[$i] && $_list->[$i] == $_pid) {
                  waitpid $_pid, 0;
                  $self->{_pids}->[$i] = undef;
                  last;
               }
            }
         }
         elsif ($_exit_pid =~ /^TID_(\d+)/) {
            my $_tid = $1; my $_list = $self->{_tids};
            for my $i (0 .. @$_list) {
               if ($_list->[$i] && $_list->[$i] == $_tid) {
                  ${ $self->{_thrs}->[$i] }->join();
                  $self->{_thrs}->[$i] = undef;
                  $self->{_tids}->[$i] = undef;
                  last;
               }
            }
         }

         ## Call on_post_exit callback if defined. Otherwise, append status
         ## information if on_post_run is defined for later retrieval.
         if (defined $_on_post_exit) {
            $_on_post_exit->($self, {
               wid => $_exit_wid, pid => $_exit_pid, status => $_exit_status,
               msg => $_exit_msg, id  => $_exit_id
            });
         }
         elsif (defined $_on_post_run) {
            push @{ $self->{_status} }, {
               wid => $_exit_wid, pid => $_exit_pid, status => $_exit_status,
               msg => $_exit_msg, id  => $_exit_id
            };
         }

         ## Call on task_end if the last worker for the task.
         if ($_has_user_tasks && $_task_id >= 0) {
            unless ($self->{_task}->[$_task_id]->{_total_running}) {
               if (defined $self->{user_tasks}->[$_task_id]->{task_end}) {
                  $self->{user_tasks}->[$_task_id]->{task_end}();
               }
            }
         }

         return;
      },

      ## ----------------------------------------------------------------------

      OUTPUT_A_ARY.$LF => sub {                   ## Array << Array
         my $_buffer;

         if ($_offset_pos >= $_input_size || $_aborted) {
            local $\ = undef;
            print $_DAT_R_SOCK "0${LF}";
            return;
         }

         if ($_single_dim && $_chunk_size == 1) {
            $_buffer = $_input_data->[$_offset_pos];
         }
         else {
            if ($_offset_pos + $_chunk_size - 1 < $_input_size) {
               $_buffer = freeze( [ @$_input_data[
                  $_offset_pos .. $_offset_pos + $_chunk_size - 1
               ] ] );
            }
            else {
               $_buffer = freeze( [ @$_input_data[
                  $_offset_pos .. $_input_size - 1
               ] ] );
            }
         }

         local $\ = undef; $_len = length($_buffer);
         print $_DAT_R_SOCK $_len, $LF, (++$_chunk_id), $LF, $_buffer;
         $_offset_pos += $_chunk_size;

         return;
      },

      ## ----------------------------------------------------------------------

      OUTPUT_S_GLB.$LF => sub {                   ## Scalar << Glob FH
         my $_buffer;

         ## The logic below honors ('Ctrl/Z' in Windows, 'Ctrl/D' in Unix)
         ## when reading from standard input. No output will be lost as
         ## far as what was previously read into the buffer.

         if ($_eof_flag || $_aborted) {
            local $\ = undef; print $_DAT_R_SOCK "0${LF}";
            return;
         }

         {
            local $/ = $_RS;

            if ($_chunk_size <= MAX_RECS_SIZE) {
               for (1 .. $_chunk_size) {
                  if (defined($_ = <$_input_glob>)) {
                     $_buffer .= $_; next;
                  }
                  $_eof_flag = 1; last;
               }
            }
            else {
               if (read($_input_glob, $_buffer, $_chunk_size) == $_chunk_size) {
                  if (defined($_ = <$_input_glob>)) {
                     $_buffer .= $_;
                  }
                  else {
                     $_eof_flag = 1;
                  }
               }
            }
         }

         local $\ = undef; $_len = length($_buffer);

         print $_DAT_R_SOCK ($_len)
            ? $_len . $LF . (++$_chunk_id) . $LF . $_buffer
            : '0' . $LF;

         return;
      },

      ## ----------------------------------------------------------------------

      OUTPUT_A_CBK.$LF => sub {                   ## Callback (/w arguments)
         my $_buffer;

         chomp($_want_id  = <$_DAT_R_SOCK>);
         chomp($_callback = <$_DAT_R_SOCK>);
         chomp($_len      = <$_DAT_R_SOCK>);

         read $_DAT_R_SOCK, $_buffer, $_len;
         my $_data_ref = thaw($_buffer);

         undef $_buffer;

         local $\ = $_O_SEP; local $/ = $_I_SEP;
         no strict 'refs';

         if ($_want_id == WANTS_UNDEFINE) {
            $_callback->(@{ $_data_ref });
         }
         elsif ($_want_id == WANTS_ARRAY) {
            my @_ret_a = $_callback->(@{ $_data_ref });
            $_buffer = freeze(\@_ret_a);
            local $\ = undef; $_len = length($_buffer);
            print $_DAT_R_SOCK "$_len${LF}", $_buffer;
         }
         else {
            my $_ret_s = $_callback->(@{ $_data_ref });
            unless (ref $_ret_s) {
               local $\ = undef; $_len = length($_ret_s);
               print $_DAT_R_SOCK WANTS_SCALAR, "${LF}$_len${LF}", $_ret_s;
            }
            else {
               $_buffer = freeze($_ret_s);
               local $\ = undef; $_len = length($_buffer);
               print $_DAT_R_SOCK WANTS_REFERENCE, "${LF}$_len${LF}", $_buffer;
            }
         }

         return;
      },

      ## ----------------------------------------------------------------------

      OUTPUT_N_CBK.$LF => sub {                   ## Callback (no arguments)
         my $_buffer;

         chomp($_want_id  = <$_DAT_R_SOCK>);
         chomp($_callback = <$_DAT_R_SOCK>);

         local $\ = $_O_SEP; local $/ = $_I_SEP;
         no strict 'refs';

         if ($_want_id == WANTS_UNDEFINE) {
            $_callback->();
         }
         elsif ($_want_id == WANTS_ARRAY) {
            my @_ret_a = $_callback->();
            $_buffer = freeze(\@_ret_a);
            local $\ = undef; $_len = length($_buffer);
            print $_DAT_R_SOCK "$_len${LF}", $_buffer;
         }
         else {
            my $_ret_s = $_callback->();
            unless (ref $_ret_s) {
               local $\ = undef; $_len = length($_ret_s);
               print $_DAT_R_SOCK WANTS_SCALAR, "${LF}$_len${LF}", $_ret_s;
            }
            else {
               $_buffer = freeze($_ret_s);
               local $\ = undef; $_len = length($_buffer);
               print $_DAT_R_SOCK WANTS_REFERENCE, "${LF}$_len${LF}", $_buffer;
            }
         }

         return;
      },

      ## ----------------------------------------------------------------------

      OUTPUT_S_CBK.$LF => sub {                   ## Callback (1 scalar arg)
         my $_buffer;

         chomp($_want_id  = <$_DAT_R_SOCK>);
         chomp($_callback = <$_DAT_R_SOCK>);
         chomp($_len      = <$_DAT_R_SOCK>);

         read $_DAT_R_SOCK, $_buffer, $_len;

         local $\ = $_O_SEP; local $/ = $_I_SEP;
         no strict 'refs';

         if ($_want_id == WANTS_UNDEFINE) {
            $_callback->($_buffer);
         }
         elsif ($_want_id == WANTS_ARRAY) {
            my @_ret_a = $_callback->($_buffer);
            $_buffer = freeze(\@_ret_a);
            local $\ = undef; $_len = length($_buffer);
            print $_DAT_R_SOCK "$_len${LF}", $_buffer;
         }
         else {
            my $_ret_s = $_callback->($_buffer);
            unless (ref $_ret_s) {
               local $\ = undef; $_len = length($_ret_s);
               print $_DAT_R_SOCK WANTS_SCALAR, "${LF}$_len${LF}", $_ret_s;
            }
            else {
               $_buffer = freeze($_ret_s);
               local $\ = undef; $_len = length($_buffer);
               print $_DAT_R_SOCK WANTS_REFERENCE, "${LF}$_len${LF}", $_buffer;
            }
         }

         return;
      },

      ## ----------------------------------------------------------------------

      OUTPUT_S_OUT.$LF => sub {                   ## Scalar >> STDOUT
         my $_buffer;

         chomp($_len = <$_DAT_R_SOCK>);
         read $_DAT_R_SOCK, $_buffer, $_len;

         if (defined $_user_output) {
            $_user_output->($_buffer);
         }
         else {
            print $_MCE_STDOUT $_buffer;
         }

         return;
      },

      OUTPUT_S_ERR.$LF => sub {                   ## Scalar >> STDERR
         my $_buffer;

         chomp($_len = <$_DAT_R_SOCK>);
         read $_DAT_R_SOCK, $_buffer, $_len;

         if (defined $_user_error) {
            $_user_error->($_buffer);
         }
         else {
            print $_MCE_STDERR $_buffer;
         }

         return;
      },

      OUTPUT_S_FLE.$LF => sub {                   ## Scalar >> File
         my ($_buffer, $_OUT_FILE);

         chomp($_file = <$_DAT_R_SOCK>);
         chomp($_len  = <$_DAT_R_SOCK>);

         read $_DAT_R_SOCK, $_buffer, $_len;

         unless (exists $_sendto_fhs{$_file}) {
            open    $_sendto_fhs{$_file}, '>>', $_file or die "$_file: $!\n";
            binmode $_sendto_fhs{$_file};

            ## Select new FH, turn on autoflush, restore the old FH.
            ## IO::Handle is too large just to call autoflush(1).
            select((select($_sendto_fhs{$_file}), $| = 1)[0])
               if ($_flush_file);
         }

         $_OUT_FILE = $_sendto_fhs{$_file};
         print $_OUT_FILE $_buffer;

         return;
      },

      ## ----------------------------------------------------------------------

      OUTPUT_B_SYN.$LF => sub {                   ## Worker barrier sync - beg

         local $\ = undef;

         if (!defined $_sync_cnt || $_sync_cnt == 0) {
            flock $_SYN_LOCK, LOCK_EX;
            $_sync_cnt = 0;
         }

         print $_DAT_R_SOCK $LF;

         if (++$_sync_cnt == $self->{_total_running}) {
            flock $_DAT_LOCK, LOCK_EX;
            flock $_SYN_LOCK, LOCK_UN;
         }

         return;
      },

      OUTPUT_E_SYN.$LF => sub {                   ## Worker barrier sync - end

         if (--$_sync_cnt == 0) {
            flock $_DAT_LOCK, LOCK_UN;
         }

         return;
      }

   );

   ## -------------------------------------------------------------------------

   sub _output_loop {

      $self = $_[0]; $_input_data = $_[1]; $_input_glob = $_[2];

      @_ = ();

      die "Private method called" unless (caller)[0]->isa( ref($self) );

      $_on_post_exit = $self->{on_post_exit};
      $_on_post_run  = $self->{on_post_run};
      $_chunk_size   = $self->{chunk_size};
      $_flush_file   = $self->{flush_file};
      $_use_slurpio  = $self->{use_slurpio};
      $_user_output  = $self->{user_output};
      $_user_error   = $self->{user_error};
      $_single_dim   = $self->{_single_dim};

      $_has_user_tasks = (defined $self->{user_tasks});
      $_aborted = $_chunk_id = $_eof_flag = 0;

      if (defined $_input_data && ref $_input_data eq 'ARRAY') {
         $_input_size = @$_input_data;
         $_offset_pos = 0;
      }
      else {
         $_input_size = $_offset_pos = 0;
      }

      ## ----------------------------------------------------------------------

      ## Set STDOUT/STDERR to user parameters.
      if (defined $self->{stdout_file}) {
         open $_MCE_STDOUT, '>>', $self->{stdout_file}
            or die $self->{stdout_file} . ": $!\n";
         binmode $_MCE_STDOUT;
      }
      else {
         open $_MCE_STDOUT, ">&=STDOUT";
         binmode $_MCE_STDOUT;
      }

      if (defined $self->{stderr_file}) {
         open $_MCE_STDERR, '>>', $self->{stderr_file}
            or die $self->{stderr_file} . ": $!\n";
         binmode $_MCE_STDERR;
      }
      else {
         open $_MCE_STDERR, ">&=STDERR";
         binmode $_MCE_STDERR;
      }

      ## Make MCE_STDOUT the default handle.
      ## Flush STDERR/STDOUT handles if requested.
      my $_old_hndl = select $_MCE_STDOUT;
      $| = 1 if ($self->{flush_stdout});

      if ($self->{flush_stderr}) {
         select $_MCE_STDERR; $| = 1;
         select $_MCE_STDOUT;
      }

      ## ----------------------------------------------------------------------

      ## Output event loop.
      $_OUT_R_SOCK = $self->{_out_r_sock};        ## For serialized reads
      $_DAT_R_SOCK = $self->{_dat_r_sock};

      $_RS    = $self->{RS} || $/;
      $_O_SEP = $\; local $\ = undef;
      $_I_SEP = $/; local $/ = $LF;

      ## Call hash function if output value is a hash key.
      ## Exit loop when all workers have completed or ended.
      my $_func;

      open $_DAT_LOCK, '+>> :stdio', $self->{_sess_dir} . '/_dat.lock';
      open $_SYN_LOCK, '+>> :stdio', $self->{_sess_dir} . '/_syn.lock';

      while (1) {
         $_func = <$_OUT_R_SOCK>;
         next unless (defined $_func);

         if (exists $_output_function{$_func}) {
            $_output_function{$_func}();
            last unless ($self->{_total_running});
         }
      }

      close $_SYN_LOCK; undef $_SYN_LOCK;
      close $_DAT_LOCK; undef $_DAT_LOCK;

      ## Call on_post_run callback.
      $_on_post_run->($self, $self->{_status}) if (defined $_on_post_run);

      ## Close opened sendto file handles.
      for (keys %_sendto_fhs) {
         close  $_sendto_fhs{$_};
         undef  $_sendto_fhs{$_};
         delete $_sendto_fhs{$_};
      }

      ## Restore the default handle. Close MCE STDOUT/STDERR handles.
      select $_old_hndl;

      close $_MCE_STDOUT; undef $_MCE_STDOUT;
      close $_MCE_STDERR; undef $_MCE_STDERR;

      return;
   }
}

###############################################################################
## ----------------------------------------------------------------------------
## Sync buffer to array.
##
###############################################################################

sub _sync_buffer_to_array {

   my $_buffer_ref = $_[0]; my $_array_ref = $_[1]; my $_RS = $_[2];
   my $_cnt = 0;

   local $/ = $_RS;

   open my $_MEM_FILE, '<', $_buffer_ref;
   binmode $_MEM_FILE;
   $_array_ref->[$_cnt++] = $_ while (<$_MEM_FILE>);
   close $_MEM_FILE; undef $_MEM_FILE;

   delete @{ $_array_ref }[$_cnt .. @$_array_ref - 1]
      if ($_cnt < @$_array_ref);

   return;
}

###############################################################################
## ----------------------------------------------------------------------------
## Worker process -- Read handle.
##
###############################################################################

sub _worker_read_handle {

   my MCE $self = $_[0]; my $_proc_type = $_[1]; my $_input_data = $_[2];

   @_ = ();

   die "Private method called" unless (caller)[0]->isa( ref($self) );

   my $_many_wrks   = ($self->{max_workers} > 1) ? 1 : 0;
   my $_QUE_R_SOCK  = $self->{_que_r_sock};
   my $_QUE_W_SOCK  = $self->{_que_w_sock};
   my $_chunk_size  = $self->{chunk_size};
   my $_use_slurpio = $self->{use_slurpio};
   my $_user_func   = $self->{user_func};
   my $_RS          = $self->{RS} || $/;

   my ($_data_size, $_next, $_chunk_id, $_offset_pos, $_IN_FILE);
   my @_records = (); $_chunk_id = $_offset_pos = 0;

   $_data_size = ($_proc_type == READ_MEMORY)
      ? length($$_input_data) : -s $_input_data;

   if ($_chunk_size <= MAX_RECS_SIZE || $_proc_type == READ_MEMORY) {
      open    $_IN_FILE, '<', $_input_data or die "$_input_data: $!\n";
      binmode $_IN_FILE;
   }
   else {
      sysopen $_IN_FILE, $_input_data, O_RDONLY or die "$_input_data: $!\n";
   }

   ## -------------------------------------------------------------------------

   $self->{_next_jmp} = sub { goto _WORKER_READ_HANDLE__NEXT; };
   $self->{_last_jmp} = sub { goto _WORKER_READ_HANDLE__LAST; };

   _WORKER_READ_HANDLE__NEXT:

   while (1) {

      ## Don't declare $_buffer with other vars above, instead it's done here.
      ## Doing so will fail with Perl 5.8.0 under Solaris 5.10 on large files.

      my $_buffer;

      ## Obtain the next chunk_id and offset position.
      if ($_many_wrks) {
         local $\ = undef; local $/ = $LF;

         read $_QUE_R_SOCK, $_next, QUE_READ_SIZE;
         ($_chunk_id, $_offset_pos) = unpack(QUE_TEMPLATE, $_next);

         if ($_offset_pos >= $_data_size) {
            print $_QUE_W_SOCK pack(QUE_TEMPLATE, 0, $_offset_pos);
            close $_IN_FILE; undef $_IN_FILE;
            return;
         }
      }
      elsif (eof $_IN_FILE) {
         close $_IN_FILE; undef $_IN_FILE;
         return;
      }

      $_chunk_id++;

      ## Read data.
      if ($_chunk_size <= MAX_RECS_SIZE) {        ## One or many records.
         local $/ = $_RS;
         seek $_IN_FILE, $_offset_pos, 0 if ($_many_wrks);
         if ($_chunk_size == 1) {
            $_buffer = <$_IN_FILE>;
         }
         else {
            if ($_use_slurpio) {
               $_buffer .= <$_IN_FILE> for (0 .. $_chunk_size - 1);
            }
            else {
               for (0 .. $_chunk_size - 1) {
                  $_records[$_] = <$_IN_FILE>;
                  unless (defined $_records[$_]) {
                     delete @_records[$_ .. $_chunk_size - 1];
                     last;
                  }
               }
            }
         }
      }
      else {                                      ## Large chunk.
         local $/ = $_RS;
         if ($_proc_type == READ_MEMORY) {
            seek $_IN_FILE, $_offset_pos, 0 if ($_many_wrks);
            if (read($_IN_FILE, $_buffer, $_chunk_size) == $_chunk_size) {
               $_buffer .= <$_IN_FILE>;
            }
         }
         else {
            sysseek $_IN_FILE, $_offset_pos, 0;
            if (sysread($_IN_FILE, $_buffer, $_chunk_size) == $_chunk_size) {
               seek $_IN_FILE, sysseek($_IN_FILE, 0, 1), 0;
               $_buffer .= <$_IN_FILE>;
            }
            else {
               seek $_IN_FILE, sysseek($_IN_FILE, 0, 1), 0;
            }
            $_offset_pos = tell $_IN_FILE unless ($_many_wrks);
         }
      }

      ## Append the next offset position into the queue.
      if ($_many_wrks) {
         local $\ = undef;
         print $_QUE_W_SOCK pack(QUE_TEMPLATE, $_chunk_id, tell $_IN_FILE);
      }

      ## Call user function.
      if ($_use_slurpio) {
         $_user_func->($self, \$_buffer, $_chunk_id);
      }
      else {
         if ($_chunk_size == 1) {
            $_user_func->($self, [ $_buffer ], $_chunk_id);
         }
         else {
            if ($_chunk_size > MAX_RECS_SIZE) {
               _sync_buffer_to_array(\$_buffer, \@_records, $_RS);
            }
            $_user_func->($self, \@_records, $_chunk_id);
         }
      }
   }

   _WORKER_READ_HANDLE__LAST:

   close $_IN_FILE; undef $_IN_FILE;

   return;
}

###############################################################################
## ----------------------------------------------------------------------------
## Worker process -- Request chunk.
##
###############################################################################

sub _worker_request_chunk {

   my MCE $self = $_[0]; my $_proc_type = $_[1];

   @_ = ();

   die "Private method called" unless (caller)[0]->isa( ref($self) );

   my $_OUT_W_SOCK  = $self->{_out_w_sock};
   my $_DAT_W_SOCK  = $self->{_dat_w_sock};
   my $_single_dim  = $self->{_single_dim};
   my $_chunk_size  = $self->{chunk_size};
   my $_use_slurpio = $self->{use_slurpio};
   my $_user_func   = $self->{user_func};
   my $_RS          = $self->{RS} || $/;

   my ($_next, $_chunk_id, $_len, $_chunk_ref);
   my ($_output_tag, @_records);

   if ($_proc_type == REQUEST_ARRAY) {
      $_output_tag = OUTPUT_A_ARY;
   }
   else {
      $_output_tag = OUTPUT_S_GLB;
      @_records    = ();
   }

   ## -------------------------------------------------------------------------

   $self->{_next_jmp} = sub { goto _WORKER_REQUEST_CHUNK__NEXT; };
   $self->{_last_jmp} = sub { goto _WORKER_REQUEST_CHUNK__LAST; };

   _WORKER_REQUEST_CHUNK__NEXT:

   while (1) {

      ## Don't declare $_buffer with other vars above, instead it's done here.
      ## Doing so will fail with Perl 5.8.0 under Solaris 5.10 on large files.

      my $_buffer;

      ## Obtain the next chunk of data.
      {
         local $\ = undef; local $/ = $LF;
         flock $_DAT_LOCK, LOCK_EX;

         print $_OUT_W_SOCK $_output_tag, $LF;
         chomp($_len = <$_DAT_W_SOCK>);

         if ($_len == 0) {
            flock $_DAT_LOCK, LOCK_UN;
            return;
         }

         chomp($_chunk_id = <$_DAT_W_SOCK>);
         read $_DAT_W_SOCK, $_buffer, $_len;
         flock $_DAT_LOCK, LOCK_UN;
      }

      ## Call user function.
      if ($_proc_type == REQUEST_ARRAY) {
         if ($_single_dim && $_chunk_size == 1) {
            $_user_func->($self, [ $_buffer ], $_chunk_id);
         }
         else {
            $_chunk_ref = thaw($_buffer);
            $_user_func->($self, $_chunk_ref, $_chunk_id);
         }
      }
      else {
         if ($_use_slurpio) {
            $_user_func->($self, \$_buffer, $_chunk_id);
         }
         else {
            if ($_chunk_size == 1) {
               $_user_func->($self, [ $_buffer ], $_chunk_id);
            }
            else {
               _sync_buffer_to_array(\$_buffer, \@_records, $_RS);
               $_user_func->($self, \@_records, $_chunk_id);
            }
         }
      }
   }

   _WORKER_REQUEST_CHUNK__LAST:

   return;
}

###############################################################################
## ----------------------------------------------------------------------------
## Worker process -- Sequence (distribution via bank-teller queuing model).
##
###############################################################################

sub _worker_sequence {

   my MCE $self = $_[0];

   @_ = ();

   die "Private method called" unless (caller)[0]->isa( ref($self) );

   my $_many_wrks  = ($self->{max_workers} > 1) ? 1 : 0;
   my $_QUE_R_SOCK = $self->{_que_r_sock};
   my $_QUE_W_SOCK = $self->{_que_w_sock};
   my $_chunk_size = $self->{chunk_size};
   my $_user_func  = $self->{user_func};

   my ($_next, $_chunk_id, $_seq_n, $_begin, $_end, $_step, $_fmt);
   my ($_abort, $_offset);

   if (ref $self->{sequence} eq 'ARRAY') {
      ($_begin, $_end, $_step, $_fmt) = @{ $self->{sequence} };
   }
   else {
      $_begin = $self->{sequence}->{begin};
      $_end   = $self->{sequence}->{end};
      $_step  = $self->{sequence}->{step};
      $_fmt   = $self->{sequence}->{format};
   }

   $_abort    = $self->{_abort_msg};
   $_chunk_id = $_offset = 0;

   ## -------------------------------------------------------------------------

   $self->{_next_jmp} = sub { goto _WORKER_SEQUENCE__NEXT; };
   $self->{_last_jmp} = sub { goto _WORKER_SEQUENCE__LAST; };

   while (1) {

      ## Obtain the next chunk_id and sequence number.
      if ($_many_wrks) {
         local $\ = undef; local $/ = $LF;

         read $_QUE_R_SOCK, $_next, QUE_READ_SIZE;
         ($_chunk_id, $_offset) = unpack(QUE_TEMPLATE, $_next);

         if ($_offset >= $_abort) {
            print $_QUE_W_SOCK pack(QUE_TEMPLATE, 0, $_offset);
            return;
         }

         print $_QUE_W_SOCK pack(QUE_TEMPLATE, $_chunk_id + 1, $_offset + 1);
      }
      else {
         return if ($_offset >= $_abort);
      }

      $_chunk_id++;

      if ($_chunk_size == 1) {
         $_seq_n = $_offset * $_step + $_begin;
         $_seq_n = sprintf("%$_fmt", $_seq_n) if (defined $_fmt);
         $_user_func->($self, $_seq_n, $_chunk_id);
      }
      else {
         my $_n_begin = ($_offset * $_chunk_size) * $_step + $_begin;
         my @_n = ();

         $_seq_n = $_n_begin;

         if ($_begin < $_end) {
            for (1 .. $_chunk_size) {
               last if ($_seq_n > $_end);
               push @_n, (defined $_fmt) ? sprintf("%$_fmt", $_seq_n) : $_seq_n;
               $_seq_n = $_step * $_ + $_n_begin;
            }
         }
         else {
            for (1 .. $_chunk_size) {
               last if ($_seq_n < $_end);
               push @_n, (defined $_fmt) ? sprintf("%$_fmt", $_seq_n) : $_seq_n;
               $_seq_n = $_step * $_ + $_n_begin;
            }
         }

         $_user_func->($self, \@_n, $_chunk_id);
      }

      _WORKER_SEQUENCE__NEXT:

      $_offset++ unless ($_many_wrks);
   }

   _WORKER_SEQUENCE__LAST:

   return;
}

###############################################################################
## ----------------------------------------------------------------------------
## Worker process -- Sequence Generator (distributes equally among workers).
##
###############################################################################

sub _worker_sequence_generator {

   my MCE $self = $_[0];

   @_ = ();

   die "Private method called" unless (caller)[0]->isa( ref($self) );

   my $_max_workers = $self->{max_workers};
   my $_chunk_size  = $self->{chunk_size};
   my $_user_func   = $self->{user_func};

   my ($_begin, $_end, $_step, $_fmt);

   if (ref $self->{sequence} eq 'ARRAY') {
      ($_begin, $_end, $_step, $_fmt) = @{ $self->{sequence} };
   }
   else {
      $_begin = $self->{sequence}->{begin};
      $_end   = $self->{sequence}->{end};
      $_step  = $self->{sequence}->{step};
      $_fmt   = $self->{sequence}->{format};
   }

   my $_wid      = $self->{_task_wid} || $self->{_wid};
   my $_next     = ($_wid - 1) * $_chunk_size * $_step + $_begin;
   my $_chunk_id = $_wid;

   ## -------------------------------------------------------------------------

   $self->{_last_jmp} = sub { goto _WORKER_SEQ_GEN__LAST; };

   if ($_begin == $_end) {                        ## Both are identical.

      if ($_wid == 1) {
         $self->{_next_jmp} = sub { goto _WORKER_SEQ_GEN__LAST; };

         my $_seq_n = (defined $_fmt) ? sprintf("%$_fmt", $_next) : $_next;

         if ($_chunk_size > 1) {
            $_user_func->($self, [ $_seq_n ], $_chunk_id);
         } else {
            $_user_func->($self, $_seq_n, $_chunk_id);
         }
      }
   }
   elsif ($_chunk_size == 1) {                    ## Does no chunking.

      $self->{_next_jmp} = sub { goto _WORKER_SEQ_GEN__NEXT_A; };

      my $_flag = ($_begin < $_end);

      while (1) {
         return if ( $_flag && $_next > $_end);
         return if (!$_flag && $_next < $_end);

         my $_seq_n = (defined $_fmt) ? sprintf("%$_fmt", $_next) : $_next;

         $_user_func->($self, $_seq_n, $_chunk_id);
         _WORKER_SEQ_GEN__NEXT_A:

         $_chunk_id += $_max_workers;
         $_next      = ($_chunk_id - 1) * $_step + $_begin;
      }
   }
   else {                                         ## Yes, does chunking.

      $self->{_next_jmp} = sub { goto _WORKER_SEQ_GEN__NEXT_B; };

      while (1) {
         my $_n_begin = $_next;
         my @_n = ();

         if ($_begin < $_end) {
            for (1 .. $_chunk_size) {
               last if ($_next > $_end);
               push @_n, (defined $_fmt) ? sprintf("%$_fmt", $_next) : $_next;
               $_next = $_step * $_ + $_n_begin;
            }
         }
         else {
            for (1 .. $_chunk_size) {
               last if ($_next < $_end);
               push @_n, (defined $_fmt) ? sprintf("%$_fmt", $_next) : $_next;
               $_next = $_step * $_ + $_n_begin;
            }
         }

         return unless (@_n > 0);

         $_user_func->($self, \@_n, $_chunk_id);
         _WORKER_SEQ_GEN__NEXT_B:

         $_chunk_id += $_max_workers;
         $_next      = ($_chunk_id - 1) * $_chunk_size * $_step + $_begin;
      }
   }

   _WORKER_SEQ_GEN__LAST:

   return;
}

###############################################################################
## ----------------------------------------------------------------------------
## Worker process -- Do.
##
###############################################################################

sub _worker_do {

   my MCE $self = $_[0]; my $_params_ref = $_[1];

   @_ = ();

   die "Private method called" unless (caller)[0]->isa( ref($self) );

   ## Set options.
   $self->{_abort_msg}  = $_params_ref->{_abort_msg};
   $self->{_run_mode}   = $_params_ref->{_run_mode};
   $self->{_single_dim} = $_params_ref->{_single_dim};
   $self->{use_slurpio} = $_params_ref->{_use_slurpio};

   ## Do not override params if defined in user_tasks during instantiation.
   for (qw(chunk_size sequence user_args)) {
      if (defined $_params_ref->{"_$_"}) {
         $self->{$_} = $_params_ref->{"_$_"}
            unless (defined $self->{_task}->{$_});
      }
   }

   ## Init local vars.
   my $_OUT_W_SOCK = $self->{_out_w_sock};
   my $_run_mode   = $self->{_run_mode};
   my $_task_id    = $self->{_task_id};

   ## Call user_begin if defined.
   $self->{user_begin}->($self) if (defined $self->{user_begin});

   ## Call worker function.
   if ($_run_mode eq 'sequence') {
      $self->_worker_sequence();
   }
   elsif (defined $self->{sequence}) {
      $self->_worker_sequence_generator();
   }
   elsif ($_run_mode eq 'array') {
      $self->_worker_request_chunk(REQUEST_ARRAY);
   }
   elsif ($_run_mode eq 'glob') {
      $self->_worker_request_chunk(REQUEST_GLOB);
   }
   elsif ($_run_mode eq 'file') {
      $self->_worker_read_handle(READ_FILE, $_params_ref->{_input_file});
   }
   elsif ($_run_mode eq 'memory') {
      $self->_worker_read_handle(READ_MEMORY, $self->{input_data});
   }
   else {
      $self->{user_func}->($self) if (defined $self->{user_func});
   }

   undef $self->{_next_jmp} if (defined $self->{_next_jmp});
   undef $self->{_last_jmp} if (defined $self->{_last_jmp});
   undef $self->{user_data} if (defined $self->{user_data});

   ## Call user_end if defined.
   $self->{user_end}->($self) if (defined $self->{user_end});

   ## Notify the main process a worker has completed.
   local $\ = undef;

   print $_OUT_W_SOCK OUTPUT_W_DNE, $LF, $_task_id, $LF;

   return;
}

###############################################################################
## ----------------------------------------------------------------------------
## Worker process -- Loop.
##
###############################################################################

sub _worker_loop {

   my MCE $self = $_[0];

   @_ = ();

   die "Private method called" unless (caller)[0]->isa( ref($self) );

   my ($_response, $_len, $_buffer, $_params_ref);

   my $_COM_W_SOCK = $self->{_com_w_sock};
   my $_job_delay  = $self->{job_delay};
   my $_wid        = $self->{_wid};

   while (1) {

      {
         local $\ = undef; local $/ = $LF;
         flock $_COM_LOCK, LOCK_EX;

         ## Wait until next job request.
         $_response = <$_COM_W_SOCK>;
         print $_COM_W_SOCK $_wid, $LF;

         last unless (defined $_response);
         chomp $_response;

         ## End loop if an invalid reply.
         last if ($_response !~ /\A(?:\d+|_data|_exit)\z/);

         if ($_response eq '_data') {
            ## Acquire and process user data.
            chomp($_len = <$_COM_W_SOCK>);
            read $_COM_W_SOCK, $_buffer, $_len;

            print $_COM_W_SOCK $_wid, $LF;
            flock $_COM_LOCK, LOCK_UN;

            $self->{user_data} = thaw($_buffer);
            undef $_buffer;

            select(undef, undef, undef, $_job_delay * $_wid)
               if ($_job_delay && $_job_delay > 0.0);

            $self->_worker_do({ });
         }
         else {
            ## Return to caller if instructed to exit.
            if ($_response eq '_exit') {
               flock $_COM_LOCK, LOCK_UN;
               return 0;
            }

            ## Retrieve params data.
            chomp($_len = <$_COM_W_SOCK>);
            read $_COM_W_SOCK, $_buffer, $_len;

            print $_COM_W_SOCK $_wid, $LF;
            flock $_COM_LOCK, LOCK_UN;

            $_params_ref = thaw($_buffer);
         }
      }

      ## Start over if the last response was for processing user data.
      next if ($_response eq '_data');

      ## Wait until MCE completes params submission to all workers.
      flock $_DAT_LOCK, LOCK_SH;
      flock $_DAT_LOCK, LOCK_UN;

      ## Update ID. Process request.
      $self->{_task_wid} = $self->{_wid} = $_wid = $_response
         unless (defined $self->{user_tasks});

      select(undef, undef, undef, $_job_delay * $_wid)
         if ($_job_delay && $_job_delay > 0.0);

      $self->_worker_do($_params_ref); undef $_params_ref;

      ## Wait until remaining workers complete processing.
      flock $_COM_LOCK, LOCK_SH;
      flock $_COM_LOCK, LOCK_UN;
   }

   ## Notify the main process a worker has ended. The following is executed
   ## when an invalid reply was received above (not likely to occur).

   flock $_COM_LOCK, LOCK_UN;
   die "worker $self->{_wid} has ended prematurely";

   return 1;
}

###############################################################################
## ----------------------------------------------------------------------------
## Worker process -- Main.
##
###############################################################################

sub _worker_main {

   my MCE $self = $_[0]; my $_wid      = $_[1]; my $_task   = $_[2];
   my $_task_id = $_[3]; my $_task_wid = $_[4]; my $_params = $_[5];

   @_ = ();

   ## Commented out -- fails with the 'forks' module under FreeBSD.
   ## die "Private method called" unless (caller)[0]->isa( ref($self) );

   $SIG{PIPE} = \&_NOOP;

   $SIG{__DIE__} = sub {
      CORE::die(@_) if $^S;         ## Direct to CORE::die if executing an eval
      local $SIG{__DIE__} = sub { };
      local $\ = undef; print STDERR $_[0];
      $self->exit(255, $_[0]);
   };

   ## Use options from user_tasks if defined.
   $self->{user_args}  = $_task->{user_args}  if (defined $_task->{user_args});
   $self->{user_begin} = $_task->{user_begin} if (defined $_task->{user_begin});
   $self->{user_func}  = $_task->{user_func}  if (defined $_task->{user_func});
   $self->{user_end}   = $_task->{user_end}   if (defined $_task->{user_end});
   $self->{sequence}   = $_task->{sequence}   if (defined $_task->{sequence});
   $self->{chunk_size} = $_task->{chunk_size} if (defined $_task->{chunk_size});

   $self->{max_workers} = $_task->{max_workers}
      if (defined $_task->{max_workers});

   ## Init runtime vars. Obtain handle to lock files.
   my $_mce_sid  = $self->{_mce_sid};
   my $_sess_dir = $self->{_sess_dir};

   $self->{_task_id}  = (defined $_task_id ) ? $_task_id  : 0;
   $self->{_task_wid} = (defined $_task_wid) ? $_task_wid : $_wid;
   $self->{_task}     = $_task;
   $self->{_wid}      = $_wid;

   _do_send_init($self);

   open $_COM_LOCK, '+>> :stdio', "$_sess_dir/_com.lock";
   open $_DAT_LOCK, '+>> :stdio', "$_sess_dir/_dat.lock";
   open $_SYN_LOCK, '+>> :stdio', "$_sess_dir/_syn.lock";

   ## Define status ID.
   my $_use_threads = (defined $_task->{use_threads})
      ? $_task->{use_threads} : $self->{use_threads};

   if ($_has_threads && $_use_threads) {
      $self->{_exit_pid} = "TID_" . threads->tid();
   } else {
      $self->{_exit_pid} = "PID_" . $$;
   }

   ## Undef vars not required after being spawned.
   $self->{_com_r_sock} = $self->{_dat_r_sock} = $self->{_out_r_sock} =
      $self->{flush_file} = $self->{flush_stderr} = $self->{flush_stdout} =
      $self->{on_post_exit} = $self->{on_post_run} = $self->{stderr_file} =
      $self->{stdout_file} = $self->{user_error} = $self->{user_output} =
      $self->{user_data} =
   undef;

   $self->{_pids} = $self->{_thrs} = $self->{_tids} = $self->{_status} =
      $self->{_state} =
   ();

   foreach (keys %_mce_spawned) {
      delete $_mce_spawned{$_} unless ($_ eq $_mce_sid);
   }

   ## Begin processing if worker was added during processing.
   ## Respond back to the main process if the last worker spawned.
   if (defined $_params) {
      $self->_worker_do($_params); undef $_params;
   }
   elsif ($self->{_wid} == $self->{_total_workers}) {
      my $_COM_W_SOCK = $self->{_com_w_sock};
      local $\ = undef;
      print $_COM_W_SOCK $LF;
   }

   ## Wait until MCE completes spawning or worker completes running.
   flock $_COM_LOCK, LOCK_SH;
   flock $_COM_LOCK, LOCK_UN;

   ## Enter worker loop.
   my $_status = $self->_worker_loop();
   delete $_mce_spawned{ $self->{_mce_sid} };

   ## Wait until MCE completes exit notification.
   $SIG{__DIE__} = $SIG{__WARN__} = sub { };

   eval {
      flock $_DAT_LOCK, LOCK_SH;
      flock $_DAT_LOCK, LOCK_UN;
   };

   close $_DAT_LOCK; undef $_DAT_LOCK;
   close $_COM_LOCK; undef $_COM_LOCK;
   close $_SYN_LOCK; undef $_SYN_LOCK;

   return;
}

###############################################################################
## ----------------------------------------------------------------------------
## Dispatch child.
##
###############################################################################

sub _dispatch_child {

   my MCE $self = $_[0]; my $_wid      = $_[1]; my $_task   = $_[2];
   my $_task_id = $_[3]; my $_task_wid = $_[4]; my $_params = $_[5];

   @_ = ();

   die "Private method called" unless (caller)[0]->isa( ref($self) );

   select(undef, undef, undef, $self->{spawn_delay})
      if ($self->{spawn_delay} && $self->{spawn_delay} > 0.0);

   my $_pid = fork();

   _croak("MCE::_dispatch_child: Failed to spawn worker $_wid: $!")
      unless (defined $_pid);

   unless ($_pid) {
      _worker_main($self, $_wid, $_task, $_task_id, $_task_wid, $_params);

      close STDERR; close STDOUT;
      kill 9, $$ unless ($_is_winperl);

      CORE::exit();
   }

   if (defined $_pid) {
      ## Store into an available slot, otherwise append to array.
      if (defined $_params) { for (0 .. @{ $self->{_pids} } - 1) {
         unless (defined $self->{_pids}->[$_]) {
            $self->{_pids}->[$_] = $_pid;
            return;
         }
      }}

      push @{ $self->{_pids} }, $_pid;
   }

   return;
}

###############################################################################
## ----------------------------------------------------------------------------
## Dispatch thread.
##
###############################################################################

sub _dispatch_thread {

   my MCE $self = $_[0]; my $_wid      = $_[1]; my $_task   = $_[2];
   my $_task_id = $_[3]; my $_task_wid = $_[4]; my $_params = $_[5];

   @_ = ();

   die "Private method called" unless (caller)[0]->isa( ref($self) );

   select(undef, undef, undef, $self->{spawn_delay})
      if ($self->{spawn_delay} && $self->{spawn_delay} > 0.0);

   my $_thr = threads->create( \&_worker_main,
      $self, $_wid, $_task, $_task_id, $_task_wid, $_params
   );

   _croak("MCE::_dispatch_thread: Failed to spawn worker $_wid: $!")
      unless (defined $_thr);

   if (defined $_thr) {
      ## Store into an available slot, otherwise append to arrays.
      if (defined $_params) { for (0 .. @{ $self->{_tids} } - 1) {
         unless (defined $self->{_tids}->[$_]) {
            $self->{_thrs}->[$_] = \$_thr;
            $self->{_tids}->[$_] = $_thr->tid();
            return;
         }
      }}

      push @{ $self->{_thrs} }, \$_thr;
      push @{ $self->{_tids} }, $_thr->tid();
   }

   return;
}

1;

