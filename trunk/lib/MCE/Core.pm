###############################################################################
## ----------------------------------------------------------------------------
## MCE::Core - Provides the core API for Many-core Engine.
##
###############################################################################

##
## The MCE::Core package is defined below just prior to the POD section and
## contains the import routine for the MCE::Core namespace, nothing else.
##
## The top level MCE.pm file is now mainly an index page (POD wise). The code
## previously located in MCE.pm (1.4) moved here, some of which placed in
## various files under the Core directory. The API documentation, previously
## MCE.pod, is now placed at the end of this file with the examples section
## becoming MCE::Examples going forward.
##
## With that said, MCE 1.5 retains backwards compability with 1.4 and below.
## The core API, in entirety, is folded under the MCE namespace.
##

package MCE;

use strict;
use warnings;

use Fcntl qw( :flock O_RDONLY );
use Socket qw( :crlf AF_UNIX SOCK_STREAM PF_UNSPEC );
use Symbol qw( qualify_to_ref );
use Storable qw( );

use Time::HiRes qw( time );
use MCE::Signal;

our $VERSION = '1.499_002'; $VERSION = eval $VERSION;

my  (%_valid_fields_new, %_params_allowed_args, %_valid_fields_task);
our ($_is_cygwin, $_is_MSWin32, $_is_WinEnv);
our ($_que_read_size, $_que_template);

our $_MCE_LOCK : shared = 1;
our $_has_threads;

our $MCE; my $_prev_mce;

our $MAX_WORKERS = 1;
our $CHUNK_SIZE  = 1;
our $TMP_DIR     = $MCE::Signal::tmp_dir;
our $FREEZE      = \&Storable::freeze;
our $THAW        = \&Storable::thaw;

BEGIN {

   ## Configure pack/unpack template for writing to and reading from
   ## the queue. Each entry contains 2 positive numbers: chunk_id & msg_id.
   ## Attempt 64-bit size, otherwize fall back to host machine's word length.
   {
      local $@; local $SIG{__DIE__} = \&_NOOP;
      eval { $_que_read_size = length pack('Q2', 0, 0); };
      $_que_template  = ($@) ? 'I2' : 'Q2';
      $_que_read_size = length pack($_que_template, 0, 0);
   }

   ## ** Attributes which are used internally.
   ## _abort_msg _chn _com_lock _dat_lock _i_app_st _i_app_tb _i_wrk_st _wuf
   ## _chunk_id _mce_sid _mce_tid _pids _run_mode _single_dim _thrs _tids _wid
   ## _exiting _exit_pid _total_exited _total_running _total_workers _task_wid
   ## _send_cnt _sess_dir _spawned _state _status _task _task_id _wrk_status
   ##
   ## _bsb_r_sock _bsb_w_sock _bse_r_sock _bse_w_sock _com_r_sock _com_w_sock
   ## _dat_r_sock _dat_w_sock _que_r_sock _que_w_sock _data_channels _lock_chn

   %_valid_fields_new = map { $_ => 1 } qw(
      max_workers tmp_dir use_threads user_tasks task_end task_name freeze thaw

      chunk_size input_data sequence job_delay spawn_delay submit_delay RS
      flush_file flush_stderr flush_stdout stderr_file stdout_file use_slurpio
      interval user_args user_begin user_end user_func user_error user_output
      gather on_post_exit on_post_run
   );

   %_params_allowed_args = map { $_ => 1 } qw(
      chunk_size input_data sequence job_delay spawn_delay submit_delay RS
      flush_file flush_stderr flush_stdout stderr_file stdout_file use_slurpio
      interval user_args user_begin user_end user_func user_error user_output
      gather on_post_exit on_post_run
   );

   %_valid_fields_task = map { $_ => 1 } qw(
      max_workers chunk_size input_data interval sequence task_end task_name
      gather user_args user_begin user_end user_func use_threads
   );

   $_is_cygwin  = ($^O eq 'cygwin');
   $_is_MSWin32 = ($^O eq 'MSWin32');
   $_is_WinEnv  = ($_is_cygwin || $_is_MSWin32);

   ## Create accessor functions.
   no strict 'refs'; no warnings 'redefine';

   foreach my $_id (qw( chunk_size max_workers task_name tmp_dir user_args )) {
      *{ $_id } = sub () {
         my $x = shift; my MCE $self = ref($x) ? $x : $MCE;
         return $self->{$_id};
      };
   }
   foreach my $_id (qw( chunk_id sess_dir task_id task_wid wid )) {
      *{ $_id } = sub () {
         my $x = shift; my MCE $self = ref($x) ? $x : $MCE;
         return $self->{"_$_id"};
      };
   }
   foreach my $_id (qw( freeze thaw )) {
      *{ $_id } = sub () {
         my $x = shift; my MCE $self = ref($x) ? $x : $MCE;
         return $self->{$_id}(@_);
      };
   }

   ## PDL + MCE (spawning as threads) is not stable. Thanks to David Mertens
   ## for reporting on how he fixed it for his PDL::Parallel::threads module.
   ## The same fix is also needed here in order for PDL + MCE threads to not
   ## crash during exiting.

   sub PDL::CLONE_SKIP { 1 };

   return;
}

###############################################################################
## ----------------------------------------------------------------------------
## Define constants & variables.
##
###############################################################################

use constant {

   DATA_CHANNELS  => 8,                  ## Maximum IPC "DATA" channels

   MAX_CHUNK_SIZE => 24 * 1024 * 1024,   ## Maximum chunk size allowed
   MAX_RECS_SIZE  => 8192,               ## Reads # of records if <= value
                                         ## Reads # of bytes   if >  value

   OUTPUT_W_ABT   => 'W~ABT',            ## Worker has aborted
   OUTPUT_W_DNE   => 'W~DNE',            ## Worker has completed
   OUTPUT_W_EXT   => 'W~EXT',            ## Worker has exited

   OUTPUT_A_ARY   => 'A~ARY',            ## Array  << Array
   OUTPUT_S_GLB   => 'S~GLB',            ## Scalar << Glob FH

   OUTPUT_A_CBK   => 'A~CBK',            ## Callback w/ multiple args
   OUTPUT_S_CBK   => 'S~CBK',            ## Callback w/ 1 scalar arg
   OUTPUT_N_CBK   => 'N~CBK',            ## Callback w/ no args

   OUTPUT_A_GTR   => 'A~GTR',            ## Gather w/ multiple args
   OUTPUT_R_GTR   => 'R~GTR',            ## Gather w/ 1 reference arg
   OUTPUT_S_GTR   => 'S~GTR',            ## Gather w/ 1 scalar arg

   OUTPUT_O_SND   => 'O~SND',            ## Send >> STDOUT
   OUTPUT_E_SND   => 'E~SND',            ## Send >> STDERR
   OUTPUT_F_SND   => 'F~SND',            ## Send >> File
   OUTPUT_D_SND   => 'D~SND',            ## Send >> File descriptor

   OUTPUT_B_SYN   => 'B~SYN',            ## Barrier sync - begin
   OUTPUT_E_SYN   => 'E~SYN',            ## Barrier sync - end

   READ_FILE      => 0,                  ## Worker reads file handle
   READ_MEMORY    => 1,                  ## Worker reads memory handle

   REQUEST_ARRAY  => 0,                  ## Worker requests next array chunk
   REQUEST_GLOB   => 1,                  ## Worker requests next glob chunk

   SENDTO_FILEV1  => 0,                  ## Worker sends to 'file', $a, '/path'
   SENDTO_FILEV2  => 1,                  ## Worker sends to 'file:/path', $a
   SENDTO_STDOUT  => 2,                  ## Worker sends to STDOUT
   SENDTO_STDERR  => 3,                  ## Worker sends to STDERR
   SENDTO_FD      => 4,                  ## Worker sends to file descriptor

   WANTS_UNDEF    => 0,                  ## Callee wants nothing
   WANTS_ARRAY    => 1,                  ## Callee wants list
   WANTS_SCALAR   => 2,                  ## Callee wants scalar
   WANTS_REF      => 3                   ## Callee wants H/A/S ref
};

my  $_mce_count    = 0;
our %_mce_sess_dir = ();
our %_mce_spawned  = ();

$MCE::Signal::mce_sess_dir_ref = \%_mce_sess_dir;
$MCE::Signal::mce_spawned_ref  = \%_mce_spawned;

## Warnings are disabled to minimize bits of noise when user or OS signals
## the script to exit. e.g. MCE_script.pl < infile | head

no warnings 'threads'; no warnings 'uninitialized';

sub DESTROY { }

###############################################################################
## ----------------------------------------------------------------------------
## Plugin interface for external modules plugging into MCE, e.g. MCE::Queue.
##
###############################################################################

my (%_plugin_function, @_plugin_loop_begin, @_plugin_loop_end);
my (%_plugin_list, @_plugin_worker_init);

sub _attach_plugin {

   my $_ext_module = caller();

   unless (exists $_plugin_list{$_ext_module}) {
      $_plugin_list{$_ext_module} = 1;

      my $_ext_output_function    = $_[0];
      my $_ext_output_loop_begin  = $_[1];
      my $_ext_output_loop_end    = $_[2];
      my $_ext_worker_init        = $_[3];

      return unless (ref $_ext_output_function   eq 'HASH');
      return unless (ref $_ext_output_loop_begin eq 'CODE');
      return unless (ref $_ext_output_loop_end   eq 'CODE');
      return unless (ref $_ext_worker_init       eq 'CODE');

      for (keys %{ $_ext_output_function }) {
         $_plugin_function{$_} = $_ext_output_function->{$_}
            unless (exists $_plugin_function{$_});
      }

      push @_plugin_loop_begin, $_ext_output_loop_begin;
      push @_plugin_loop_end, $_ext_output_loop_end;
      push @_plugin_worker_init, $_ext_worker_init;
   }

   return;
}

## Functions for saving and restoring $MCE::MCE. This is mainly helpful
## for Modules using MCE. e.g. MCE::Map.

sub _save_state {
   $_prev_mce = $MCE;
   return;
}

sub _restore_state {
   $MCE = $_prev_mce; $_prev_mce = undef;
   return;
}

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
   $self->{max_workers} = (exists $argv{max_workers})
      ? $argv{max_workers} : $MCE::MAX_WORKERS;

   $self->{chunk_size}   = $argv{chunk_size}   || $MCE::CHUNK_SIZE;
   $self->{tmp_dir}      = $argv{tmp_dir}      || $MCE::TMP_DIR;
   $self->{freeze}       = $argv{freeze}       || $MCE::FREEZE;
   $self->{thaw}         = $argv{thaw}         || $MCE::THAW;
   $self->{task_name}    = $argv{task_name}    || 'MCE';

   if (exists $argv{_module_instance}) {
      $self->{_spawned} = $self->{_task_id} = $self->{_task_wid} =
          $self->{_chunk_id} = $self->{_wid} = $self->{_wrk_status} = 0;

      return $MCE = $self;
   }

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

   $self->{gather}       = $argv{gather}       if (exists $argv{gather});
   $self->{interval}     = $argv{interval}     if (exists $argv{interval});
   $self->{input_data}   = $argv{input_data}   if (exists $argv{input_data});
   $self->{sequence}     = $argv{sequence}     if (exists $argv{sequence});
   $self->{job_delay}    = $argv{job_delay}    if (exists $argv{job_delay});
   $self->{spawn_delay}  = $argv{spawn_delay}  if (exists $argv{spawn_delay});
   $self->{submit_delay} = $argv{submit_delay} if (exists $argv{submit_delay});
   $self->{on_post_exit} = $argv{on_post_exit} if (exists $argv{on_post_exit});
   $self->{on_post_run}  = $argv{on_post_run}  if (exists $argv{on_post_run});
   $self->{user_args}    = $argv{user_args}    if (exists $argv{user_args});
   $self->{user_begin}   = $argv{user_begin}   if (exists $argv{user_begin});
   $self->{user_func}    = $argv{user_func}    if (exists $argv{user_func});
   $self->{user_end}     = $argv{user_end}     if (exists $argv{user_end});
   $self->{user_error}   = $argv{user_error}   if (exists $argv{user_error});
   $self->{user_output}  = $argv{user_output}  if (exists $argv{user_output});
   $self->{stderr_file}  = $argv{stderr_file}  if (exists $argv{stderr_file});
   $self->{stdout_file}  = $argv{stdout_file}  if (exists $argv{stdout_file});
   $self->{user_tasks}   = $argv{user_tasks}   if (exists $argv{user_tasks});
   $self->{task_end}     = $argv{task_end}     if (exists $argv{task_end});
   $self->{RS}           = $argv{RS}           if (exists $argv{RS});

   $self->{flush_file}   = $argv{flush_file}   || 0;
   $self->{flush_stderr} = $argv{flush_stderr} || 0;
   $self->{flush_stdout} = $argv{flush_stdout} || 0;
   $self->{use_slurpio}  = $argv{use_slurpio}  || 0;

   ## -------------------------------------------------------------------------
   ## Validation.

   for (keys %argv) {
      _croak("MCE::new: '$_' is not a valid constructor argument")
         unless (exists $_valid_fields_new{$_});
   }

   _croak("MCE::new: '$self->{tmp_dir}' is not a directory or does not exist")
      unless (-d $self->{tmp_dir});
   _croak("MCE::new: '$self->{tmp_dir}' is not writeable")
      unless (-w $self->{tmp_dir});

   if (defined $self->{user_tasks}) {
      _croak("MCE::new: 'user_tasks' is not an ARRAY reference")
         unless (ref $self->{user_tasks} eq 'ARRAY');

      _parse_max_workers($self);

      for my $_task (@{ $self->{user_tasks} }) {
         for (keys %{ $_task }) {
            _croak("MCE::new: '$_' is not a valid task constructor argument")
               unless (exists $_valid_fields_task{$_});
         }
         $_task->{max_workers} = $self->{max_workers}
            unless (defined $_task->{max_workers});
         $_task->{use_threads} = $self->{use_threads}
            unless (defined $_task->{use_threads});

         bless($_task, ref($self) || $self);
      }

      ## File locking fails among children and threads under Cygwin.
      ## Must be all children or all threads, not intermixed.
      my (%_values, $_value);

      for my $_task (@{ $self->{user_tasks} }) {
         $_value = (defined $_task->{use_threads})
            ? $_task->{use_threads} : $self->{use_threads};

         $_values{$_value} = '';
      }

      _croak("MCE::new: 'cannot mix' use_threads => 0/1 under Cygwin")
         if ($_is_cygwin && keys %_values > 1);
   }

   _validate_args($self); %argv = ();

   ## -------------------------------------------------------------------------
   ## Private options. Limit chunk_size.

   $self->{_chunk_id}   = 0;     ## Chunk ID
   $self->{_send_cnt}   = 0;     ## Number of times data was sent via send
   $self->{_spawned}    = 0;     ## Have workers been spawned
   $self->{_task_id}    = 0;     ## Task ID, starts at 0 (array index)
   $self->{_task_wid}   = 0;     ## Task Worker ID, starts at 1 per task
   $self->{_wid}        = 0;     ## MCE Worker ID, starts at 1 per MCE instance
   $self->{_wrk_status} = 0;     ## For saving exit status when worker exits

   $self->{chunk_size} = MAX_CHUNK_SIZE
      if ($self->{chunk_size} > MAX_CHUNK_SIZE);

   my $_total_workers = 0;

   if (defined $self->{user_tasks}) {
      $_total_workers += $_->{max_workers}
         for (@{ $self->{user_tasks} });
   }
   else {
      $_total_workers = $self->{max_workers};
   }

   $self->{_data_channels} = ($_total_workers < DATA_CHANNELS)
      ? $_total_workers : DATA_CHANNELS;

   $self->{_lock_chn} = ($_total_workers > DATA_CHANNELS)
      ? 1 : 0;

   return $MCE = $self;
}

###############################################################################
## ----------------------------------------------------------------------------
## Spawn method.
##
###############################################################################

sub spawn {

   my $x = shift; my MCE $self = ref($x) ? $x : $MCE;

   ## To avoid leaking (Scalars leaked: 1) messages (fixed in Perl 5.12.x).
   @_ = ();

   _croak("MCE::spawn: method cannot be called by the worker process")
      if ($self->{_wid});

   ## Return if workers have already been spawned.
   return $self if ($self->{_spawned});

   $MCE = undef;

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

      $_sess_dir = $self->{_sess_dir} = "$_tmp_dir/$_mce_sid." . (++$_cnt)
         while ( !(mkdir $_sess_dir, 0770) );

      $_mce_sess_dir{$_sess_dir} = 1;
   }

   ## Obtain lock.
   open my $_COM_LOCK, '+>>:raw:stdio', "$_sess_dir/_com.lock"
      or die "(M) open error $_sess_dir/_com.lock: $!\n";

   flock $_COM_LOCK, LOCK_EX;

   ## -------------------------------------------------------------------------

   my $_data_channels = $self->{_data_channels};
   my $_max_workers   = $self->{max_workers};
   my $_use_threads   = $self->{use_threads};

   ## Create socket pairs for IPC.
   if (exists $self->{_dat_r_sock}) {
      @{ $self->{_dat_r_sock} } = (); @{ $self->{_dat_w_sock} } = ();
   } else {
      $self->{_dat_r_sock} = []; $self->{_dat_w_sock} = [];
   }

   _create_socket_pair($self, '_bsb_r_sock', '_bsb_w_sock', 1);  ## Sync
   _create_socket_pair($self, '_bse_r_sock', '_bse_w_sock', 1);  ## Sync
   _create_socket_pair($self, '_com_r_sock', '_com_w_sock', 0);  ## Core
   _create_socket_pair($self, '_que_r_sock', '_que_w_sock', 1);  ## Core

   _create_socket_pair($self, '_dat_r_sock', '_dat_w_sock', 1, 0);
   _create_socket_pair($self, '_dat_r_sock', '_dat_w_sock', 0, $_)
      for (1 .. $_data_channels);

   ## Place 1 char in one socket to ensure Perl loads the required modules
   ## prior to spawning. The last worker spawned will perform the read.
   syswrite $self->{_que_w_sock}, $LF;

   ## Preload the input module if required.
   if (!defined $self->{user_tasks}) {
      if (defined $self->{input_data}) {
         my $_ref_type = ref $self->{input_data};
         if ($_ref_type eq '' || $_ref_type eq 'SCALAR') {
            require MCE::Core::Input::Handle
               unless (defined $MCE::Core::Input::Handle::VERSION);
         }
         else {
            require MCE::Core::Input::Request
               unless (defined $MCE::Core::Input::Request::VERSION);
         }
      }
      elsif (defined $self->{sequence}) {
         require MCE::Core::Input::Sequence
            unless (defined $MCE::Core::Input::Sequence::VERSION);
      }
   }

   ## -------------------------------------------------------------------------

   ## Spawn workers.
   $_mce_spawned{$_mce_sid} = $self;

   $self->{_pids}   = []; $self->{_thrs}  = []; $self->{_tids} = [];
   $self->{_status} = []; $self->{_state} = []; $self->{_task} = [];

   if (!defined $self->{user_tasks}) {
      $self->{_total_workers} = $_max_workers;

      if (defined $_use_threads && $_use_threads == 1) {
         _dispatch_thread($self, $_) for (1 .. $_max_workers);
      } else {
         _dispatch_child($self, $_) for (1 .. $_max_workers);
      }

      $self->{_task}->[0] = { _total_workers => $_max_workers };

      for (1 .. $_max_workers) {
         keys(%{ $self->{_state}->[$_] }) = 5;
         $self->{_state}->[$_] = {
            _task => undef, _task_id => undef, _task_wid => undef,
            _params => undef, _chn => $_ % $_data_channels + 1
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
            _dispatch_thread($self, ++$_wid, $_task, $_task_id, $_)
               for (1 .. $_task->{max_workers});
         } else {
            _dispatch_child($self, ++$_wid, $_task, $_task_id, $_)
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
            keys(%{ $self->{_state}->[++$_wid] }) = 5;
            $self->{_state}->[$_wid] = {
               _task => $_task, _task_id => $_task_id, _task_wid => $_,
               _params => undef, _chn => $_wid % $_data_channels + 1
            }
         }

         $_task_id++;
      }
   }

   ## -------------------------------------------------------------------------

   $self->{_com_lock} = $_COM_LOCK;
   $self->{_send_cnt} = 0;
   $self->{_spawned}  = 1;

   ## Await reply from the last worker spawned.
   if ($self->{_total_workers} > 0) {
      my $_COM_R_SOCK = $self->{_com_r_sock};
      local $/ = $LF; <$_COM_R_SOCK>;
   }

   ## Release lock.
   flock $_COM_LOCK, LOCK_UN;

   $SIG{__DIE__}  = $_die_handler;
   $SIG{__WARN__} = $_warn_handler;

   $MCE = $self;
   return $self;
}

###############################################################################
## ----------------------------------------------------------------------------
## Forchunk, foreach, and forseq methods.
##
###############################################################################

sub forchunk {

   my $x = shift; my MCE $self = ref($x) ? $x : $MCE;
   my $_input_data = $_[0];

   _validate_runstate($self, "MCE::forchunk");

   my ($_user_func, $_params_ref);

   if (ref $_[1] eq 'HASH') {
      $_user_func = $_[2]; $_params_ref = $_[1];
   } else {
      $_user_func = $_[1]; $_params_ref = {};
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

sub foreach {

   my $x = shift; my MCE $self = ref($x) ? $x : $MCE;
   my $_input_data = $_[0];

   _validate_runstate($self, "MCE::foreach");

   my ($_user_func, $_params_ref);

   if (ref $_[1] eq 'HASH') {
      $_user_func = $_[2]; $_params_ref = $_[1];
   } else {
      $_user_func = $_[1]; $_params_ref = {};
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

sub forseq {

   my $x = shift; my MCE $self = ref($x) ? $x : $MCE;
   my $_sequence = $_[0];

   _validate_runstate($self, "MCE::forseq");

   my ($_user_func, $_params_ref);

   if (ref $_[1] eq 'HASH') {
      $_user_func = $_[2]; $_params_ref = $_[1];
   } else {
      $_user_func = $_[1]; $_params_ref = {};
   }

   @_ = ();

   _croak("MCE::forseq: 'sequence' is not specified")
      unless (defined $_sequence);
   _croak("MCE::forseq: 'code_block' is not specified")
      unless (defined $_user_func);

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

   my $x = shift; my MCE $self = ref($x) ? $x : $MCE;

   _validate_runstate($self, "MCE::process");

   my ($_input_data, $_params_ref);

   if (ref $_[0] eq 'HASH') {
      $_input_data = $_[1]; $_params_ref = $_[0];
   } else {
      $_input_data = $_[0]; $_params_ref = $_[1];
   }

   @_ = ();

   ## Set input data.
   if (defined $_input_data) {
      $_params_ref->{input_data} = $_input_data;
   }
   elsif ( !defined $_params_ref->{input_data} &&
           !defined $_params_ref->{sequence} ) {
      _croak("MCE::process: 'input_data or sequence' is not specified");
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

   my $x = shift; my MCE $self = ref($x) ? $x : $MCE;

   _croak("MCE::restart_worker: method cannot be called by the worker process")
      if ($self->{_wid});

   @_ = ();

   my $_wid = $self->{_exited_wid};

   my $_params   = $self->{_state}->[$_wid]->{_params};
   my $_task_wid = $self->{_state}->[$_wid]->{_task_wid};
   my $_task_id  = $self->{_state}->[$_wid]->{_task_id};
   my $_task     = $self->{_state}->[$_wid]->{_task};
   my $_chn      = $self->{_state}->[$_wid]->{_chn};

   $_params->{_chn} = $_chn;

   my $_use_threads = (defined $_task_id)
      ? $_task->{use_threads} : $self->{use_threads};

   $self->{_task}->[$_task_id]->{_total_running} += 1 if (defined $_task_id);
   $self->{_task}->[$_task_id]->{_total_workers} += 1 if (defined $_task_id);

   $self->{_total_running} += 1;
   $self->{_total_workers} += 1;

   if (defined $_use_threads && $_use_threads == 1) {
      _dispatch_thread($self, $_wid, $_task, $_task_id, $_task_wid, $_params);
   } else {
      _dispatch_child($self, $_wid, $_task, $_task_id, $_task_wid, $_params);
   }

   select(undef, undef, undef, 0.002);

   return;
}

###############################################################################
## ----------------------------------------------------------------------------
## Run method.
##
###############################################################################

sub run {

   my $x = shift; my MCE $self = ref($x) ? $x : $MCE;

   _croak("MCE::run: method cannot be called by the worker process")
      if ($self->{_wid});

   my ($_auto_shutdown, $_params_ref);

   if (ref $_[0] eq 'HASH') {
      $_auto_shutdown = (defined $_[1]) ? $_[1] : 1;
      $_params_ref    = $_[0];
   } else {
      $_auto_shutdown = (defined $_[0]) ? $_[0] : 1;
      $_params_ref    = $_[1];
   }

   @_ = ();

   my $_has_user_tasks = (defined $self->{user_tasks});
   my $_requires_shutdown = 0;

   ## Unset params if workers have been sent user_data via send.
   $_params_ref = undef if ($self->{_send_cnt});

   ## Set user_func to NOOP if not specified.
   $self->{user_func} = \&_NOOP
      if (!defined $self->{user_func} && !defined $_params_ref->{user_func});

   ## Set user specified params if specified.
   if (defined $_params_ref && ref $_params_ref eq 'HASH') {
      $_requires_shutdown = _sync_params($self, $_params_ref);
      _validate_args($self);
   }

   ## Shutdown workers if determined by _sync_params or if processing a
   ## scalar reference. Workers need to be restarted in order to pick up
   ## on the new code blocks and/or scalar reference.

   $self->{input_data} = $self->{user_tasks}->[0]->{input_data}
      if ($_has_user_tasks && $self->{user_tasks}->[0]->{input_data});

   $self->shutdown()
      if ($_requires_shutdown || ref $self->{input_data} eq 'SCALAR');

   ## -------------------------------------------------------------------------

   $self->{_wrk_status} = 0;

   ## Spawn workers.
   $self->spawn() unless ($self->{_spawned});
   return $self   unless ($self->{_total_workers});

   local $SIG{__DIE__}  = \&_die;
   local $SIG{__WARN__} = \&_warn;

   $MCE = $self;

   my ($_input_data, $_input_file, $_input_glob, $_seq);
   my ($_abort_msg, $_first_msg, $_run_mode, $_single_dim);
   my $_chunk_size = $self->{chunk_size};

   $_seq = ($_has_user_tasks && $self->{user_tasks}->[0]->{sequence})
      ? $self->{user_tasks}->[0]->{sequence}
      : $self->{sequence};

   ## Determine run mode for workers.
   if (defined $_seq) {
      my ($_begin, $_end, $_step, $_fmt) = (ref $_seq eq 'ARRAY')
         ? @{ $_seq } : ($_seq->{begin}, $_seq->{end}, $_seq->{step});
      $_chunk_size = $self->{user_tasks}->[0]->{chunk_size}
         if ($_has_user_tasks && $self->{user_tasks}->[0]->{chunk_size});
      $_run_mode  = 'sequence';
      $_abort_msg = int(($_end - $_begin) / $_step / $_chunk_size) + 1;
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

   my $_COM_LOCK      = $self->{_com_lock};
   my $_interval      = $self->{interval};
   my $_sequence      = $self->{sequence};
   my $_user_args     = $self->{user_args};
   my $_use_slurpio   = $self->{use_slurpio};
   my $_sess_dir      = $self->{_sess_dir};
   my $_total_workers = $self->{_total_workers};
   my $_send_cnt      = $self->{_send_cnt};

   ## Begin processing.
   unless ($_send_cnt) {

      my %_params = (
         '_abort_msg'   => $_abort_msg,    '_run_mode'   => $_run_mode,
         '_chunk_size'  => $_chunk_size,   '_single_dim' => $_single_dim,
         '_input_file'  => $_input_file,   '_sequence'   => $_sequence,
         '_interval'    => $_interval,     '_user_args'  => $_user_args,
         '_use_slurpio' => $_use_slurpio
      );
      my %_params_nodata = (
         '_abort_msg'   => undef,          '_run_mode'   => 'nodata',
         '_chunk_size'  => $_chunk_size,   '_single_dim' => $_single_dim,
         '_input_file'  => $_input_file,   '_sequence'   => $_sequence,
         '_interval'    => $_interval,     '_user_args'  => $_user_args,
         '_use_slurpio' => $_use_slurpio
      );

      local $\ = undef; local $/ = $LF;
      lock $_MCE_LOCK if ($_has_threads);            ## Obtain MCE lock.

      my ($_wid, %_task0_wids);

      my $_BSE_W_SOCK    = $self->{_bse_w_sock};
      my $_COM_R_SOCK    = $self->{_com_r_sock};
      my $_submit_delay  = $self->{submit_delay};
      my $_frozen_params = $self->{freeze}(\%_params);

      my $_frozen_nodata = $self->{freeze}(\%_params_nodata)
         if ($_has_user_tasks);

      if ($_has_user_tasks) { for (1 .. @{ $self->{_state} } - 1) {
         $_task0_wids{$_} = 1 unless ($self->{_state}->[$_]->{_task_id});
      }}

      ## Insert the first message into the queue if defined.
      if (defined $_first_msg) {
         my $_QUE_W_SOCK = $self->{_que_w_sock};
         syswrite $_QUE_W_SOCK, pack($_que_template, 0, $_first_msg);
      }

      ## Submit params data to workers.
      for (1 .. $_total_workers) {
         print $_COM_R_SOCK $_ . $LF;
         chomp($_wid = <$_COM_R_SOCK>);

         if (!$_has_user_tasks || exists $_task0_wids{$_wid}) {
            print $_COM_R_SOCK length($_frozen_params) . $LF . $_frozen_params;
            $self->{_state}->[$_wid]->{_params} = \%_params;
         } else {
            print $_COM_R_SOCK length($_frozen_nodata) . $LF . $_frozen_nodata;
            $self->{_state}->[$_wid]->{_params} = \%_params_nodata;
         }

         <$_COM_R_SOCK>;

         select(undef, undef, undef, $_submit_delay)
            if (defined $_submit_delay && $_submit_delay > 0.0);
      }

      select(undef, undef, undef, 0.005) if ($_is_WinEnv);

      ## Obtain lock.
      flock $_COM_LOCK, LOCK_EX;

      syswrite $_BSE_W_SOCK, $LF for (1 .. $_total_workers);

      select(undef, undef, undef, 0.002)
         if (($self->{_mce_tid} ne '' && $self->{_mce_tid} ne '0') ||
             $_is_WinEnv );
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

      _output_loop( $self, $_input_data, $_input_glob,
         \%_plugin_function, \@_plugin_loop_begin, \@_plugin_loop_end
      );

      undef $self->{_abort_msg};
      undef $self->{_run_mode};
      undef $self->{_single_dim};
   }

   unless ($_send_cnt) {
      ## Remove the last message from the queue.
      unless ($_run_mode eq 'nodata') {
         unlink "$_sess_dir/_store.db" if ($_run_mode eq 'array');
         if (defined $self->{_que_r_sock}) {
            my $_next; my $_QUE_R_SOCK = $self->{_que_r_sock};
            sysread $_QUE_R_SOCK, $_next, $_que_read_size;
         }
      }

      ## Release lock.
      flock $_COM_LOCK, LOCK_UN;
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

   my $x = shift; my MCE $self = ref($x) ? $x : $MCE;

   _croak("MCE::send: method cannot be called by the worker process")
      if ($self->{_wid});
   _croak("MCE::send: method cannot be called while running")
      if ($self->{_total_running});

   _croak("MCE::send: method cannot be used with input_data or sequence")
      if (defined $self->{input_data} || defined $self->{sequence});
   _croak("MCE::send: method cannot be used with user_tasks")
      if (defined $self->{user_tasks});

   my $_data_ref;

   if (ref $_[0] eq 'ARRAY' || ref $_[0] eq 'HASH' || ref $_[0] eq 'PDL') {
      $_data_ref = $_[0];
   } else {
      _croak("MCE::send: ARRAY, HASH, or a PDL reference is not specified");
   }

   $self->{_send_cnt} = 0 unless (defined $self->{_send_cnt});

   @_ = ();

   ## -------------------------------------------------------------------------

   ## Spawn workers.
   $self->spawn() unless ($self->{_spawned});

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
      my $_frozen_data  = $self->{freeze}($_data_ref);

      ## Submit data to worker.
      print $_COM_R_SOCK '_data' . $LF;
      <$_COM_R_SOCK>;

      print $_COM_R_SOCK length($_frozen_data) . $LF . $_frozen_data;
      <$_COM_R_SOCK>;

      select(undef, undef, undef, $_submit_delay)
         if (defined $_submit_delay && $_submit_delay > 0.0);

      select(undef, undef, undef, 0.002) if ($_is_cygwin);
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

   my $x = shift; my MCE $self = ref($x) ? $x : $MCE;

   @_ = ();

   _validate_runstate($self, "MCE::shutdown");

   ## Return if workers have not been spawned or have already been shutdown.
   return unless ($self->{_spawned});

   ## Wait for workers to complete processing before shutting down.
   $self->run(0) if ($self->{_send_cnt});

   local $SIG{__DIE__}  = \&_die;
   local $SIG{__WARN__} = \&_warn;

   lock $_MCE_LOCK if ($_has_threads);            ## Obtain MCE lock.

   my $_is_mce_thr     = ($self->{_mce_tid} ne '' && $self->{_mce_tid} ne '0');
   my $_COM_R_SOCK     = $self->{_com_r_sock};
   my $_data_channels  = $self->{_data_channels};
   my $_mce_sid        = $self->{_mce_sid};
   my $_sess_dir       = $self->{_sess_dir};
   my $_total_workers  = $self->{_total_workers};

   ## Delete entry.
   delete $_mce_spawned{$_mce_sid};

   ## Notify workers to exit loop.
   local $\ = undef; local $/ = $LF;

   for (1 .. $_total_workers) {
      print $_COM_R_SOCK '_exit' . $LF;
      <$_COM_R_SOCK>;
   }

   CORE::shutdown $self->{_bse_w_sock}, 1;        ## Barrier end channels
   CORE::shutdown $self->{_bse_r_sock}, 0;

   ## Reap children/threads.
   if ( $self->{_pids} && @{ $self->{_pids} } > 0 ) {
      my $_list = $self->{_pids};
      for my $i (0 .. @$_list) {
         waitpid $_list->[$i], 0 if ($_list->[$i]);
      }
   }
   elsif ( $self->{_thrs} && @{ $self->{_thrs} } > 0 ) {
      my $_list = $self->{_thrs};
      for my $i (0 .. @$_list) {
         ${ $_list->[$i] }->join() if ($_list->[$i]);
      }
   }

   close $self->{_com_lock}; undef $self->{_com_lock};

   ## -------------------------------------------------------------------------

   ## Close sockets.
   CORE::shutdown $self->{_bsb_w_sock}, 1;        ## Barrier begin channels
   CORE::shutdown $self->{_bsb_r_sock}, 0;
   CORE::shutdown $self->{_com_w_sock}, 2;        ## Communication channels
   CORE::shutdown $self->{_com_r_sock}, 2;
   CORE::shutdown $self->{_que_w_sock}, 1;        ## Queue channels
   CORE::shutdown $self->{_que_r_sock}, 0;

   CORE::shutdown $self->{_dat_w_sock}->[0], 1;   ## Data channels
   CORE::shutdown $self->{_dat_r_sock}->[0], 0;

   for (1 .. $_data_channels) {
      CORE::shutdown $self->{_dat_w_sock}->[$_], 2;
      CORE::shutdown $self->{_dat_r_sock}->[$_], 2;
   }

   for (
      qw( _bsb_w_sock _bsb_r_sock _bse_w_sock _bse_r_sock _com_w_sock
          _com_r_sock _que_w_sock _que_r_sock _dat_w_sock _dat_r_sock )
   ) {
      if (ref $self->{$_} eq 'ARRAY') {
         for my $_s (@{ $self->{$_} }) {
            close $_s; undef $_s;
         }
      } else {
         close $self->{$_}; undef $self->{$_};
      }
   }

   ## -------------------------------------------------------------------------

   ## Remove the session directory.
   if (defined $_sess_dir) {
      unlink "$_sess_dir/_dat.lock.$_" for (1 .. $_data_channels);
      unlink "$_sess_dir/_com.lock";
      rmdir  "$_sess_dir";

      delete $_mce_sess_dir{$_sess_dir};
   }

   ## Reset instance.
   @{$self->{_pids}}   = (); @{$self->{_thrs}}  = (); @{$self->{_tids}} = ();
   @{$self->{_status}} = (); @{$self->{_state}} = (); @{$self->{_task}} = ();

   $self->{_mce_sid}  = $self->{_mce_tid}  = $self->{_sess_dir} = undef;
   $self->{_chunk_id} = $self->{_send_cnt} = $self->{_spawned}  = 0;

   select(undef, undef, undef, 0.082) if ($_is_mce_thr);

   $self->{_total_running} = $self->{_total_workers} = 0;
   $self->{_total_exited}  = 0;

   return;
}

###############################################################################
## ----------------------------------------------------------------------------
## Barrier sync method.
##
###############################################################################

sub sync {

   my $x = shift; my MCE $self = ref($x) ? $x : $MCE;

   _croak("MCE::sync: method cannot be called by the manager process")
      unless ($self->{_wid});

   ## Barrier synchronization is supported for task 0 at this time.
   ## Note: Workers are assigned task_id 0 when omitting user_tasks.

   return if ($self->{_task_id} > 0);

   my $_chn        = $self->{_chn};
   my $_DAT_LOCK   = $self->{_dat_lock};
   my $_DAT_W_SOCK = $self->{_dat_w_sock}->[0];
   my $_BSB_R_SOCK = $self->{_bsb_r_sock};
   my $_BSE_R_SOCK = $self->{_bse_r_sock};
   my $_lock_chn   = $self->{_lock_chn};
   my $_buffer;

   local $\ = undef if (defined $\); local $/ = $LF if (!$/ || $/ ne $LF);

   ## Notify the manager process (begin).
   flock $_DAT_LOCK, LOCK_EX if ($_lock_chn);
   print $_DAT_W_SOCK OUTPUT_B_SYN . $LF . $_chn . $LF;
   flock $_DAT_LOCK, LOCK_UN if ($_lock_chn);

   ## Wait here until all workers (task_id 0) have synced.
   sysread $_BSB_R_SOCK, $_buffer, 1;

   ## Notify the manager process (end).
   flock $_DAT_LOCK, LOCK_EX if ($_lock_chn);
   print $_DAT_W_SOCK OUTPUT_E_SYN . $LF . $_chn . $LF;
   flock $_DAT_LOCK, LOCK_UN if ($_lock_chn);

   ## Wait here until all workers (task_id 0) have un-synced.
   sysread $_BSE_R_SOCK, $_buffer, 1;

   return;
}

###############################################################################
## ----------------------------------------------------------------------------
## Yield method.
##
###############################################################################

sub yield {

   my $x = shift; my MCE $self = ref($x) ? $x : $MCE;

   return unless ($self->{_i_wrk_st});
   return unless ($self->{_task_wid});

   my $_delay = $self->{_i_wrk_st} - time();
   my $_count;

   if ($_delay < 0.0) {
      $_count  = int($_delay * -1 / $self->{_i_app_tb} + 0.499) + 1;
      $_delay += $self->{_i_app_tb} * $_count;
   }

   select(undef, undef, undef, $_delay) if ($_delay > 0.0);
   $self->{_i_wrk_st} = time() if ($_count && $_count > 2000000000);

   return;
}

###############################################################################
## ----------------------------------------------------------------------------
## Miscellaneous methods: abort exit last next status.
##
###############################################################################

## Abort current job.

sub abort {

   my $x = shift; my MCE $self = ref($x) ? $x : $MCE;

   my $_QUE_R_SOCK = $self->{_que_r_sock};
   my $_QUE_W_SOCK = $self->{_que_w_sock};
   my $_abort_msg  = $self->{_abort_msg};
   my $_lock_chn   = $self->{_lock_chn};

   if (defined $_abort_msg) {
      local $\ = undef;

      my $_next; sysread $_QUE_R_SOCK, $_next, $_que_read_size;
      syswrite $_QUE_W_SOCK, pack($_que_template, 0, $_abort_msg);

      if ($self->{_wid} > 0) {
         my $_chn        = $self->{_chn};
         my $_DAT_LOCK   = $self->{_dat_lock};
         my $_DAT_W_SOCK = $self->{_dat_w_sock}->[0];

         flock $_DAT_LOCK, LOCK_EX if ($_lock_chn);
         print $_DAT_W_SOCK OUTPUT_W_ABT . $LF . $_chn . $LF;
         flock $_DAT_LOCK, LOCK_UN if ($_lock_chn);
      }
   }

   return;
}

## Worker exits from MCE.

sub exit {

   my $x = shift; my MCE $self = ref($x) ? $x : $MCE;

   my $_exit_status = (defined $_[0]) ? $_[0] : $?;
   my $_exit_msg    = (defined $_[1]) ? $_[1] : '';
   my $_exit_id     = (defined $_[2]) ? $_[2] : '';

   @_ = ();

   _croak("MCE::exit: method cannot be called by the manager process")
      unless ($self->{_wid});

   delete $_mce_spawned{ $self->{_mce_sid} };

   my $_chn        = $self->{_chn};
   my $_COM_LOCK   = $self->{_com_lock};
   my $_DAT_LOCK   = $self->{_dat_lock};
   my $_DAT_W_SOCK = $self->{_dat_w_sock}->[0];
   my $_DAU_W_SOCK = $self->{_dat_w_sock}->[$_chn];
   my $_lock_chn   = $self->{_lock_chn};
   my $_task_id    = $self->{_task_id};
   my $_sess_dir   = $self->{_sess_dir};

   if (!$_lock_chn || $_chn != 1) {
      if (defined $_DAT_LOCK) {
         close $_DAT_LOCK; undef $_DAT_LOCK;
         select(undef, undef, undef, 0.002);
      }
      open $_DAT_LOCK, '+>>:raw:stdio', "$_sess_dir/_dat.lock.1"
         or die "(W) open error $_sess_dir/_dat.lock.1: $!\n";
   }

   unless ($self->{_exiting}) {
      $self->{_exiting} = 1;

      my $_len = length $_exit_msg;
      local $\ = undef;

      $_exit_id =~ s/[\r\n][\r\n]*/ /mg;

      flock $_DAT_LOCK, LOCK_EX;
      select(undef, undef, undef, 0.02) if ($_is_cygwin);

      print $_DAT_W_SOCK OUTPUT_W_EXT . $LF . $_chn . $LF;
      print $_DAU_W_SOCK
         $_task_id . $LF . $self->{_wid} . $LF . $self->{_exit_pid} . $LF .
         $_exit_status . $LF . $_exit_id . $LF . $_len . $LF . $_exit_msg
      ;

      flock $_DAT_LOCK, LOCK_UN;
   }

   ## Exit thread/child process.
   $SIG{__DIE__} = $SIG{__WARN__} = sub { };

   close $_DAT_LOCK; undef $_DAT_LOCK;
   close $_COM_LOCK; undef $_COM_LOCK;
   close STDERR; close STDOUT;

   threads->exit($_exit_status)
      if ($_has_threads && threads->can('exit'));

   kill 9, $$ unless ($_is_WinEnv);

   CORE::exit($_exit_status);
}

## Worker immediately exits the chunking loop.

sub last {

   my $x = shift; my MCE $self = ref($x) ? $x : $MCE;

   _croak("MCE::last: method cannot be called by the manager process")
      unless ($self->{_wid});

   $self->{_last_jmp}() if (defined $self->{_last_jmp});

   return;
}

## Worker starts the next iteration of the chunking loop.

sub next {

   my $x = shift; my MCE $self = ref($x) ? $x : $MCE;

   _croak("MCE::next: method cannot be called by the manager process")
      unless ($self->{_wid});

   $self->{_next_jmp}() if (defined $self->{_next_jmp});

   return;
}

## Return the exit status. "_wrk_status" holds the greatest exit status
## among workers exiting.

sub status {

   my $x = shift; my MCE $self = ref($x) ? $x : $MCE;

   _croak("MCE::status: method cannot be called by the worker process")
      if ($self->{_wid});

   return (defined $self->{_wrk_status}) ? $self->{_wrk_status} : 0;
}

###############################################################################
## ----------------------------------------------------------------------------
## Methods for serializing data from workers to the main thread.
##
###############################################################################

## Do method. Additional arguments are optional.

sub do {

   my $x = shift; my MCE $self = ref($x) ? $x : $MCE;
   my $_callback = shift;

   _croak("MCE::do: method cannot be called by the manager process")
      unless ($self->{_wid});
   _croak("MCE::do: 'callback' is not specified")
      unless (defined $_callback);

   $_callback = "main::$_callback" if (index($_callback, ':') < 0);

   return _do_callback($self, $_callback, \@_);
}

## Gather method.

sub gather {

   my $x = shift; my MCE $self = ref($x) ? $x : $MCE;

   _croak("MCE::gather: method cannot be called by the manager process")
      unless ($self->{_wid});

   return _do_gather($self, \@_);
}

## Sendto method.

{
   my %_sendto_lkup = (
      'file'   => SENDTO_FILEV1, 'FILE'   => SENDTO_FILEV1,
      'file:'  => SENDTO_FILEV2, 'FILE:'  => SENDTO_FILEV2,
      'stdout' => SENDTO_STDOUT, 'STDOUT' => SENDTO_STDOUT,
      'stderr' => SENDTO_STDERR, 'STDERR' => SENDTO_STDERR,
      'fd:'    => SENDTO_FD,     'FD:'    => SENDTO_FD
   );

   my $_v2_regx = qr/^([^:]+:)(.+)/;

   sub sendto {

      my $x = shift; my MCE $self = ref($x) ? $x : $MCE;
      my $_to = shift;

      _croak("MCE::sendto: method cannot be called by the manager process")
         unless ($self->{_wid});

      return unless (defined $_[0]);

      my ($_dest, $_value);
      $_dest = (exists $_sendto_lkup{$_to}) ? $_sendto_lkup{$_to} : undef;

      if (!defined $_dest) {
         if (defined (my $_fd = fileno($_to))) {
            my $_data = (scalar @_) ? join('', @_) : $_;
            _do_send_glob($self, $_to, $_fd, \$_data);
            return;
         }
         if (defined $_to && $_to =~ /$_v2_regx/o) {
            $_dest  = (exists $_sendto_lkup{$1}) ? $_sendto_lkup{$1} : undef;
            $_value = $2;
         }
         if ( !defined $_dest || ( !defined $_value && (
               $_dest == SENDTO_FILEV2 || $_dest == SENDTO_FD
         ))) {
            my $_msg  = "\n";
               $_msg .= "MCE::sendto: improper use of method\n";
               $_msg .= "\n";
               $_msg .= "## usage:\n";
               $_msg .= "##    ->sendto(\"stderr\", ...);\n";
               $_msg .= "##    ->sendto(\"stdout\", ...);\n";
               $_msg .= "##    ->sendto(\"file:/path/to/file\", ...);\n";
               $_msg .= "##    ->sendto(\"fd:2\", ...);\n";
               $_msg .= "\n";

            _croak($_msg);
         }
      }

      if ($_dest == SENDTO_FILEV1) {              ## sendto 'file', $a, $path
         return if (!defined $_[1] || @_ > 2);    ## Please switch to using V2
         $_value = $_[1]; delete $_[1];           ## sendto 'file:/path', $a
         $_dest  = SENDTO_FILEV2;
      }

      return _do_send($self, $_dest, $_value, @_);
   }
}

###############################################################################
## ----------------------------------------------------------------------------
## Functions for serializing print, printf and say statements.
##
###############################################################################

sub print {

   my $x = shift; my MCE $self = ref($x) ? $x : $MCE;

   if (defined (my $_fd = fileno($_[0]))) {
      my $_glob = shift;
      my $_data = (scalar @_) ? join('', @_) : $_;

      _do_send_glob($self, $_glob, $_fd, \$_data);
   }
   else {
      my $_data = (scalar @_) ? join('', @_) : $_;

      if ($self->{_wid}) {
         $self->_do_send(SENDTO_STDOUT, undef, \$_data);
      }
      else {
         local $\ = undef if (defined $\);
         print $_data;
      }
   }

   return;
}

sub printf {

   my $x = shift; my MCE $self = ref($x) ? $x : $MCE;

   if (defined (my $_fd = fileno($_[0]))) {
      my $_glob = shift; my $_fmt = shift || '%s';
      my $_data = (scalar @_) ? sprintf($_fmt, @_) : sprintf($_fmt, $_);

      _do_send_glob($self, $_glob, $_fd, \$_data);
   }
   else {
      my $_fmt  = shift || '%s';
      my $_data = (scalar @_) ? sprintf($_fmt, @_) : sprintf($_fmt, $_);

      if ($self->{_wid}) {
         $self->_do_send(SENDTO_STDOUT, undef, \$_data);
      }
      else {
         local $\ = undef if (defined $\);
         print $_data;
      }
   }

   return;
}

sub say {

   my $x = shift; my MCE $self = ref($x) ? $x : $MCE;

   if (defined (my $_fd = fileno($_[0]))) {
      my $_glob = shift;
      my $_data = (scalar @_) ? join("\n", @_) . "\n" : $_ . "\n";

      _do_send_glob($self, $_glob, $_fd, \$_data);
   }
   else {
      my $_data = (scalar @_) ? join("\n", @_) . "\n" : $_ . "\n";

      if ($self->{_wid}) {
         $self->_do_send(SENDTO_STDOUT, undef, \$_data);
      }
      else {
         local $\ = undef if (defined $\);
         print $_data;
      }
   }

   return;
}

###############################################################################
## ----------------------------------------------------------------------------
## Private methods.
##
###############################################################################

{
   my $_ncpu;

   sub _parse_max_workers {

      my MCE $self = shift;
      return unless ($self);

      if ($self->{max_workers} =~ /^auto(?:$|\s*([\-\+\/\*])\s*(.+)$)/i) {
         require  MCE::Util unless (defined $MCE::Util::VERSION);
         $_ncpu = MCE::Util::get_ncpu() unless ($_ncpu);

         if ($1 && $2) {
            local $@; $self->{max_workers} = eval "int($_ncpu $1 $2 + 0.5)";

            $self->{max_workers} = 1
               if (!$self->{max_workers} || $self->{max_workers} < 1);
         }
         else {
            $self->{max_workers} = $_ncpu;
         }
      }

      return;
   }
}

sub _die  { MCE::Signal->_die_handler(@_);  }
sub _warn { MCE::Signal->_warn_handler(@_); }

sub _croak {

   $SIG{__DIE__}  = \&MCE::_die;
   $SIG{__WARN__} = \&MCE::_warn;

   $\ = undef; require Carp; goto &Carp::croak;

   return;
}

sub _NOOP { }

###############################################################################
## ----------------------------------------------------------------------------
## Create socket pair.
##
###############################################################################

sub _create_socket_pair {

   my MCE $self  = $_[0]; my $_r_sock = $_[1]; my $_w_sock = $_[2];
   my $_shutdown = $_[3]; my $_i      = $_[4];

   @_ = ();

   die "Private method called" unless (caller)[0]->isa( ref($self) );

   if (defined $_i) {
      socketpair( $self->{$_r_sock}->[$_i], $self->{$_w_sock}->[$_i],
         AF_UNIX, SOCK_STREAM, PF_UNSPEC ) or die "socketpair: $!\n";

      binmode $self->{$_r_sock}->[$_i];
      binmode $self->{$_w_sock}->[$_i];

      if ($_shutdown) {
         CORE::shutdown $self->{$_r_sock}->[$_i], 1;   ## No more writing
         CORE::shutdown $self->{$_w_sock}->[$_i], 0;   ## No more reading
      }

      ## Autoflush handles.
      my $_old_hndl = select $self->{$_r_sock}->[$_i]; $| = 1;
                      select $self->{$_w_sock}->[$_i]; $| = 1;

      select $_old_hndl;
   }
   else {
      socketpair( $self->{$_r_sock}, $self->{$_w_sock},
         AF_UNIX, SOCK_STREAM, PF_UNSPEC ) or die "socketpair: $!\n";

      binmode $self->{$_r_sock};
      binmode $self->{$_w_sock};

      if ($_shutdown) {
         CORE::shutdown $self->{$_r_sock}, 1;          ## No more writing
         CORE::shutdown $self->{$_w_sock}, 0;          ## No more reading
      }

      ## Autoflush handles.
      my $_old_hndl = select $self->{$_r_sock}; $| = 1;
                      select $self->{$_w_sock}; $| = 1;

      select $_old_hndl;
   }

   return;
}

###############################################################################
## ----------------------------------------------------------------------------
## Sync methods.
##
###############################################################################

sub _sync_buffer_to_array {

   my $_buffer_ref = $_[0]; my $_array_ref = $_[1];
   my $_cnt = 0;

   open my $_MEM_FILE, '<', $_buffer_ref;
   binmode $_MEM_FILE;
   $_array_ref->[$_cnt++] = $_ while (<$_MEM_FILE>);
   close $_MEM_FILE; undef $_MEM_FILE;

   delete @{ $_array_ref }[$_cnt .. @$_array_ref - 1]
      if ($_cnt < @$_array_ref);

   return;
}

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

###############################################################################
## ----------------------------------------------------------------------------
## Worker process -- Wrap.
##
###############################################################################

sub _worker_wrap {

   $MCE = $_[0];

   return _worker_main(@_, \@_plugin_worker_init);
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

   my $_thr = threads->create( \&_worker_wrap,
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

   select(undef, undef, undef, $self->{spawn_delay})
      if (defined $self->{spawn_delay} && $self->{spawn_delay} > 0.0);

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

   my $_pid = fork();

   _croak("MCE::_dispatch_child: Failed to spawn worker $_wid: $!")
      unless (defined $_pid);

   unless ($_pid) {
      _worker_wrap($self, $_wid, $_task, $_task_id, $_task_wid, $_params);
      kill 9, $$ unless ($MCE::_is_WinEnv);
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

   select(undef, undef, undef, $self->{spawn_delay})
      if (defined $self->{spawn_delay} && $self->{spawn_delay} > 0.0);

   return;
}

###############################################################################
## ----------------------------------------------------------------------------
## Import routine.
##
###############################################################################

package MCE::Core;

our $VERSION = '1.499_002'; $VERSION = eval $VERSION;

my $_loaded;

sub import {

   my $class = shift; return if ($_loaded++);

   ## This module does not load the threads module. Please include your
   ## threading library of choice prir to including MCE library. This is
   ## only a requirement if you're wanting to use threads versus forking.

   unless (defined $MCE::_has_threads) {
      if (defined $threads::VERSION) {
         unless (defined $threads::shared::VERSION) {
            local $@; local $SIG{__DIE__} = \&_NOOP;
            eval 'use threads::shared; threads::shared::share($MCE::_MCE_LOCK)';
         }
         $MCE::_has_threads = 1;
      }
      $MCE::_has_threads = $MCE::_has_threads || 0;
   }

   ## Preload essential modules early on.
   require MCE::Core::Validation;
   require MCE::Core::Manager;
   require MCE::Core::Worker;

   ## Instantiate a module-level instance.
   $MCE::MCE = MCE->new( _module_instance => 1, max_workers => 0 );

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

MCE::Core - Provides the core API for Many-core Engine

=head1 VERSION

This document describes MCE::Core version 1.499_002

=head1 SYNOPSIS

This is a simplistic use case of MCE running with 4 workers.

   use MCE;

   my $mce = MCE->new(
      max_workers => 4,
      user_func => sub {
         my ($self) = @_;
         print "Hello from ", $self->wid, "\n";
      }
   );

   $mce->run;

      ## All public methods beginning with MCE 1.5 can be called
      ## directly via the package method e.g. MCE->wid, MCE->run.

   -- Output

   Hello from 3
   Hello from 1
   Hello from 2
   Hello from 4

=head2 EXPORT_CONST, CONST

The anonymous user_func above can be written using one less line. There are
3 constants defined in MCE which are exportable. Using the constants in lieu
of 0,1,2 makes it more legible when accessing the array elements.

=head3 SELF CHUNK CID

   ## The following exports SELF => 0, CHUNK => 1, CID => 2

   use MCE EXPORT_CONST => 1;
   use MCE CONST => 1;              ## Same thing in 1.415 and later

   user_func => sub {
    # my ($self, $chunk_ref, $chunk_id) = @_;
      print "Hello from ", $_[SELF]->wid, "\n";
   }

   ## MCE 1.5 allows any public method to be be called directly.

   use MCE;

   user_func => sub {
    # my ($self, $chunk_ref, $chunk_id) = @_;
      print "Hello from ", MCE->wid, "\n";
   }

=head2 new ( [ options ] )

Below, a new instance is configured with all available options.

   use MCE;

   my $mce = MCE->new(

      max_workers  => 8,                 ## Default $MCE::MAX_WORKERS

          ## Number of workers to spawn. This can be set automatically
          ## with MCE 1.412 and later releases.

          ## max_workers => 'auto',      ## = MCE::Util::get_ncpu()
          ## max_workers => 'Auto-1',    ## = MCE::Util::get_ncpu() - 1
          ## max_workers => 'AUTO + 3',  ## = MCE::Util::get_ncpu() + 3
          ## max_workers => 'AUTO * 1.5',
          ## max_workers => 'auto / 1.333 + 2',

      chunk_size   => 2000,              ## Default $MCE::CHUNK_SIZE

          ## Less than or equal to 8192 is number of records.
          ## Greater than 8192 is number of bytes. MCE reads
          ## till the end of record before calling user_func.

          ## chunk_size =>     1,        ## Consists of 1 record
          ## chunk_size =>  1000,        ## Consists of 1000 records
          ## chunk_size => 16384,        ## Approximate 16384 bytes
          ## chunk_size => 50000,        ## Approximate 50000 bytes

      tmp_dir      => $tmp_dir,          ## Default $MCE::TMP_DIR

          ## Default is $MCE::Signal::tmp_dir which points to
          ## $ENV{TEMP} if defined. Otherwise, tmp_dir points
          ## to a location under /tmp.

      freeze       => \&encode_sereal,   ## Default $MCE::FREEZE
      thaw         => \&decode_sereal,   ## Default $MCE::THAW

          ## Release 1.412 allows freeze and thaw to be overridden.
          ## Just include a serialization module prior to loading MCE.

          ## use Sereal qw(encode_sereal decode_sereal);
          ## use JSON::XS qw(encode_json decode_json);
          ## use MCE;

      gather       => \@a,               ## Default undef

          ## Release 1.5 allows for gathering of data to an array
          ## reference, a MCE::Queue or Thread::Queue object, or
          ## a code reference. One invokes gathering by calling
          ## the gather method as often as needed.

          ## gather => $Q,
          ## gather => \&preserve_order,

      input_data   => $input_file,       ## Default undef
      RS           => "\n::\n",          ## Default undef

          ## input_data => '/path/to/file' for input file
          ## input_data => \@array for input array
          ## input_data => \*FILE_HNDL for file handle
          ## input_data => \$scalar to treat like a file

          ## Use the sequence option if simply wanting to loop
          ## through a sequence of numbers instead.

          ## Release 1.4 and later allows one to specify the
          ## record separator (RS) applicable to input files,
          ## file handles, and scalar references.

      use_slurpio  => 1,                 ## Default 0
      use_threads  => 1,                 ## Default 0 or 1

          ## Whether or not to enable slurpio when reading files
          ## (passes raw chunk to user function).

          ## By default MCE does forking (spawns child processes).
          ## MCE also supports threads via 2 threading libraries.
          ##
          ## The use of threads in MCE requires that you include
          ## threads support prior to loading MCE. The use_threads
          ## option defaults to 1 when a thread library is present.
          ##
          ##    use threads;                  use forks;
          ##    use threads::shared;   (or)   use forks::shared;
          ##
          ##    use MCE                       use MCE;

      spawn_delay  => 0.035,             ## Default undef
      submit_delay => 0.002,             ## Default undef
      job_delay    => 0.150,             ## Default undef

          ## Time to wait, in fractional seconds, before spawning
          ## each worker, parameters submission to each worker,
          ## and job commencement (staggered) for each worker.
          ## For example, use job_delay when wanting to stagger
          ## many workers connecting to a database.

      on_post_exit => \&on_post_exit,    ## Default undef
      on_post_run  => \&on_post_run,     ## Default undef

          ## Execute code block immediately after a worker exits
          ## (exit, MCE->exit, or die). Execute code block after
          ## running a job (MCE->process or MCE->run).

          ## One can take action immediately after a worker exits
          ## or wait until after the job has completed.

      user_args    => { env => 'test' }, ## Default undef

          ## MCE release 1.4 adds a new parameter to allow one to
          ## specify arbitrary arguments such as a string, an ARRAY
          ## or a HASH reference. Workers can access this directly:
          ##    my $args = $self->{user_args};

      user_begin   => \&user_begin,      ## Default undef
      user_func    => \&user_func,       ## Default undef
      user_end     => \&user_end,        ## Default undef

          ## Think of user_begin, user_func, user_end like the awk
          ## scripting language:
          ##    awk 'BEGIN { ... } { ... } END { ... }'

          ## MCE workers call user_begin once per job, then
          ## call user_func repeatedly until no chunks remain.
          ## Afterwards, user_end is called.

      user_error   => \&user_error,      ## Default undef
      user_output  => \&user_output,     ## Default undef

          ## When workers call the following functions, MCE will
          ## pass the data to user_error/user_output if defined.
          ## MCE->sendto('stderr', 'Sending to STDERR');
          ## MCE->sendto('stdout', 'Sending to STDOUT');

      stderr_file  => 'err_file',        ## Default STDERR
      stdout_file  => 'out_file',        ## Default STDOUT

          ## Or to file. User_error/user_output take precedence.

      flush_file   => 1,                 ## Default 0
      flush_stderr => 1,                 ## Default 0
      flush_stdout => 1,                 ## Default 0

          ## Flush sendto file, standard error, or standard output.

      interval     => {
          delay => 0.005 [, max_nodes => 4, node_id => 1 ]
      },

          ## For use with the yield method introduced in MCE 1.5.
          ## Both max_nodes & node_id are optional and default to 1.
          ## Delay is the amount of time between intervals.

      sequence     => {                  ## Default undef
          begin => -1, end => 1 [, step => 0.1 [, format => "%4.1f" ] ]
      },

          ## For looping through a sequence of numbers in parallel.
          ## STEP, if omitted, defaults to 1 if BEGIN is smaller than
          ## END or -1 if BEGIN is greater than END. The FORMAT string
          ## is passed to sprintf behind the scene (% can be omitted).
          ## e.g. $seq_n_formated = sprintf("%4.1f", $seq_n);

          ## Leave input_data set to undef when specifying the sequence
          ## option. One cannot specify both options together.

          ## Release 1.4 allows one to specify an array reference
          ## instead (3rd & 4th values are optional):
          ##
          ##    sequence => [ -1, 1, 0.1, "%4.1f" ]

      task_end     => \&task_end,        ## Default undef

          ## MCE 1.5 allows this to be specified at the top level.
          ## This is called by the manager process after the task
          ## has completed processing.

      task_name    => 'string',          ## Default 'MCE'

          ## Added in MCE 1.5. This is beneficial for user_tasks.
          ## Each task can be specified with a different name value.
          ## It allows for task_end to be specified at the top level.
          ## The task_name value is passed as the 3rd arg to task_end.

      user_tasks   => [                  ## Default undef
         { ... },                        ## Options for task 0
         { ... },                        ## Options for task 1
         { ... },                        ## Options for task 2
      ],

          ## Takes a list of hash references, each allowing up to 12
          ## options. All other MCE options are ignored. Input_data
          ## is applicable for the first task only.
          ##   task_name, max_workers, chunk_size, input_data, sequence,
          ##   task_end, user_args, user_begin, user_end, user_func,
          ##   gather, use_threads

          ## Options not specified here will default to same option
          ## specified at the top level.

   );

=head2 OVERRIDING DEFAULTS

The following list 5 options which may be overridden when loading the module.

   use Sereal qw(encode_sereal decode_sereal);

   use MCE max_workers => 4,                    ## Default 1
           chunk_size  => 100,                  ## Default 1
           tmp_dir     => "/path/to/app/tmp",   ## $MCE::Signal::tmp_dir
           freeze      => \&encode_sereal,      ## \&Storable::freeze
           thaw        => \&decode_sereal       ## \&Storable::thaw
   ;

   my $mce = MCE->new( ... );

There is a simplier way to enable Sereal with MCE 1.5. The following will
attempt to use Sereal if available, otherwise will default back to using
Storable for serialization.

   use MCE Sereal => 1;

   ## Serialization is through Sereal if available.
   my $mce = MCE->new( ... );

=head2 RUNNING

Run calls spawn, kicks off job, workers call user_begin, user_func, and
user_end. Run shuts down workers afterwards. Call the spawn method early
whenever the need arises for large data structures within the main process
prior to running.

   MCE->spawn();                         ## This is optional

   MCE->run();                           ## Call run or process below

   ## Acquire data arrays and/or input_files. The same pool of
   ## workers are used.

   MCE->process(\@input_data_1);         ## Process arrays
   MCE->process(\@input_data_2);
   MCE->process(\@input_data_n);

   MCE->process('input_file_1');         ## Process files
   MCE->process('input_file_2');
   MCE->process('input_file_n');

   ## Shutdown workers afterwards.

   MCE->shutdown();

=head2 SYNTAX for ON_POST_EXIT

Often times, one may want to capture the exit status. The on_post_exit option,
if defined, is executed immediately after a worker exits via exit (children),
MCE->exit (children and threads), or die.

The format of $e->{pid} is PID_123 for children and THR_123 for threads.

   sub on_post_exit {
      my ($self, $e) = @_;
      print "$e->{wid}: $e->{pid}: $e->{status}: $e->{msg}: $e->{id}\n";
   }

   sub user_func {
      my $self = $_[0];
      MCE->exit(0, 'ok', 'pebbles');
   }

   my $mce = MCE->new(
      on_post_exit => \&on_post_exit,
      user_func    => \&user_func
   );

   MCE->run();

   -- Output

   2: PID_7625: 0: ok: pebbles

=head2 SYNTAX for ON_POST_RUN

The on_post_run option, if defined, is executed immediately after running
MCE->process or MCE->run. This option receives an array reference of hashes.

The difference between on_post_exit and on_post_run is that the former is
called immediately whereas the latter is called after all workers have
completed processing or running.

   sub on_post_run {
      my ($self, $status_ref) = @_;
      foreach my $e ( @{ $status_ref } ) {
         print "$e->{wid}: $e->{pid}: $e->{status}: $e->{msg}: $e->{id}\n";
      }
   }

   sub user_func {
      my $self = $_[0];
      MCE->exit(0, 'ok', 'pebbles');
   }

   my $mce = MCE->new(
      on_post_run => \&on_post_run,
      user_func   => \&user_func
   );

   MCE->run();

=head2 SYNTAX for SEQUENCE

The 1.3 release and above allows workers to loop through a sequence of numbers
computed mathematically without the overhead of an array. The sequence can be
specified separately per each user_task entry unlike input_data which is
applicable to the first task only.

See the seq_demo.pl example, included with this distribution, on applying
sequences with the user_tasks option including chunking.

Sequence can be specified using an array or a hash reference.

   use MCE;

   my $mce = MCE->new(
      max_workers => 3,

    # sequence => [ 10, 19, 0.7, "%4.1f" ],

      sequence => {
         begin => 10, end => 19, step => 0.7, format => "%4.1f"
      },

      user_func => sub {
         my ($self, $n, $chunk_id) = @_;
         print $n, " from ", MCE->wid(), " id ", $chunk_id, "\n";
      }
   );

   MCE->run();

   -- Output (sorted afterwards, notice wid and chunk_id in output)

   10.0 from 1 id 1
   10.7 from 2 id 2
   11.4 from 3 id 3
   12.1 from 1 id 4
   12.8 from 2 id 5
   13.5 from 3 id 6
   14.2 from 1 id 7
   14.9 from 2 id 8
   15.6 from 3 id 9
   16.3 from 1 id 10
   17.0 from 2 id 11
   17.7 from 3 id 12
   18.4 from 1 id 13

=head2 SYNTAX for USER_BEGIN and USER_END

The user_begin and user_end options, if specified, behave similarly to
awk 'BEGIN { ... } { ... } END { ... }'. These are called once per each
worker during a run.

   sub user_begin {                   ## Called once at the beginning
      my $self = shift;
      $self->{wk_total_rows} = 0;
   }

   sub user_func {                    ## Called while processing
      my $self = shift;
      $self->{wk_total_rows} += 1;
   }

   sub user_end {                     ## Called once at the end
      my $self = shift;
      printf "## %d: Processed %d rows\n",
         MCE->wid(), $self->{wk_total_rows};
   }

   my $mce = MCE->new(
      user_begin => \&user_begin,
      user_func  => \&user_func,
      user_end   => \&user_end
   );

   MCE->run();

=head2 SYNTAX for USER_FUNC with USE_SLURPIO => 0

When processing input data, MCE can pass an array or rows or a slurped chunk.
Below, a reference to an array containing the chunk data is processed.

e.g. $chunk_ref = [ record1, record2, record3, ... ]

   sub user_func {

      my ($self, $chunk_ref, $chunk_id) = @_;

      foreach my $row ( @{ $chunk_ref } ) {
         $self->{wk_total_rows} += 1;
         print $row;
      }
   }

   my $mce = MCE->new(
      chunk_size  => 100,
      input_data  => "/path/to/file",
      user_func   => \&user_func,
      use_slurpio => 0
   );

   MCE->run();

=head2 SYNTAX for USER_FUNC with USE_SLURPIO => 1

Here, a reference to a scalar containing the raw chunk data is processed.

   sub user_func {

      my ($self, $chunk_ref, $chunk_id) = @_;

      my $count = () = $$chunk_ref =~ /abc/;
   }

   my $mce = MCE->new(
      chunk_size  => 16000,
      input_data  => "/path/to/file",
      user_func   => \&user_func,
      use_slurpio => 1
   );

   MCE->run();

=head2 SYNTAX for USER_ERROR and USER_OUTPUT

Output coming from MCE->sendto('stderr/stdout', ...) can be intercepted by
specifying the user_error and user_output options. MCE on receiving output
will instead direct to user_error and user_output in a serialized fashion.
Handy when wanting to filter, modify, and/or direct the output elsewhere.

   sub user_error {                   ## Redirect STDERR to STDOUT
      my $error = shift;
      print STDOUT $error;
   }

   sub user_output {                  ## Redirect STDOUT to STDERR
      my $output = shift;
      print STDERR $output;
   }

   sub user_func {
      my ($self, $chunk_ref, $chunk_id) = @_;
      my $count = 0;

      foreach my $row ( @{ $chunk_ref } ) {
         MCE->sendto('stdout', $row);
         $count += 1;
      }

      MCE->sendto('stderr', "$chunk_id: processed $count rows\n");
   }

   my $mce = MCE->new(
      chunk_size  => 1000,
      input_data  => "/path/to/file",
      user_error  => \&user_error,
      user_output => \&user_output,
      user_func   => \&user_func
   );

   MCE->run();

=head2 SYNTAX for USER_TASKS and TASK_END

This option takes an array of tasks. Each task allows for 12 MCE options.
Input_data can be specified inside the first task or at the top level only,
otherwise is ignored.

   max_workers, chunk_size, input_data, sequence, task_end, task_name,
   gather, user_args, user_begin, user_end, user_func, use_threads

Sequence and chunk_size were added in 1.3. User_args was introduced in 1.4.
Name and input_data are new options allowed in 1.5. In addition, one can
specify task_end at the top level. Task_end also receives 2 additional
arguments $task_id and $task_name (shown below).

Options not specified here will default to the same option specified at the
top level. The task_end option is called by the manager process when all
workers for that role have completed processing.

Forking and threading can be intermixed among tasks unless running Cygwin.
The run method will continue running until all workers have completed
processing.

   use MCE;

   my $mce = MCE->new(
      input_data => $list_file,

      task_end   => sub {
         my ($self, $task_id, $task_name) = @_;
         print "Task [$task_id -- $task_name] completed processing\n";
      },

      user_tasks => [{
         task_name   => 'a',
         max_workers => 2,
         user_func   => \&parallel_task1,
         use_threads => 0

      },{
         task_name   => 'b',
         max_workers => 4,
         user_func   => \&parallel_task2,
         use_threads => 1

      }]
   );

   MCE->run();

=head1 DEFAULT INPUT SCALAR

Beginning with MCE 1.5, the input scalar $_ is localized prior to calling
user_func for input_data and sequence of numbers. The following applies.

=over 5

=item use_slurpio => 1

   $_ is a reference to the buffer e.g. $_ = \$_buffer;
   $_ is a ref irregardless of whether chunk_size is 1 or greater

   user_func => sub {
    # my ($self, $chunk_ref, $chunk_id) = @_;
      print ${ $_ };    ## $_ is same as $chunk_ref
   }

=item chunk_size is greater than 1, use_slurpio => 0

   $_ is a reference to an array. $_ = \@_records; $_ = \@_seq_n;
   $_ is same as $chunk_ref or $_[CHUNK]

   user_func => sub {
    # my ($self, $chunk_ref, $chunk_id) = @_;
      for my $row ( @{ $_ } ) {
         print $row, "\n";
      }
   }

   use MCE CONST => 1;

   user_func => sub {
    # my ($self, $chunk_ref, $chunk_id) = @_;
      for my $row ( @{ $_[CHUNK] } ) {
         print $row, "\n";
      }
   }

=item chunk_size equals 1, use_slurpio => 0

   $_ contains the actual value. $_ = $_buffer; $_ = $seq_n;

   ## Note that $_ and $chunk_ref are not the same below.
   ## $chunk_ref is a reference to an array.

   user_func => sub {
    # my ($self, $chunk_ref, $chunk_id) = @_;
      print $_, "\n;    ## Same as $chunk_ref->[0];
   }

   MCE->foreach("/path/to/file", sub {
    # my ($self, $chunk_ref, $chunk_id) = @_;
      print $_;         ## Same as $chunk_ref->[0];
   });

   ## However, that's not the case for the forseq method.
   ## Both $_ and $n_seq are the same when chunk_size => 1.

   MCE->forseq([ 1, 9 ], sub {
    # my ($self, $n_seq, $chunk_id) = @_;
      print $_, "\n";   ## Same as $n_seq
   });

Sequence can also be specified using an array reference. The below is the same
as the example afterwards.

   MCE->forseq( { begin => 10, end => 40, step => 2 }, ... );

The code block receives an array containing the next 5 sequences. Chunk 1
(chunk_id = 1) contains 10,12,14,16,18. $n_seq is a reference to an array,
same as $_, due to chunk_size being greater than 1.

   MCE->forseq( [ 10, 40000, 2 ], { chunk_size => 5 }, sub {
    # my ($self, $n_seq, $chunk_id) = @_;
      my @result;
      for my $n ( @{ $_ } ) {
         ... do work, append to result for 5
      }
      ... do something with result afterwards
   });

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

