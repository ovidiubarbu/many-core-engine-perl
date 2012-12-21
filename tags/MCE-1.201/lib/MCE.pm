###############################################################################
## ----------------------------------------------------------------------------
## Many-Core Engine (MCE) for Perl. Provides parallel processing capabilities.
##
###############################################################################

package MCE;

my ($_que_template, $_que_read_size);
my ($_has_threads, $_max_files, $_max_procs);

BEGIN {
   ## Configure template to use for pack/unpack for writing to and reading from
   ## the queue. Each entry contains 2 positive numbers: chunk_id & msg_id.
   ## Attempt 64-bit size, otherwize fall back to host machine's word length.
   {
      local $@; local $SIG{__DIE__} = \&_NOOP;
      eval { $_que_read_size = length(pack('Q2', 0, 0)); };
      $_que_template = ($@) ? 'I2' : 'Q2';
      $_que_read_size = length(pack($_que_template, 0, 0));
   }

   ## Determine the underlying maximum number of files.
   if ($^O eq 'MSWin32') {
      $_max_files = $_max_procs = 256;
   }
   else {
      my $_bash = (-x '/bin/bash') ? '/bin/bash' : undef;
         $_bash = '/usr/bin/bash' if (!$_bash && -x '/usr/bin/bash');

      if ($_bash) {
         my $_res = `$_bash -c 'ulimit -n; ulimit -u'`;  ## max files, procs

         $_res =~ /^(\S+)\s(\S+)/m;
         $_max_files = $1 || 256;
         $_max_procs = $2 || 256;

         $_max_files = ($_max_files =~ /\A\d+\z/) ? $_max_files : 3152;
         $_max_procs = ($_max_procs =~ /\A\d+\z/) ? $_max_procs :  788;
      }
      else {
         $_max_files = $_max_procs = 256;
      }
   }

   ## Limit to 3152 and 788 for MCE.
   $_max_files = 3152 if ($_max_files > 3152);
   $_max_procs =  788 if ($_max_procs >  788);
}

###############################################################################
## ----------------------------------------------------------------------------
## This module does not load the threads module. Please include your threading
## library of choice prir to including MCE library. This is only a requirement
## if you're wanting to use threads versus forking.
##
###############################################################################

my ($_COM_LOCK, $_DAT_LOCK);
our $_MCE_LOCK : shared = 1;

INIT {
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

use strict;
use warnings;

our $VERSION = '1.201';
$VERSION = eval $VERSION;

use Fcntl qw( :flock O_CREAT O_TRUNC O_RDWR O_RDONLY );
use Storable 2.04 qw( store retrieve freeze thaw );
use Socket qw( :DEFAULT :crlf );

use MCE::Signal;

###############################################################################
## ----------------------------------------------------------------------------
## Define constants & variables.
##
###############################################################################

use constant {
   MAX_CHUNK_SIZE   => 24 * 1024 * 1024, ## Set max constraints
   MAX_OPEN_FILES   => $_max_files,
   MAX_USER_PROCS   => $_max_procs,

   MAX_RECS_SIZE    => 8192,             ## Read # of records if <= value
                                         ## Read # of bytes   if >  value

   QUE_TEMPLATE     => $_que_template,   ## Pack template for queue socket
   QUE_READ_SIZE    => $_que_read_size,  ## Read size

   OUTPUT_W_EXT     => ':W~EXT',         ## Worker has exited job
   OUTPUT_W_DNE     => ':W~DNE',         ## Worker has completed job
   OUTPUT_W_END     => ':W~END',         ## Worker has ended prematurely

   OUTPUT_A_ARY     => ':A~ARY',         ## Array  << Array
   OUTPUT_S_GLB     => ':S~GLB',         ## Scalar << Glob FH
   OUTPUT_A_CBK     => ':A~CBK',         ## Callback (/w arguments)
   OUTPUT_N_CBK     => ':N~CBK',         ## Callback (no arguments)
   OUTPUT_S_CBK     => ':S~CBK',         ## Callback (1 scalar arg)
   OUTPUT_S_OUT     => ':S~OUT',         ## Scalar >> STDOUT
   OUTPUT_S_ERR     => ':S~ERR',         ## Scalar >> STDERR
   OUTPUT_S_FLE     => ':S~FLE',         ## Scalar >> File

   READ_FILE        => 0,                ## Worker reads file handle
   READ_MEMORY      => 1,                ## Worker reads memory handle

   REQUEST_ARRAY    => 0,                ## Worker requests next array chunk
   REQUEST_GLOB     => 1,                ## Worker requests next glob chunk

   SENDTO_FILEV1    => 0,                ## Worker sends to 'file', $a, '/path'
   SENDTO_FILEV2    => 1,                ## Worker sends to 'file:/path', $a
   SENDTO_STDOUT    => 2,                ## Worker sends to STDOUT
   SENDTO_STDERR    => 3,                ## Worker sends to STDERR

   WANTS_UNDEFINE   => 0,                ## Callee wants nothing
   WANTS_ARRAY      => 1,                ## Callee wants list
   WANTS_SCALAR     => 2,                ## Callee wants scalar
   WANTS_REFERENCE  => 3                 ## Callee wants H/A/S ref
};

undef $_max_files; undef $_max_procs;
undef $_que_template; undef $_que_read_size;

my %_valid_fields = map { $_ => 1 } qw(
   chunk_size input_data max_workers use_slurpio use_threads
   flush_file flush_stderr flush_stdout stderr_file stdout_file
   job_delay spawn_delay submit_delay tmp_dir user_tasks task_end
   user_begin user_end user_func user_error user_output

   _mce_sid _mce_tid _pids _sess_dir _spawned _task0_max_workers _tids _wid
   _com_r_sock _com_w_sock _dat_r_sock _dat_w_sock _out_r_sock _out_w_sock
   _que_r_sock _que_w_sock _abort_msg _run_mode _single_dim _task_id
);

my %_params_allowed_args = map { $_ => 1 } qw(
   chunk_size input_data job_delay spawn_delay submit_delay use_slurpio
   flush_file flush_stderr flush_stdout stderr_file stdout_file
   user_begin user_end user_func user_error user_output
);

my $_is_cygwin   = ($^O eq 'cygwin');
my $_is_winperl  = ($^O eq 'MSWin32');
my $_mce_tmp_dir = $MCE::Signal::tmp_dir;
my %_mce_spawned = ();
my $_mce_count   = 0;

$MCE::Signal::mce_spawned_ref = \%_mce_spawned;

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

   my $self = { };

   bless $self, $class;

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

   ## Validation.
   for (keys %argv) {
      _croak("MCE::new: '$_' is not a valid constructor argument")
         unless (exists $_valid_fields{$_});
   }

   _croak("MCE::new: '$self->{tmp_dir}' is not a directory or does not exist")
      unless (-d $self->{tmp_dir});
   _croak("MCE::new: '$self->{tmp_dir}' is not writeable")
      unless (-w $self->{tmp_dir});

   _validate_args($self);

   %argv = ();

   ## Private options.
   @{ $self->{_pids} }  = ();    ## Array for joining children when completed
   @{ $self->{_tids} }  = ();    ## Array for joining threads when completed
   $self->{_mce_sid}    = undef; ## Spawn ID defined at time of spawning
   $self->{_mce_tid}    = undef; ## Thread ID when spawn was called
   $self->{_sess_dir}   = undef; ## Unique session dir when spawn was called
   $self->{_spawned}    = 0;     ## 1 equals workers have been spawned
   $self->{_wid}        = 0;     ## Unique Worker ID
   $self->{_com_r_sock} = undef; ## Communication channel for MCE
   $self->{_com_w_sock} = undef; ## Communication channel for workers
   $self->{_dat_r_sock} = undef; ## Data channel for MCE
   $self->{_dat_w_sock} = undef; ## Data channel for workers
   $self->{_out_r_sock} = undef; ## For serialized reads by main thread/process
   $self->{_out_w_sock} = undef; ## Workers write to this for serialized writes
   $self->{_que_r_sock} = undef; ## Queue channel for MCE
   $self->{_que_w_sock} = undef; ## Queue channel for workers

   ## -------------------------------------------------------------------------

   ## Limit chunk_size.
   $self->{chunk_size} = MAX_CHUNK_SIZE
      if ($self->{chunk_size} > MAX_CHUNK_SIZE);

   ## Adjust max_workers -- allow for some headroom.
   if ($^O eq 'MSWin32') {
      $self->{max_workers} = MAX_OPEN_FILES
         if ($self->{max_workers} > MAX_OPEN_FILES);
   }
   else {
      if ($self->{use_threads}) {
         $self->{max_workers} = int(MAX_OPEN_FILES / 2) - 32
            if ($self->{max_workers} > int(MAX_OPEN_FILES / 2) - 32);
      }
      else {
         $self->{max_workers} = MAX_USER_PROCS - 64
            if ($self->{max_workers} > MAX_USER_PROCS - 64);
      }
   }

   if ($^O eq 'cygwin') {                 ## Limit to 48 threads, 24 children
      $self->{max_workers} = 48
         if ($self->{use_threads} && $self->{max_workers} > 48);
      $self->{max_workers} = 24
         if (!$self->{use_threads} && $self->{max_workers} > 24);
   }
   elsif ($^O eq 'MSWin32') {             ## Limit to 96 threads, 48 children
      $self->{max_workers} = 96
         if ($self->{use_threads} && $self->{max_workers} > 96);
      $self->{max_workers} = 48
         if (!$self->{use_threads} && $self->{max_workers} > 48);
   }

   return $self;
}

###############################################################################
## ----------------------------------------------------------------------------
## Spawn Routine.
##
###############################################################################

sub spawn {

   my MCE $self = $_[0];

   ## To avoid leaking (Scalars leaked: 1) messages (fixed in Perl 5.12.x).
   @_ = ();

   ## Croak if method was called by a MCE worker.
   _croak("MCE::spawn: method cannot be called by a MCE worker")
      if ($self->wid());

   ## Return if workers have already been spawned.
   return $self unless ($self->{_spawned} == 0);

   lock $_MCE_LOCK if ($_has_threads);            ## Obtain MCE lock.

   my $_die_handler  = $SIG{__DIE__};  $SIG{__DIE__}  = \&_die;
   my $_warn_handler = $SIG{__WARN__}; $SIG{__WARN__} = \&_warn;

   ## Delay spawning.
   select(undef, undef, undef, $self->{spawn_delay})
      if ($self->{spawn_delay} && $self->{spawn_delay} > 0.0);

   ## Configure tid/sid for this instance here, not in the new method above.
   ## We want the actual thread id in which spawn was called under.
   unless ($self->{_mce_tid}) {
      $self->{_mce_tid} = ($_has_threads) ? threads->tid() : '';
      $self->{_mce_tid} = '' unless (defined $self->{_mce_tid});
      $self->{_mce_sid} = $$ .'.'. $self->{_mce_tid} .'.'. (++$_mce_count);
   }

   ## Local vars.
   my $_mce_sid      = $self->{_mce_sid};
   my $_mce_tid      = $self->{_mce_tid};
   my $_sess_dir     = $self->{_sess_dir};
   my $_tmp_dir      = $self->{tmp_dir};
   my $_max_workers  = $self->{max_workers};
   my $_use_threads  = $self->{use_threads};

   ## Create temp dir.
   unless ($_sess_dir) {
      _croak("MCE::spawn: '$_tmp_dir' is not defined")
         if (!defined $_tmp_dir || $_tmp_dir eq '');
      _croak("MCE::spawn: '$_tmp_dir' is not a directory or does not exist")
         unless (-d $_tmp_dir);
      _croak("MCE::spawn: '$_tmp_dir' is not writeable")
         unless (-w $_tmp_dir);

      $_sess_dir = $self->{_sess_dir} = "$_tmp_dir/$_mce_sid";
      my $_cnt = 0;

      while ( !(mkdir $_sess_dir, 0770) ) {
         $_sess_dir = $self->{_sess_dir} = "$_tmp_dir/$_mce_sid." . (++$_cnt);
      }
   }

   ## -------------------------------------------------------------------------

   ## Create socket pair for communication channels between MCE and workers.
   socketpair( $self->{_com_r_sock}, $self->{_com_w_sock},
      AF_UNIX, SOCK_STREAM, PF_UNSPEC ) or die "socketpair: $!\n";

   binmode $self->{_com_r_sock};                  ## Set binary mode
   binmode $self->{_com_w_sock};

   ## Create socket pair for data channels between MCE and workers.
   socketpair( $self->{_dat_r_sock}, $self->{_dat_w_sock},
      AF_UNIX, SOCK_STREAM, PF_UNSPEC ) or die "socketpair: $!\n";

   binmode $self->{_dat_r_sock};                  ## Set binary mode
   binmode $self->{_dat_w_sock};

   ## Create socket pair for serializing send requests and STDOUT output.
   socketpair( $self->{_out_r_sock}, $self->{_out_w_sock},
      AF_UNIX, SOCK_STREAM, PF_UNSPEC ) or die "socketpair: $!\n";

   binmode $self->{_out_r_sock};                  ## Set binary mode
   binmode $self->{_out_w_sock};

   shutdown $self->{_out_r_sock}, 1;              ## No more writing
   shutdown $self->{_out_w_sock}, 0;              ## No more reading

   ## Create socket pairs for queue channels between MCE and workers.
   socketpair( $self->{_que_r_sock}, $self->{_que_w_sock},
      AF_UNIX, SOCK_STREAM, PF_UNSPEC ) or die "socketpair: $!\n";

   binmode $self->{_que_r_sock};                  ## Set binary mode
   binmode $self->{_que_w_sock};

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

   my $_wid = 0;

   @{ $self->{_pids} } = ();
   @{ $self->{_tids} } = ();

   ## Obtain lock.
   open my $_COM_LOCK, '+>> :stdio', "$_sess_dir/com.lock";
   flock $_COM_LOCK, LOCK_EX;

   ## Spawn workers.
   unless (defined $self->{user_tasks}) {
      if (defined $_use_threads && $_use_threads == 1) {
         $self->_dispatch_thread(++$_wid) for (1 .. $_max_workers);
      } else {
         $self->_dispatch_child(++$_wid) for (1 .. $_max_workers);
      }

      $self->{_task0_max_workers} = $_max_workers;
   }
   else {
      if ($_is_cygwin) {
         ## File locking fails when being locked by both children & threads.
         ## Must be all children or all threads, but not both.
         my (%_values, $_value);

         for my $_task (@{ $self->{user_tasks} }) {
            $_value = (defined $_task->{use_threads})
               ? $_task->{use_threads} : $self->{use_threads};
            $_values{$_value} = '';
         }
         _croak("MCE::spawn: 'cannot mix' use_threads => 0/1 in Cygwin")
            if (keys %_values > 1);
      }

      my $_task_id = 0;

      for my $_task (@{ $self->{user_tasks} }) {
         $_task->{max_workers} = $self->{max_workers}
            unless (defined $_task->{max_workers});

         my $_use_threads = (defined $_task->{use_threads})
            ? $_task->{use_threads} : $self->{use_threads};

         if (defined $_use_threads && $_use_threads == 1) {
            $self->_dispatch_thread(++$_wid, $_task, $_task_id)
               for (1 .. $_task->{max_workers});
         } else {
            $self->_dispatch_child(++$_wid, $_task, $_task_id)
               for (1 .. $_task->{max_workers});
         }

         $_task_id++;
      }

      $self->{_task0_max_workers} = $self->{user_tasks}->[0]->{max_workers};
   }

   ## Release lock.
   flock $_COM_LOCK, LOCK_UN;
   close $_COM_LOCK; undef $_COM_LOCK;

   $SIG{__DIE__}  = $_die_handler;
   $SIG{__WARN__} = $_warn_handler;

   ## Update max workers spawned.
   $_mce_spawned{$_mce_sid} = $self;
   $self->{max_workers} = $_wid;
   $self->{_spawned} = 1;

   return $self;
}

###############################################################################
## ----------------------------------------------------------------------------
## Foreach Routine.
##
###############################################################################

sub foreach {

   my MCE $self    = $_[0];
   my $_input_data = $_[1];

   ## Croak if method was called by a MCE worker.
   _croak("MCE::foreach: method cannot be called by a MCE worker")
      if ($self->wid());

   ## Parse args.
   my ($_user_func, $_params_ref);

   if (ref $_[2] eq 'HASH') {
      $_user_func  = $_[3];
      $_params_ref = $_[2];
   }
   else {
      $_user_func  = $_[2];
      $_params_ref = {};
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
## Forchunk Routine.
##
###############################################################################

sub forchunk {

   my MCE $self    = $_[0];
   my $_input_data = $_[1];

   ## Croak if method was called by a MCE worker.
   _croak("MCE::forchunk: method cannot be called by a MCE worker")
      if ($self->wid());

   ## Parse args.
   my ($_user_func, $_params_ref);

   if (ref $_[2] eq 'HASH') {
      $_user_func  = $_[3];
      $_params_ref = $_[2];
   }
   else {
      $_user_func  = $_[2];
      $_params_ref = {};
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
## Process Routine.
##
###############################################################################

sub process {

   my MCE $self    = $_[0];
   my $_input_data = $_[1];
   my $_params_ref = $_[2] || undef;

   @_ = ();

   ## Croak if method was called by a MCE worker.
   _croak("MCE::process: method cannot be called by a MCE worker")
      if ($self->wid());

   ## Set input data.
   if (defined $_input_data) {
      $_params_ref->{input_data} = $_input_data;
   }
   else {
      _croak("MCE::process: 'input_data' is not specified")
   }

   ## Pass 0 to "not" auto-shutdown after processing.
   $self->run(0, $_params_ref);

   return $self;
}

###############################################################################
## ----------------------------------------------------------------------------
## Run Routine.
##
###############################################################################

sub run {

   my MCE $self = $_[0];

   ## Croak if method was called by a MCE worker.
   _croak("MCE::run: method cannot be called by a MCE worker")
      if ($self->wid());

   ## Parse args.
   my ($_auto_shutdown, $_params_ref);

   if (ref $_[1] eq 'HASH') {
      $_auto_shutdown = (defined $_[2]) ? $_[2] : 1;
      $_params_ref    = $_[1];
   }
   else {
      $_auto_shutdown = (defined $_[1]) ? $_[1] : 1;
      $_params_ref    = $_[2] || undef;
   }

   @_ = ();

   my $_requires_shutdown = 0;

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
   $self->shutdown()
      if ($_requires_shutdown || ref $self->{input_data} eq 'SCALAR');

   ## -------------------------------------------------------------------------

   ## Spawn workers.
   $self->spawn() if ($self->{_spawned} == 0);

   local $SIG{__DIE__}  = \&_die;
   local $SIG{__WARN__} = \&_warn;

   ## Delay job.
   select(undef, undef, undef, $self->{job_delay})
      if ($self->{job_delay} && $self->{job_delay} > 0.0);

   ## Local vars.
   my ($_input_data, $_input_file, $_input_glob);
   my ($_abort_msg, $_first_msg, $_run_mode, $_single_dim);

   my $_mce_tid     = $self->{_mce_tid};
   my $_sess_dir    = $self->{_sess_dir};
   my $_chunk_size  = $self->{chunk_size};
   my $_max_workers = $self->{max_workers};
   my $_use_slurpio = $self->{use_slurpio};

   ## Determine run mode for workers.
   if (defined $self->{input_data}) {
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

   ## Set flag on whether array is single dimension.
   $self->{_single_dim} = $_single_dim;

   ## -------------------------------------------------------------------------

   my %_params = (
      '_abort_msg'   => $_abort_msg,    '_run_mode'    => $_run_mode,
      '_chunk_size'  => $_chunk_size,   '_single_dim'  => $_single_dim,
      '_input_file'  => $_input_file,   '_use_slurpio' => $_use_slurpio,
      '_max_workers' => $_max_workers
   );
   my %_params_nodata = (
      '_abort_msg'   => undef,          '_run_mode'    => 'nodata',
      '_chunk_size'  => $_chunk_size,   '_single_dim'  => $_single_dim,
      '_input_file'  => $_input_file,   '_use_slurpio' => $_use_slurpio,
      '_max_workers' => $_max_workers
   );

   ## Begin processing.
   {
      local $\ = undef; local $/ = $LF;
      lock $_MCE_LOCK if ($_has_threads);            ## Obtain MCE lock.

      my $_COM_R_SOCK    = $self->{_com_r_sock};
      my $_submit_delay  = $self->{submit_delay};
      my $_wid;

      my $_frozen_params = freeze(\%_params);
      my $_frozen_nodata = freeze(\%_params_nodata)
         if (defined $self->{user_tasks});

      ## Obtain lock.
      open my $_DAT_LOCK, '+>> :stdio', "$_sess_dir/dat.lock";
      flock $_DAT_LOCK, LOCK_EX;

      ## Insert the first message into the queue if defined.
      if (defined $_first_msg) {
         local $\ = undef;
         my $_QUE_W_SOCK = $self->{_que_w_sock};
         print $_QUE_W_SOCK pack(QUE_TEMPLATE, 0, $_first_msg);
      }

      ## Submit params data to workers.
      for (1 .. $_max_workers) {
         select(undef, undef, undef, $_submit_delay)
            if ($_submit_delay && $_submit_delay > 0.0);

         print $_COM_R_SOCK $_, $LF;
         chomp($_wid = <$_COM_R_SOCK>);

         if ($_wid <= $self->{_task0_max_workers}) {
            print $_COM_R_SOCK length($_frozen_params), $LF, $_frozen_params;
         } else {
            print $_COM_R_SOCK length($_frozen_nodata), $LF, $_frozen_nodata;
         }

         <$_COM_R_SOCK>;
      }

      select(undef, undef, undef, $_submit_delay)
         if ($_submit_delay && $_submit_delay > 0.0);

      ## Release lock.
      flock $_DAT_LOCK, LOCK_UN;
      close $_DAT_LOCK; undef $_DAT_LOCK;
   }

   ## -------------------------------------------------------------------------

   ## Call the output function.
   if ($_max_workers > 0) {
      $self->{_abort_msg} = $_abort_msg;
      $self->_output_loop($_max_workers, $_input_data, $_input_glob);
      undef $self->{_abort_msg};
   }

   ## Remove the last message from the queue.
   unless ($_run_mode eq 'nodata') {
      unlink "$_sess_dir/_store.db" if ($_run_mode eq 'array');
      if (defined $self->{_que_r_sock}) {
         local $/ = $LF;
         my $_next; my $_QUE_R_SOCK = $self->{_que_r_sock};
         read $_QUE_R_SOCK, $_next, QUE_READ_SIZE;
      }
   }

   ## Shutdown workers.
   $self->shutdown() if ($_auto_shutdown == 1);

   return $self;
}

###############################################################################
## ----------------------------------------------------------------------------
## Shutdown Routine.
##
###############################################################################

sub shutdown {

   my MCE $self = $_[0];

   @_ = ();

   ## Croak if method was called by a MCE worker.
   _croak("MCE::shutdown: method cannot be called by a MCE worker")
      if ($self->wid());

   ## Return if workers have not been spawned or have already been shutdown.
   return $self unless ($self->{_spawned});

   local $SIG{__DIE__}  = \&_die;
   local $SIG{__WARN__} = \&_warn;

   lock $_MCE_LOCK if ($_has_threads);            ## Obtain MCE lock.

   ## Local vars.
   my $_COM_R_SOCK  = $self->{_com_r_sock};
   my $_mce_sid     = $self->{_mce_sid};
   my $_mce_tid     = $self->{_mce_tid};
   my $_sess_dir    = $self->{_sess_dir};
   my $_max_workers = $self->{max_workers};
   my $_use_threads = $self->{use_threads};

   ## Delete entry.
   delete $_mce_spawned{$_mce_sid};

   ## Notify workers to exit loop.
   local $\ = undef; local $/ = $LF;

   open my $_DAT_LOCK, '+>> :stdio', "$_sess_dir/dat.lock";
   flock $_DAT_LOCK, LOCK_EX;

   for (1 .. $_max_workers) {
      print $_COM_R_SOCK "_exit${LF}";
      <$_COM_R_SOCK>;
   }

   flock $_DAT_LOCK, LOCK_UN;

   ## Reap children/threads.
   waitpid $_, 0 for ( @{ $self->{_pids} } );
   $_->join()    for ( @{ $self->{_tids} } );

   close $_DAT_LOCK; undef $_DAT_LOCK;

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
      unlink "$_sess_dir/com.lock";
      unlink "$_sess_dir/dat.lock";
      rmdir $_sess_dir;
   }

   ## Reset vars.
   $self->{_out_r_sock} = $self->{_out_w_sock} = undef;
   $self->{_que_r_sock} = $self->{_que_w_sock} = undef;
   $self->{_com_r_sock} = $self->{_com_w_sock} = undef;
   $self->{_dat_r_sock} = $self->{_dat_w_sock} = undef;

   $self->{_mce_sid}    = $self->{_mce_tid}    = undef;
   $self->{_spawned}    = $self->{_wid}        = 0;
   $self->{_sess_dir}   = undef;

   @{ $self->{_pids} }  = ();
   @{ $self->{_tids} }  = ();

   return $self;
}

###############################################################################
## ----------------------------------------------------------------------------
## Miscellaneous routines.
##
###############################################################################

## Abort current job.

sub abort {

   my MCE $self = $_[0];

   @_ = ();

   my $_QUE_R_SOCK = $self->{_que_r_sock};
   my $_QUE_W_SOCK = $self->{_que_w_sock};
   my $_abort_msg  = $self->{_abort_msg};

   if (defined $_abort_msg) {
      local $\ = undef; local $/ = $LF;
      my $_next; read $_QUE_R_SOCK, $_next, QUE_READ_SIZE;
      print $_QUE_W_SOCK pack(QUE_TEMPLATE, 0, $_abort_msg);
   }

   return;
}

## Worker exits current job.

sub exit {

   my MCE $self = $_[0];

   @_ = ();

   _croak("MCE::exit: method cannot be called by the main MCE process")
      unless ($self->wid());

   my $_OUT_W_SOCK = $self->{_out_w_sock};
   my $_DAT_W_SOCK = $self->{_dat_w_sock};
   my $_task_id    = $self->{_task_id};

   local $\ = undef;
   print $_OUT_W_SOCK OUTPUT_W_EXT, $LF;

   unless (defined $_task_id) {
      print $_OUT_W_SOCK OUTPUT_W_DNE, $LF;
   }
   else {
      flock $_DAT_LOCK, LOCK_EX;
      print $_OUT_W_SOCK OUTPUT_W_DNE, $LF;
      print $_DAT_W_SOCK $_task_id, $LF;
      flock $_DAT_LOCK, LOCK_UN;
   }

   ## Enter loop and wait for exit notification.
   $self->_worker_loop();

   $SIG{__DIE__} = $SIG{__WARN__} = sub { };

   eval {
      flock $_DAT_LOCK, LOCK_SH;
      flock $_DAT_LOCK, LOCK_UN;
      close $_DAT_LOCK; undef $_DAT_LOCK;
      close $_COM_LOCK; undef $_COM_LOCK;
   };

   ## Exit thread/child process.
   threads->exit() if ($_has_threads && threads->can('exit'));
   CORE::exit();
}

## Worker immediately exits the chunking loop.

sub last {

   my MCE $self = $_[0];

   @_ = ();

   _croak("MCE::last: method cannot be called by the main MCE process")
      unless ($self->wid());

   $self->{_last_jmp}() if (defined $self->{_last_jmp});

   return;
}

## Worker starts the next iteration of the chunking loop.

sub next {

   my MCE $self = $_[0];

   @_ = ();

   _croak("MCE::next: method cannot be called by the main MCE process")
      unless ($self->wid());

   $self->{_next_jmp}() if (defined $self->{_next_jmp});

   return;
}

## Return worker ID.

sub wid {

   my MCE $self = $_[0];

   @_ = ();

   return $self->{_wid};
}

###############################################################################
## ----------------------------------------------------------------------------
## Do & sendto routines for serializing data from workers to main thread.
##
###############################################################################

## Do routine. Additional arguments are optional.

sub do {

   my MCE $self  = shift;
   my $_callback = shift;

   _croak("MCE::do: method cannot be called by the main MCE process")
      unless ($self->wid());

   _croak("MCE::do: 'callback' is not specified")
      unless (defined $_callback);

   $_callback = "main::$_callback" if (index($_callback, ':') < 0);

   return _do_callback($self, $_callback, \@_);
}

## Sendto routine.

{
   my %_sendto_lkup = (
      'file'   => SENDTO_FILEV1, 'FILE'   => SENDTO_FILEV1,
      'file:'  => SENDTO_FILEV2, 'FILE:'  => SENDTO_FILEV2,
      'stdout' => SENDTO_STDOUT, 'STDOUT' => SENDTO_STDOUT,
      'stderr' => SENDTO_STDERR, 'STDERR' => SENDTO_STDERR
   );

   my $_filev2_regx = qr/^([^:]+:)(.+)/;

   sub sendto {

      my MCE $self = shift;
      my $_to      = shift;

      _croak("MCE::sendto: method cannot be called by the main MCE process")
         unless ($self->wid());

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

sub _NOOP {

}

sub _croak {

   $SIG{__DIE__}  = \&_die;
   $SIG{__WARN__} = \&_warn;

   $\ = undef; require Carp; goto &Carp::croak;

   return;
}

sub _die {

   MCE::Signal->_die_handler(@_);
}

sub _warn {

   MCE::Signal->_warn_handler(@_);
}

###############################################################################
## ----------------------------------------------------------------------------
## Sync & validation routines.
##
###############################################################################

sub _sync_params {

   my MCE $self    = $_[0];
   my $_params_ref = $_[1];

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
      _croak "$_tag: '$_s->{input_data}' does not exist"
         unless (-e $_s->{input_data});
   }

   _croak "$_tag: 'chunk_size' is not valid"
      if ($_s->{chunk_size} !~ /\A\d+\z/ or $_s->{chunk_size} == 0);
   _croak "$_tag: 'max_workers' is not valid"
      if ($_s->{max_workers} !~ /\A\d+\z/ or $_s->{max_workers} == 0);
   _croak "$_tag: 'use_slurpio' is not 0 or 1"
      if ($_s->{use_slurpio} && $_s->{use_slurpio} !~ /\A[01]\z/);
   _croak "$_tag: 'use_threads' is not 0 or 1"
      if ($_s->{use_threads} && $_s->{use_threads} !~ /\A[01]\z/);

   _croak "$_tag: 'job_delay' is not valid"
      if ($_s->{job_delay} && $_s->{job_delay} !~ /\A[\d\.]+\z/);
   _croak "$_tag: 'spawn_delay' is not valid"
      if ($_s->{spawn_delay} && $_s->{spawn_delay} !~ /\A[\d\.]+\z/);
   _croak "$_tag: 'submit_delay' is not valid"
      if ($_s->{submit_delay} && $_s->{submit_delay} !~ /\A[\d\.]+\z/);

   _croak "$_tag: 'user_begin' is not a CODE reference"
      if ($_s->{user_begin} && ref $_s->{user_begin} ne 'CODE');
   _croak "$_tag: 'user_func' is not a CODE reference"
      if ($_s->{user_func} && ref $_s->{user_func} ne 'CODE');
   _croak "$_tag: 'user_end' is not a CODE reference"
      if ($_s->{user_end} && ref $_s->{user_end} ne 'CODE');
   _croak "$_tag: 'user_error' is not a CODE reference"
      if ($_s->{user_error} && ref $_s->{user_error} ne 'CODE');
   _croak "$_tag: 'user_output' is not a CODE reference"
      if ($_s->{user_output} && ref $_s->{user_output} ne 'CODE');

   _croak "$_tag: 'flush_file' is not 0 or 1"
      if ($_s->{flush_file} && $_s->{flush_file} !~ /\A[01]\z/);
   _croak "$_tag: 'flush_stderr' is not 0 or 1"
      if ($_s->{flush_stderr} && $_s->{flush_stderr} !~ /\A[01]\z/);
   _croak "$_tag: 'flush_stdout' is not 0 or 1"
      if ($_s->{flush_stdout} && $_s->{flush_stdout} !~ /\A[01]\z/);

   if (defined $_s->{user_tasks}) {
      _croak "$_tag: 'user_tasks' is not an ARRAY reference"
         if ($_s->{user_tasks} && ref $_s->{user_tasks} ne 'ARRAY');

      for my $_t (@{ $_s->{user_tasks} }) {
         _croak "$_tag: 'max_workers' is not valid"
            if ($_t->{max_workers} !~ /\A\d+\z/ or $_t->{max_workers} == 0);
         _croak "$_tag: 'use_threads' is not 0 or 1"
            if ($_t->{use_threads} && $_t->{use_threads} !~ /\A[01]\z/);

         _croak "$_tag: 'user_begin' is not a CODE reference"
            if ($_t->{user_begin} && ref $_t->{user_begin} ne 'CODE');
         _croak "$_tag: 'user_func' is not a CODE reference"
            if ($_t->{user_func} && ref $_t->{user_func} ne 'CODE');
         _croak "$_tag: 'user_end' is not a CODE reference"
            if ($_t->{user_end} && ref $_t->{user_end} ne 'CODE');

         _croak "$_tag: 'task_end' is not a CODE reference"
            if ($_t->{task_end} && ref $_t->{task_end} ne 'CODE');
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

      my MCE $self = $_[0];
      $_value      = $_[1];
      $_data_ref   = $_[2];

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
         if ($_want_id == WANTS_SCALAR) {
            return $_buffer;
         }
         else {
            return thaw($_buffer);
         }
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

      my MCE $self = shift;
      $_dest       = shift;
      $_value      = shift;

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
## Process Output.
##
## Awaits data from workers. Calls user_output function if specified.
## Otherwise, processes output internally.
##
## The send related functions tag the output. The hash structure below
## is driven by a hash key.
##
###############################################################################

{
   my ($_total_workers, $_value, $_want_id, $_input_data, $_eof_flag);
   my ($_max_workers, $_user_error, $_user_output, $_flush_file, $self);
   my ($_callback, $_file, %_sendto_fhs, $_len);

   my ($_DAT_R_SOCK, $_OUT_R_SOCK, $_MCE_STDERR, $_MCE_STDOUT);
   my ($_I_SEP, $_O_SEP, $_total_ended, $_input_glob, $_chunk_size);
   my ($_input_size, $_offset_pos, $_single_dim, $_use_slurpio);

   my ($_total_exited, $_has_user_tasks, $_task_id, @_task_max_workers);

   ## Create hash structure containing various output functions.
   my %_output_function = (

      OUTPUT_W_EXT.$LF => sub {                   ## Worker has exited job
         $_total_exited += 1;

         return;
      },

      OUTPUT_W_DNE.$LF => sub {                   ## Worker has completed job
         $_total_workers -= 1;

         if ($_has_user_tasks) {
            chomp($_task_id = <$_DAT_R_SOCK>);
            $_task_max_workers[$_task_id] -= 1;

            unless ($_task_max_workers[$_task_id]) {
               if (defined $self->{user_tasks}->[$_task_id]->{task_end}) {
                  $self->{user_tasks}->[$_task_id]->{task_end}();
               }
            }
         }

         return;
      },

      OUTPUT_W_END.$LF => sub {                   ## Worker has ended
         $_total_workers -= 1;                    ## prematurely
         $_total_ended   += 1;

         return;
      },

      ## ----------------------------------------------------------------------

      OUTPUT_A_ARY.$LF => sub {                   ## Array << Array
         my $_buffer;

         if ($_offset_pos >= $_input_size) {
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
         print $_DAT_R_SOCK $_len, $LF, $_buffer;
         $_offset_pos += $_chunk_size;

         return;
      },

      ## ----------------------------------------------------------------------

      OUTPUT_S_GLB.$LF => sub {                   ## Scalar << Glob FH
         my $_buffer;

         ## The logic below honors ('Ctrl/Z' in Windows, 'Ctrl/D' in Unix)
         ## when reading from standard input. No output will be lost as
         ## far as what was previously read into the buffer.

         if ($_eof_flag) {
            local $\ = undef; print $_DAT_R_SOCK "0${LF}"; return;
         }

         {
            local $/ = $_I_SEP;

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
         print $_DAT_R_SOCK ($_len) ? $_len . $LF . $_buffer : '0' . $LF;

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
      }

   );

   ## -------------------------------------------------------------------------

   sub _output_loop {

      $self           = $_[0];
      $_total_workers = $_[1];
      $_input_data    = $_[2];
      $_input_glob    = $_[3];

      @_ = ();

      die "Private method called" unless (caller)[0]->isa( ref($self) );

      $_chunk_size  = $self->{chunk_size};
      $_flush_file  = $self->{flush_file};
      $_max_workers = $self->{max_workers};
      $_use_slurpio = $self->{use_slurpio};
      $_user_output = $self->{user_output};
      $_user_error  = $self->{user_error};
      $_single_dim  = $self->{_single_dim};

      $_total_ended = $_total_exited = $_eof_flag = 0;
      $_has_user_tasks = (defined $self->{user_tasks});

      if ($_has_user_tasks) {
         push @_task_max_workers, $_->{max_workers} for (
            @{ $self->{user_tasks} }
         );
      }

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

      $_O_SEP = $\; local $\ = undef;
      $_I_SEP = $/; local $/ = $LF;

      ## Call hash function if output value is a hash key.
      ## Exit loop when all workers have completed or ended.
      my $_func;

      while (1) {
         $_func = <$_OUT_R_SOCK>;
         next unless (defined $_func);

         if (exists $_output_function{$_func}) {
            $_output_function{$_func}();
            last unless ($_total_workers);
         }
      }

      @_task_max_workers = undef if ($_has_user_tasks);

      ## Close opened sendto file handles.
      for (keys %_sendto_fhs) {
         close  $_sendto_fhs{$_};
         undef  $_sendto_fhs{$_};
         delete $_sendto_fhs{$_};
      }

      ## Restore the default handle.
      select $_old_hndl;

      ## Close MCE STDOUT/STDERR handles.
      close $_MCE_STDOUT; undef $_MCE_STDOUT;
      close $_MCE_STDERR; undef $_MCE_STDERR;

      ## Shutdown all workers if one or more have exited.
      if ($_total_exited > 0 && $_total_ended == 0) {
         $self->shutdown();
      }

      ## Shutdown all workers if one or more have ended prematurely.
      if ($_total_ended > 0) {
         warn("[ $_total_ended ] workers ended prematurely\n");
         $self->shutdown();
      }

      return;
   }
}

###############################################################################
## ----------------------------------------------------------------------------
## Sync Buffer to Array
##
###############################################################################

sub _sync_buffer_to_array {

   my $_buffer_ref = $_[0];
   my $_array_ref  = $_[1];
   my $_cnt        = 0;

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
## Worker Process -- Read Handle.
##
###############################################################################

sub _worker_read_handle {

   my MCE $self    = $_[0];
   my $_proc_type  = $_[1];
   my $_input_data = $_[2];

   @_ = ();

   die "Private method called" unless (caller)[0]->isa( ref($self) );

   my $_many_wrks   = ($self->{max_workers} > 1) ? 1 : 0;

   my $_QUE_R_SOCK  = $self->{_que_r_sock};
   my $_QUE_W_SOCK  = $self->{_que_w_sock};
   my $_chunk_size  = $self->{chunk_size};
   my $_use_slurpio = $self->{use_slurpio};
   my $_user_func   = $self->{user_func};

   my ($_data_size, $_next, $_chunk_id, $_offset_pos, $_IN_FILE);
   my @_records = (); $_chunk_id = $_offset_pos = 0;

   $_data_size = ($_proc_type == READ_MEMORY)
      ? length($$_input_data) : -s $_input_data;

   if ($_chunk_size <= MAX_RECS_SIZE || $_proc_type == READ_MEMORY) {
      open $_IN_FILE, '<', $_input_data or die "$_input_data: $!\n";
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
               _sync_buffer_to_array(\$_buffer, \@_records);
            }
            $_user_func->($self, \@_records, $_chunk_id);
         }
      }
   }

   _WORKER_READ_HANDLE__LAST:
}

###############################################################################
## ----------------------------------------------------------------------------
## Worker Process -- Request Chunk.
##
###############################################################################

sub _worker_request_chunk {

   my MCE $self   = $_[0];
   my $_proc_type = $_[1];

   @_ = ();

   die "Private method called" unless (caller)[0]->isa( ref($self) );

   my $_QUE_R_SOCK  = $self->{_que_r_sock};
   my $_QUE_W_SOCK  = $self->{_que_w_sock};
   my $_OUT_W_SOCK  = $self->{_out_w_sock};
   my $_DAT_W_SOCK  = $self->{_dat_w_sock};
   my $_single_dim  = $self->{_single_dim};
   my $_chunk_size  = $self->{chunk_size};
   my $_use_slurpio = $self->{use_slurpio};
   my $_user_func   = $self->{user_func};

   my ($_next, $_chunk_id, $_has_data, $_len, $_chunk_ref);
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

         read $_QUE_R_SOCK, $_next, QUE_READ_SIZE;
         ($_chunk_id, $_has_data) = unpack(QUE_TEMPLATE, $_next);

         if ($_has_data == 0) {
            print $_QUE_W_SOCK pack(QUE_TEMPLATE, 0, $_has_data);
            flock $_DAT_LOCK, LOCK_UN;
            return;
         }

         $_chunk_id++;

         print $_OUT_W_SOCK $_output_tag, $LF;
         chomp($_len = <$_DAT_W_SOCK>);
         print $_QUE_W_SOCK pack(QUE_TEMPLATE, $_chunk_id, $_len);

         if ($_len == 0) {
            flock $_DAT_LOCK, LOCK_UN;
            return;
         }

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
               _sync_buffer_to_array(\$_buffer, \@_records);
               $_user_func->($self, \@_records, $_chunk_id);
            }
         }
      }
   }

   _WORKER_REQUEST_CHUNK__LAST:
}

###############################################################################
## ----------------------------------------------------------------------------
## Worker Process -- Do.
##
###############################################################################

sub _worker_do {

   my MCE $self    = $_[0];
   my $_params_ref = $_[1];

   @_ = ();

   die "Private method called" unless (caller)[0]->isa( ref($self) );

   ## Set parameters.
   $self->{_abort_msg}  = $_params_ref->{_abort_msg};
   $self->{_run_mode}   = $_params_ref->{_run_mode};
   $self->{_single_dim} = $_params_ref->{_single_dim};
   $self->{chunk_size}  = $_params_ref->{_chunk_size};
   $self->{max_workers} = $_params_ref->{_max_workers};
   $self->{use_slurpio} = $_params_ref->{_use_slurpio};

   ## Init local vars.
   my $_OUT_W_SOCK  = $self->{_out_w_sock};
   my $_DAT_W_SOCK  = $self->{_dat_w_sock};
   my $_run_mode    = $self->{_run_mode};
   my $_task_id     = $self->{_task_id};
   my $_use_slurpio = $self->{use_slurpio};
   my $_user_begin  = $self->{user_begin};
   my $_user_func   = $self->{user_func};
   my $_user_end    = $self->{user_end};

   ## Call user_begin if defined.
   $_user_begin->($self) if ($_user_begin);

   ## Call worker function.
   if ($_run_mode eq 'array') {
      _worker_request_chunk($self, REQUEST_ARRAY);
   }
   elsif ($_run_mode eq 'glob') {
      _worker_request_chunk($self, REQUEST_GLOB);
   }
   elsif ($_run_mode eq 'file') {
      _worker_read_handle($self, READ_FILE, $_params_ref->{_input_file});
   }
   elsif ($_run_mode eq 'memory') {
      _worker_read_handle($self, READ_MEMORY, $self->{input_data});
   }
   else {
      $_user_func->($self);
   }

   undef $self->{_next_jmp} if (defined $self->{_next_jmp});
   undef $self->{_last_jmp} if (defined $self->{_last_jmp});

   ## Call user_end if defined.
   $_user_end->($self) if ($_user_end);

   ## Notify the main process a worker has completed.
   local $\ = undef;

   unless (defined $_task_id) {
      print $_OUT_W_SOCK OUTPUT_W_DNE, $LF;
   }
   else {
      flock $_DAT_LOCK, LOCK_EX;
      print $_OUT_W_SOCK OUTPUT_W_DNE, $LF;
      print $_DAT_W_SOCK $_task_id, $LF;
      flock $_DAT_LOCK, LOCK_UN;
   }

   return;
}

###############################################################################
## ----------------------------------------------------------------------------
## Worker Process -- Loop.
##
###############################################################################

sub _worker_loop {

   my MCE $self = $_[0];

   @_ = ();

   die "Private method called" unless (caller)[0]->isa( ref($self) );

   my ($_response, $_len, $_buffer, $_params_ref);
   my $_COM_W_SOCK = $self->{_com_w_sock};
   my $_wid = $self->{_wid};
 
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
         last if ($_response !~ /\A(?:\d+|_exit)\z/);

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

      ## Wait until MCE completes job submission.
      if (defined $self->{user_tasks}) {
         flock $_DAT_LOCK, LOCK_SH;
         flock $_DAT_LOCK, LOCK_UN;
      }

      ## Update ID. Process request.
      $self->{_wid} = $_response unless (defined $self->{user_tasks});
      _worker_do($self, $_params_ref);
      undef $_params_ref;
   }

   ## -------------------------------------------------------------------------

   ## Notify the main process a worker has ended. This code block is only
   ## executed if an invalid reply was received above.

   local $\ = undef;
   flock $_COM_LOCK, LOCK_UN;

   select STDOUT;

   my $_OUT_W_SOCK = $self->{_out_w_sock};
   print $_OUT_W_SOCK OUTPUT_W_END, $LF;

   ## Pause before returning.
   select(undef, undef, undef, 0.08 + (0.01 * $self->{max_workers}));

   return 1;
}

###############################################################################
## ----------------------------------------------------------------------------
## Worker Process -- Main.
##
###############################################################################

sub _worker_main {

   my MCE $self = $_[0];
   my $_wid     = $_[1];
   my $_task    = $_[2];
   my $_task_id = $_[3];

   @_ = ();

   ## Commented out -- fails with the 'forks' module under FreeBSD.
   ## die "Private method called" unless (caller)[0]->isa( ref($self) );

   $SIG{PIPE} = \&_NOOP;
   my $_sess_dir = $self->{_sess_dir};

   ## Undef vars not required after being spawned.
   $self->{_com_r_sock}  = $self->{_dat_r_sock}  = $self->{_out_r_sock} = undef;
   $self->{flush_stderr} = $self->{flush_stdout} = undef;
   $self->{stderr_file}  = $self->{stdout_file}  = undef;
   $self->{user_error}   = $self->{user_output}  = undef;
   $self->{flush_file}   = undef;

   $MCE::Signal::mce_spawned_ref = undef;
   %_mce_spawned = ();

   ## Use code references from user task if defined.
   $self->{user_begin} = $_task->{user_begin} if (defined $_task->{user_begin});
   $self->{user_func}  = $_task->{user_func}  if (defined $_task->{user_func});
   $self->{user_end}   = $_task->{user_end}   if (defined $_task->{user_end});

   ## Init runtime vars. Obtain handle to lock files.
   $self->{_task_id} = $_task_id;
   $self->{_wid}     = $_wid;

   _do_send_init($self);

   open $_COM_LOCK, '+>> :stdio', "$_sess_dir/com.lock";
   open $_DAT_LOCK, '+>> :stdio', "$_sess_dir/dat.lock";

   ## Wait until MCE completes spawning.
   flock $_COM_LOCK, LOCK_SH;
   flock $_COM_LOCK, LOCK_UN;

   ## Enter worker loop.
   my $_status = _worker_loop($self);

   ## Wait until MCE completes exit notification.
   $SIG{__DIE__} = $SIG{__WARN__} = sub { };

   eval {
      flock $_DAT_LOCK, LOCK_SH;
      flock $_DAT_LOCK, LOCK_UN;
      close $_DAT_LOCK; undef $_DAT_LOCK;
      close $_COM_LOCK; undef $_COM_LOCK;
   };

   return;
}

###############################################################################
## ----------------------------------------------------------------------------
## Dispatch Child.
##
###############################################################################

sub _dispatch_child {

   my MCE $self = $_[0];
   my $_wid     = $_[1];
   my $_task    = $_[2];
   my $_task_id = $_[3];

   @_ = ();

   die "Private method called" unless (caller)[0]->isa( ref($self) );

   my $_pid = fork();
   push @{ $self->{_pids} }, $_pid if (defined $_pid);

   _croak "MCE::_dispatch_child: Failed to spawn worker $_wid: $!"
      unless (defined $_pid);

   unless ($_pid) {
      _worker_main($self, $_wid, $_task, $_task_id);

      ## This child may still exist after exiting or MCE may hang during
      ## reaping. Therefore, using kill instead. This is done for child
      ## processes that were spawned from a non-main thread (tid != 0).
      kill 9, $$ if ($self->{_mce_tid} ne '0' && ! $_is_winperl);

      CORE::exit();
   }

   return;
}

###############################################################################
## ----------------------------------------------------------------------------
## Dispatch Thread.
##
###############################################################################

sub _dispatch_thread {

   my MCE $self = $_[0];
   my $_wid     = $_[1];
   my $_task    = $_[2];
   my $_task_id = $_[3];

   @_ = ();

   die "Private method called" unless (caller)[0]->isa( ref($self) );

   my $_thr = threads->create(\&_worker_main, $self, $_wid, $_task, $_task_id);
   push @{ $self->{_tids} }, $_thr if (defined $_thr);

   _croak "MCE::_dispatch_thread: Failed to spawn worker $_wid: $!"
      unless (defined $_thr);

   return;
}

1;

