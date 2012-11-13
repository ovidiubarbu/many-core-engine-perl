###############################################################################
## ----------------------------------------------------------------------------
## Many-Core Engine (MCE) for Perl. Provides parallel processing cabilities.
##
###############################################################################

package MCE;

my ($_que_template, $_que_read_size, $_die_handler);
my ($_has_threads, $_max_files, $_max_procs);

BEGIN {
   if (ref $SIG{__DIE__} eq 'CODE') {
      $_die_handler = $SIG{__DIE__};
      $SIG{__DIE__} = 'DEFAULT';
   }

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
         local $@; local $SIG{__DIE__} = \&_NOOP;
         eval { require BSD::Resource; };

         if ($@) {
            $_max_files = $_max_procs = 256;
         }
         else {
            $_max_files = BSD::Resource::getrlimit('RLIMIT_NOFILE') || 256;
            $_max_procs = BSD::Resource::getrlimit('RLIMIT_NPROC' ) || 256;
         }
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
##    use threads;           (or)   use forks;
##    use threads::shared;          use forks::shared;
##
###############################################################################

my ($_COM_LOCK, $_DAT_LOCK);
our $_MCE_LOCK : shared = 1;

INIT {
   if ($threads::VERSION) {
      unless (defined $threads::shared::VERSION) {
         local $@; local $SIG{__DIE__} = \&_NOOP;
         eval 'use threads::shared; threads::shared::share($_MCE_LOCK)';
      }
      $_has_threads = 1;
   }
   elsif ($forks::VERSION) {
      unless (defined $forks::shared::VERSION) {
         local $@; local $SIG{__DIE__} = \&_NOOP;
         eval 'use forks::shared; forks::shared::share($_MCE_LOCK)';
      }
      $_has_threads = 1;
   }
}

use strict;
use warnings;

our $VERSION = '1.005';
$VERSION = eval $VERSION;

use Fcntl qw( :flock O_CREAT O_TRUNC O_RDWR O_RDONLY );
use Storable 2.04 qw( store retrieve freeze thaw );
use Socket qw( :DEFAULT :crlf );

if (ref $_die_handler eq 'CODE') {
   $SIG{__DIE__} = $_die_handler;
   $_die_handler = undef;
}

require MCE::Signal; MCE::Signal->import();

###############################################################################
## ----------------------------------------------------------------------------
## Define constants & variables.
##
###############################################################################

use constant {
   ILLEGAL_ARGUMENT => 'ILLEGAL_ARGUMENT',  ## Usage related
   ILLEGAL_STATE    => 'ILLEGAL_STATE',
   INVALID_DIR      => 'INVALID_DIR',
   INVALID_PATH     => 'INVALID_PATH',
   INVALID_REF      => 'INVALID_REF',

   MAX_CHUNK_SIZE   => 24 * 1024 * 1024, ## Set max constraints
   MAX_OPEN_FILES   => $_max_files,
   MAX_USER_PROCS   => $_max_procs,

   MAX_RECS_SIZE    => 8192,             ## Read # of records if <= value
                                         ## Read # of bytes if > value

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

   SENDTO_CALLBK    => 0,                ## Worker sends to callback
   SENDTO_STDOUT    => 1,                ## Worker sends to STDOUT
   SENDTO_STDERR    => 2,                ## Worker sends to STDERR
   SENDTO_FILE      => 3,                ## Worker sends to file

   WANTS_UNDEFINE   => 0,                ## Callee wants nothing
   WANTS_ARRAY      => 1,                ## Callee wants list
   WANTS_SCALAR     => 2,                ## Callee wants scalar
   WANTS_REFERENCE  => 3                 ## Callee wants H/A/S ref
};

undef $_max_files; undef $_max_procs;
undef $_que_template; undef $_que_read_size;

my %_valid_fields = map { $_ => 1 } qw(
   chunk_size input_data max_workers use_slurpio use_threads
   user_begin user_end user_func user_error user_output
   flush_stderr flush_stdout stderr_file stdout_file
   job_delay spawn_delay submit_delay tmp_dir

   _mce_id _mce_tid _pids _sess_dir _spawned _wid
   _com_r_sock _com_w_sock _dat_r_sock _dat_w_sock
   _out_r_sock _out_w_sock _que_r_sock _que_w_sock
   _abort_msg _run_mode _single_dim
);

my %_params_allowed_args = map { $_ => 1 } qw(
   chunk_size input_data job_delay spawn_delay submit_delay use_slurpio
   user_begin user_end user_func user_error user_output
   flush_stderr flush_stdout stderr_file stdout_file
);

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
   $self->{chunk_size}   = $argv{chunk_size}   || 500;
   $self->{max_workers}  = $argv{max_workers}  || 2;
   $self->{use_slurpio}  = $argv{use_slurpio}  || 0;

   if (exists $argv{use_threads}) {
      $self->{use_threads} = $argv{use_threads};

      if (!$_has_threads && $argv{use_threads} ne '0') {
         my $_msg  = "Please include threads support.\n";
            $_msg .= "\n";
            $_msg .= "   use threads;           (or)   use forks;\n";
            $_msg .= "   use threads::shared;          use forks::shared;\n";
            $_msg .= "\n";
            $_msg .= "## Include threads modules before MCE.\n";
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
   $self->{flush_stderr} = $argv{flush_stderr} || 0;
   $self->{flush_stdout} = $argv{flush_stdout} || 0;
   $self->{stderr_file}  = $argv{stderr_file}  || undef;
   $self->{stdout_file}  = $argv{stdout_file}  || undef;

   ## Validation.
   foreach (keys %argv) {
      _croak(ILLEGAL_ARGUMENT . ": '$_' is not a valid constructor argument")
         unless (exists $_valid_fields{$_});
   }

   _croak(INVALID_DIR . ": '$self->{tmp_dir}' does not exist")
      unless (-d $self->{tmp_dir});

   _validate_args($self);

   %argv = ();

   ## Private options.
   @{ $self->{_pids} }  = ();    ## Array for joining workers when completed
   $self->{_sess_dir}   = undef; ## Unique session dir for this MCE
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
         $self->{max_workers} = int(MAX_OPEN_FILES / 4) - 28
            if ($self->{max_workers} > int(MAX_OPEN_FILES / 4) - 28);
      }
      else {
         $self->{max_workers} = MAX_USER_PROCS - 56
            if ($self->{max_workers} > MAX_USER_PROCS - 56);
      }
   }

   if ($^O eq 'cygwin') {                 ## Limit to 32 threads, 16 children
      $self->{max_workers} = 32
         if ($self->{use_threads} && $self->{max_workers} > 32);
      $self->{max_workers} = 16
         if (!$self->{use_threads} && $self->{max_workers} > 16);
   }
   elsif ($^O eq 'MSWin32') {             ## Limit to 64 threads, 32 children
      $self->{max_workers} = 64
         if ($self->{use_threads} && $self->{max_workers} > 64);
      $self->{max_workers} = 32
         if (!$self->{use_threads} && $self->{max_workers} > 32);
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

   ## Return if workers have already been spawned.
   return $self unless ($self->{_spawned} == 0);

   lock $_MCE_LOCK if ($_has_threads);            ## Obtain MCE lock.

   ## Delay spawning.
   select(undef, undef, undef, $self->{spawn_delay})
      if ($self->{spawn_delay} && $self->{spawn_delay} > 0.0);

   ## Configure this MCE ID here.
   $self->{_mce_tid} = ($_has_threads) ? threads->tid() : '';
   $self->{_mce_tid} = '' unless defined $self->{_mce_tid};

   $self->{_mce_id}  = $$ .'.'. $self->{_mce_tid} .'.'. (++$_mce_count);

   ## Local vars.
   my $_mce_tid      = $self->{_mce_tid};
   my $_mce_id       = $self->{_mce_id};
   my $_sess_dir     = $self->{_sess_dir};
   my $_tmp_dir      = $self->{tmp_dir};
   my $_max_workers  = $self->{max_workers};
   my $_use_threads  = $self->{use_threads};

   ## Create temp dir.
   unless ($_sess_dir) {
      my $_cnt = 0;
      $_sess_dir = $self->{_sess_dir} = "$_tmp_dir/$_mce_id";
      while ( !(mkdir $_sess_dir, 0770) ) {
         $_sess_dir = $self->{_sess_dir} = "$_tmp_dir/$_mce_id." . (++$_cnt);
      }
   }

   ## -------------------------------------------------------------------------

   ## Create socket pair for communication channels between MCE and workers.
   socketpair( $self->{_com_r_sock}, $self->{_com_w_sock},
      AF_UNIX, SOCK_STREAM, PF_UNSPEC ) or die "socketpair: $!${LF}";

   binmode $self->{_com_r_sock};                  ## Set binary mode
   binmode $self->{_com_w_sock};

   ## Create socket pair for data channels between MCE and workers.
   socketpair( $self->{_dat_r_sock}, $self->{_dat_w_sock},
      AF_UNIX, SOCK_STREAM, PF_UNSPEC ) or die "socketpair: $!${LF}";

   binmode $self->{_dat_r_sock};                  ## Set binary mode
   binmode $self->{_dat_w_sock};

   ## Create socket pair for serializing send requests and STDOUT output.
   socketpair( $self->{_out_r_sock}, $self->{_out_w_sock},
      AF_UNIX, SOCK_STREAM, PF_UNSPEC ) or die "socketpair: $!${LF}";

   binmode $self->{_out_r_sock};                  ## Set binary mode
   binmode $self->{_out_w_sock};

   shutdown $self->{_out_r_sock}, 1;              ## No more writing
   shutdown $self->{_out_w_sock}, 0;              ## No more reading

   ## Create socket pairs for queue channels between MCE and workers.
   socketpair( $self->{_que_r_sock}, $self->{_que_w_sock},
      AF_UNIX, SOCK_STREAM, PF_UNSPEC ) or die "socketpair: $!${LF}";

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

   @{ $self->{_pids} } = ();
   my $_wid = 0;

   ## Obtain lock.
   open my $_COM_LOCK, '+>> :stdio', "$_sess_dir/com.lock";
   flock $_COM_LOCK, LOCK_EX;

   ## Spawn workers.
   foreach (1 .. $_max_workers) {
      $_wid += 1;

      if (defined $_use_threads && $_use_threads == 1) {
         my $_thr = threads->create(\&_worker_main, $self, $_wid);
         push @{ $self->{_pids} }, $_thr if (defined $_thr);
      }
      else {
         my $_pid = fork();
         push @{ $self->{_pids} }, $_pid if (defined $_pid);

         if (defined $_pid && !$_pid) {
            _worker_main($self, $_wid);

            ## This child may still exist after exiting or MCE may hang during
            ## reaping. Therefore, using kill instead. This is done for child
            ## processes that were spawned from a non-main thread (tid != 0).
            kill 9, $$ if ($self->{_mce_tid} ne '0' && !$_is_winperl);

            exit;
         }
      }

      ## Exit if new thread/child failed to spawn.
      unless (defined $self->{_pids}[$_wid - 1]) {
         die "Failed to spawn worker $_wid: $!${LF}";
      }
   }

   ## Release lock.
   flock $_COM_LOCK, LOCK_UN;
   close $_COM_LOCK; undef $_COM_LOCK;

   ## Update max workers spawned.
   $_mce_spawned{$_mce_id} = $self;
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

   _croak(ILLEGAL_STATE . ": 'input_data' is not specified")
      unless (defined $_input_data);
   _croak(ILLEGAL_STATE . ": 'code_block' is not specified")
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

   _croak(ILLEGAL_STATE . ": 'input_data' is not specified")
      unless (defined $_input_data);
   _croak(ILLEGAL_STATE . ": 'code_block' is not specified")
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

   ## Set input data.
   if (defined $_input_data) {
      $_params_ref->{input_data} = $_input_data;
   }
   else {
      _croak(ILLEGAL_STATE . ": 'input_data' is not specified")
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
         _croak(ILLEGAL_STATE . ": 'input_data' is not valid");
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
      '_input_file'  => $_input_file,
      '_chunk_size'  => $_chunk_size,
      '_max_workers' => $_max_workers,
      '_use_slurpio' => $_use_slurpio,
      '_abort_msg'   => $_abort_msg,
      '_run_mode'    => $_run_mode,
      '_single_dim'  => $_single_dim
   );

   my $_max_wid = 0;

   ## Begin processing.
   {
      local $\ = undef; local $/ = $LF;
      lock $_MCE_LOCK if ($_has_threads);            ## Obtain MCE lock.

      my $_COM_R_SOCK    = $self->{_com_r_sock};
      my $_submit_delay  = $self->{submit_delay};
      my $_frozen_params = freeze(\%_params);
      my $_reply;

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
      while (1) {
         last if ($_max_wid >= $_max_workers);

         select(undef, undef, undef, $_submit_delay)
            if ($_submit_delay && $_submit_delay > 0.0);

         print $_COM_R_SOCK
            ++$_max_wid, $LF, length($_frozen_params), $LF, $_frozen_params;

         $_reply = <$_COM_R_SOCK>;
      }

      select(undef, undef, undef, $_submit_delay)
         if ($_submit_delay && $_submit_delay > 0.0);

      ## Release lock.
      flock $_DAT_LOCK, LOCK_UN;
      close $_DAT_LOCK; undef $_DAT_LOCK;
   }

   ## -------------------------------------------------------------------------

   ## Call the output function.
   if ($_max_wid + $_max_workers > 0) {
      $self->{_abort_msg} = $_abort_msg;
      $self->_output_loop($_max_wid, $_input_data, $_input_glob);
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

   ## Return if workers have not been spawned or have already been shutdown.
   return $self unless ($self->{_spawned});

   lock $_MCE_LOCK if ($_has_threads);            ## Obtain MCE lock.

   ## Local vars.
   my $_COM_R_SOCK  = $self->{_com_r_sock};
   my $_mce_id      = $self->{_mce_id};
   my $_mce_tid     = $self->{_mce_tid};
   my $_sess_dir    = $self->{_sess_dir};
   my $_max_workers = $self->{max_workers};
   my $_use_threads = $self->{use_threads};

   my @_pids = (); my $_pid;

   ## Delete entry.
   delete $_mce_spawned{$_mce_id};

   ## Notify workers to exit loop.
   local $\ = undef; local $/ = $LF;

   open my $_DAT_LOCK, '+>> :stdio', "$_sess_dir/dat.lock";
   flock $_DAT_LOCK, LOCK_EX;

   foreach my $_wid (1 .. $_max_workers) {
      print $_COM_R_SOCK "_exit${LF}";
      chomp($_pid = <$_COM_R_SOCK>);
      push @_pids, $_pid;
   }

   flock $_DAT_LOCK, LOCK_UN;

   ## Reap children/threads.
   if ($_use_threads == 0) {
      waitpid $_, 0 foreach ( @_pids );
   }
   else {
      $_->join() foreach ( @{ $self->{_pids} } );
   }

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

   $self->{_spawned} = $self->{_mce_tid} = $self->{_wid} = 0;
   $self->{_sess_dir} = undef;
   @{ $self->{_pids} } = ();

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

   if ($self->wid() > 0) {
      my $_OUT_W_SOCK = $self->{_out_w_sock};

      local $\ = undef;
      print $_OUT_W_SOCK OUTPUT_W_DNE, $LF, OUTPUT_W_EXT, $LF;

      ## Enter loop and wait for exit notification.
      $self->_worker_loop();

      ## Wait till MCE completes exit notification to all workers.
      flock $_DAT_LOCK, LOCK_SH;
      flock $_DAT_LOCK, LOCK_UN;

      close $_DAT_LOCK; undef $_DAT_LOCK;
      close $_COM_LOCK; undef $_COM_LOCK;

      ## Exit thread/child process.
      threads->exit() if ($_has_threads && threads->can('exit'));
      exit;
   }

   return;
}

## Worker immediately exits the chunking loop.

sub last {

   my MCE $self = $_[0];

   @_ = ();

   if ($self->wid() > 0) {
      $self->{_last_jmp}() if (defined $self->{_last_jmp});
   }

   return;
}

## Worker starts the next iteration of the chunking loop.

sub next {

   my MCE $self = $_[0];

   @_ = ();

   if ($self->wid() > 0) {
      $self->{_next_jmp}() if (defined $self->{_next_jmp});
   }

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
   my $_ref_type = (@_ > 0) ? ref $_[0] : undef;

   _croak(ILLEGAL_ARGUMENT . ": 'callback argument' is not specified")
      unless defined $_callback;

   $_callback = "main::$_callback" if (index($_callback, ':') < 0);

   return _do_send($self, SENDTO_CALLBK, $_ref_type, \@_, $_callback);
}

## Sendto routine.

{
   my %_sendto_lkup = (
      'stdout' => SENDTO_STDOUT, 'STDOUT' => SENDTO_STDOUT,
      'stderr' => SENDTO_STDERR, 'STDERR' => SENDTO_STDERR,
      'file'   => SENDTO_FILE,   'FILE'   => SENDTO_FILE
   );

   sub sendto {

      my MCE $self  = $_[0];
      my $_to       = $_[1];
      my $_data_ref = $_[2];
      my $_value    = $_[3];

      @_ = ();

      _croak(ILLEGAL_ARGUMENT . ": data argument is not specified")
         unless defined $_data_ref;
      _croak(ILLEGAL_ARGUMENT . ": type argument is not specified")
         unless defined $_to;
      _croak(ILLEGAL_ARGUMENT . ": type argument is not valid")
         unless exists $_sendto_lkup{$_to};

      my $_ref_type = ref $_data_ref;

      _do_send($self, $_sendto_lkup{$_to}, $_ref_type, $_data_ref, $_value)
         if ($_ref_type ne 'HASH' && $_ref_type ne 'GLOB');

      return;
   }
}

###############################################################################
## ----------------------------------------------------------------------------
## Private Methods.
## ----------------------------------------------------------------------------
###############################################################################

sub _NOOP {

}

sub _croak {

   $\ = undef; require Carp; goto &Carp::croak;

   return;
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

   foreach (keys %{ $_params_ref }) {
      _croak(ILLEGAL_ARGUMENT . ": '$_' is not a valid params argument")
         unless exists $_params_allowed_args{$_};

      $self->{$_} = $_params_ref->{$_};
   }

   return ($self->{_spawned}) ? $_requires_shutdown : 0;
}

sub _validate_args {

   my MCE $self = $_[0];

   @_ = ();

   die "Private method called" unless (caller)[0]->isa( ref($self) );

   if ($self->{input_data} && ref $self->{input_data} eq '') {
      _croak INVALID_PATH . ": '$self->{input_data}' does not exist"
         unless (-e $self->{input_data});
   }

   _croak ILLEGAL_STATE . ": 'chunk_size' is not valid"
      if ($self->{chunk_size} !~ /\A\d+\z/ or $self->{chunk_size} == 0);
   _croak ILLEGAL_STATE . ": 'max_workers' is not valid"
      if ($self->{max_workers} !~ /\A\d+\z/ or $self->{max_workers} == 0);
   _croak ILLEGAL_STATE . ": 'use_slurpio' is not 0 or 1"
      if ($self->{use_slurpio} && $self->{use_slurpio} !~ /\A[01]\z/);
   _croak ILLEGAL_STATE . ": 'use_threads' is not 0 or 1"
      if ($self->{use_threads} && $self->{use_threads} !~ /\A[01]\z/);

   _croak ILLEGAL_STATE . ": 'job_delay' is not valid"
      if ($self->{job_delay} && $self->{job_delay} !~ /\A[\d\.]+\z/);
   _croak ILLEGAL_STATE . ": 'spawn_delay' is not valid"
      if ($self->{spawn_delay} && $self->{spawn_delay} !~ /\A[\d\.]+\z/);
   _croak ILLEGAL_STATE . ": 'submit_delay' is not valid"
      if ($self->{submit_delay} && $self->{submit_delay} !~ /\A[\d\.]+\z/);

   _croak INVALID_REF . ": 'user_begin' is not a CODE reference"
      if ($self->{user_begin} && ref $self->{user_begin} ne 'CODE');
   _croak INVALID_REF . ": 'user_func' is not a CODE reference"
      if ($self->{user_func} && ref $self->{user_func} ne 'CODE');
   _croak INVALID_REF . ": 'user_end' is not a CODE reference"
      if ($self->{user_end} && ref $self->{user_end} ne 'CODE');
   _croak INVALID_REF . ": 'user_error' is not a CODE reference"
      if ($self->{user_error} && ref $self->{user_error} ne 'CODE');
   _croak INVALID_REF . ": 'user_output' is not a CODE reference"
      if ($self->{user_output} && ref $self->{user_output} ne 'CODE');

   _croak ILLEGAL_STATE . ": 'flush_stderr' is not 0 or 1"
      if ($self->{flush_stderr} && $self->{flush_stderr} !~ /\A[01]\z/);
   _croak ILLEGAL_STATE . ": 'flush_stdout' is not 0 or 1"
      if ($self->{flush_stdout} && $self->{flush_stdout} !~ /\A[01]\z/);

   return;
}

###############################################################################
## ----------------------------------------------------------------------------
## Internal send related functions for serializing data to destination.
##
###############################################################################

{
   my ($_data_ref, $_dest, $_len, $_ref_type, $_send_init_called);
   my ($_sess_dir, $_value, $_DAT_W_SOCK, $_OUT_W_SOCK, $_want_id);

   ## Create hash structure containing various send functions.
   my @_dest_function = ();

   $_dest_function[SENDTO_CALLBK] = sub {

      local $\ = undef; my $_buffer;

      unless (defined wantarray) {
         $_want_id = WANTS_UNDEFINE;
      }
      elsif (wantarray) {
         $_want_id = WANTS_ARRAY;
      }
      else {
         $_want_id = WANTS_SCALAR;
      }

      ## Crossover: Send arguments

      if (@$_data_ref > 0) {
         if ($_ref_type || @$_data_ref > 1) {     ## Reference >> Callback
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

         $_data_ref = thaw($_buffer);
         return @{ $_data_ref };
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
            $_data_ref = thaw($_buffer);
            return $_data_ref;
         }
      }
   };

   ## -------------------------------------------------------------------------

   $_dest_function[SENDTO_STDOUT] = sub {

      local $\ = undef; my $_buffer;

      if ($_ref_type) {                           ## Reference >> STDOUT
         if ($_ref_type eq 'ARRAY') {
            $_buffer = join('', @$_data_ref);

            $_len = length($_buffer);
            flock $_DAT_LOCK, LOCK_EX;

            print $_OUT_W_SOCK OUTPUT_S_OUT, $LF;
            print $_DAT_W_SOCK "$_len${LF}", $_buffer;
         }
         else {
            $_len = length($$_data_ref);
            flock $_DAT_LOCK, LOCK_EX;

            print $_OUT_W_SOCK OUTPUT_S_OUT, $LF;
            print $_DAT_W_SOCK "$_len${LF}", $$_data_ref;
         }
      }
      else {                                      ## Scalar >> STDOUT
         $_len = length($_data_ref);
         flock $_DAT_LOCK, LOCK_EX;

         print $_OUT_W_SOCK OUTPUT_S_OUT, $LF;
         print $_DAT_W_SOCK "$_len${LF}", $_data_ref;
      }

      flock $_DAT_LOCK, LOCK_UN;

      return;
   };

   ## -------------------------------------------------------------------------

   $_dest_function[SENDTO_STDERR] = sub {

      local $\ = undef; my $_buffer;

      if ($_ref_type) {                           ## Reference >> STDERR
         if ($_ref_type eq 'ARRAY') {
            $_buffer = join('', @$_data_ref);

            $_len = length($_buffer);
            flock $_DAT_LOCK, LOCK_EX;

            print $_OUT_W_SOCK OUTPUT_S_ERR, $LF;
            print $_DAT_W_SOCK "$_len${LF}", $_buffer;
         }
         else {
            $_len = length($$_data_ref);
            flock $_DAT_LOCK, LOCK_EX;

            print $_OUT_W_SOCK OUTPUT_S_ERR, $LF;
            print $_DAT_W_SOCK "$_len${LF}", $$_data_ref;
         }
      }
      else {                                      ## Scalar >> STDERR
         $_len = length($_data_ref);
         flock $_DAT_LOCK, LOCK_EX;

         print $_OUT_W_SOCK OUTPUT_S_ERR, $LF;
         print $_DAT_W_SOCK "$_len${LF}", $_data_ref;
      }

      flock $_DAT_LOCK, LOCK_UN;

      return;
   };

   ## -------------------------------------------------------------------------

   $_dest_function[SENDTO_FILE] = sub {

      return unless (defined $_value);
      local $\ = undef; my $_buffer;

      if ($_ref_type) {                           ## Reference >> File
         if ($_ref_type eq 'ARRAY') {
            $_buffer = join('', @$_data_ref);

            $_len = length($_buffer);
            flock $_DAT_LOCK, LOCK_EX;

            print $_OUT_W_SOCK OUTPUT_S_FLE, $LF;
            print $_DAT_W_SOCK "$_value${LF}$_len${LF}", $_buffer;
         }
         else {
            $_len = length($$_data_ref);
            flock $_DAT_LOCK, LOCK_EX;

            print $_OUT_W_SOCK OUTPUT_S_FLE, $LF;
            print $_DAT_W_SOCK "$_value${LF}$_len${LF}", $$_data_ref;
         }
      }
      else {                                      ## Scalar >> File
         $_len = length($_data_ref);
         flock $_DAT_LOCK, LOCK_EX;

         print $_OUT_W_SOCK OUTPUT_S_FLE, $LF;
         print $_DAT_W_SOCK "$_value${LF}$_len${LF}", $_data_ref;
      }

      flock $_DAT_LOCK, LOCK_UN;

      return;
   };

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

      my MCE $self = $_[0];
      $_dest       = $_[1];
      $_ref_type   = $_[2];
      $_data_ref   = $_[3];
      $_value      = $_[4];

      @_ = ();

      die "Improper use of function call" unless ($_send_init_called);

      return $_dest_function[$_dest]();           ## Call send function
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
   my ($_callback, $_data_ref, $_file, $_len, $_ref_type);
   my ($_total_workers, $_value, $_want_id, $_input_data, $_eof_flag);
   my ($_max_workers, $_user_error, $_user_output, $self);

   my ($_DAT_R_SOCK, $_OUT_R_SOCK, $_MCE_STDERR, $_MCE_STDOUT);
   my ($_I_SEP, $_O_SEP, $_total_ended, $_input_glob, $_chunk_size);
   my ($_input_size, $_offset_pos, $_single_dim, $_use_slurpio);

   my ($_total_exited);

   ## Create hash structure containing various output functions.
   my %_output_function = (

      OUTPUT_W_EXT.$LF => sub {                   ## Worker has exited job
         $_total_exited += 1;

         return;
      },

      OUTPUT_W_DNE.$LF => sub {                   ## Worker has completed job
         $_total_workers -= 1;

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
         $_data_ref = thaw($_buffer);

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
         my $_buffer;

         chomp($_file = <$_DAT_R_SOCK>);
         chomp($_len  = <$_DAT_R_SOCK>);

         read $_DAT_R_SOCK, $_buffer, $_len;

         open my $_OUT_FILE, '>>', $_file or die "$_file: $!\n";
         binmode $_OUT_FILE;
         print   $_OUT_FILE $_buffer;
         close   $_OUT_FILE;
         undef   $_OUT_FILE;

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
      $_max_workers = $self->{max_workers};
      $_use_slurpio = $self->{use_slurpio};
      $_user_output = $self->{user_output};
      $_user_error  = $self->{user_error};
      $_single_dim  = $self->{_single_dim};

      $_total_ended = $_total_exited = $_eof_flag = 0;

      if (defined $_input_data && ref $_input_data eq 'ARRAY') {
         $_input_size = @$_input_data;
         $_offset_pos = 0;
      }
      else {
         $_input_size = $_offset_pos = 0;
      }

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
         warn("[ $_total_ended ] workers ended prematurely${LF}");
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
   my $_run_mode    = $self->{_run_mode};
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
   local $\ = undef; print $_OUT_W_SOCK OUTPUT_W_DNE, $LF;

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

   my ($_wid, $_len, $_buffer, $_params_ref);
   my $_COM_W_SOCK = $self->{_com_w_sock};
 
   while (1) {

      {
         local $\ = undef; local $/ = $LF;

         ## Obtain lock.
         flock $_COM_LOCK, LOCK_EX;

         ## Wait till next job request.
         $_wid = <$_COM_W_SOCK>;
         last unless (defined $_wid);

         ## End loop if an invalid reply.
         chomp $_wid; last if ($_wid !~ /\A(?:_exit|\d+)\z/);

         ## Return to caller if instructed to exit.
         if ($_wid eq '_exit') {
            print $_COM_W_SOCK $$, $LF;
            flock $_COM_LOCK, LOCK_UN;
            return 0;
         }

         ## Retrieve params data.
         chomp($_len = <$_COM_W_SOCK>);
         read $_COM_W_SOCK, $_buffer, $_len;

         print $_COM_W_SOCK $$, $LF;
         flock $_COM_LOCK, LOCK_UN;

         $_params_ref = thaw($_buffer);
      }

      ## Update ID. Process request.
      $self->{_wid} = $_wid;
      _worker_do($self, $_params_ref);
      undef $_params_ref;
   }

   ## -------------------------------------------------------------------------

   ## Notify the main process a worker has ended. This code block is only
   ## executed if an invalid reply was received above.

   local $\ = undef;
   print $_COM_W_SOCK $$, $LF;
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

   @_ = ();

   ## Commented out -- fails with forks and forks::shared under FreeBSD.
   ## die "Private method called" unless (caller)[0]->isa( ref($self) );

   $SIG{PIPE} = \&_NOOP;

   my $_sess_dir = $self->{_sess_dir};

   $self->{_com_r_sock}  = $self->{_dat_r_sock}  = $self->{_out_r_sock} = undef;
   $self->{flush_stderr} = $self->{flush_stdout} = undef;
   $self->{stderr_file}  = $self->{stdout_file}  = undef;
   $self->{user_error}   = $self->{user_output}  = undef;

   $MCE::Signal::mce_spawned_ref = undef;
   %_mce_spawned = ();
   $self->{_wid} = $_wid;

   ## Init runtime vars for send.
   _do_send_init($self);

   ## Open lock files.
   open $_COM_LOCK, '+>> :stdio', "$_sess_dir/com.lock";
   open $_DAT_LOCK, '+>> :stdio', "$_sess_dir/dat.lock";

   ## Wait till MCE completes spawning.
   flock $_COM_LOCK, LOCK_SH;
   flock $_COM_LOCK, LOCK_UN;

   ## Enter loop.
   my $_status = _worker_loop($self);

   ## Wait till MCE completes exit notification.
   flock $_DAT_LOCK, LOCK_SH;
   flock $_DAT_LOCK, LOCK_UN;

   close $_DAT_LOCK; undef $_DAT_LOCK;
   close $_COM_LOCK; undef $_COM_LOCK;

   return;
}

1;

__END__

###############################################################################
## ----------------------------------------------------------------------------
## Module Usage.
##
###############################################################################

=head1 NAME

MCE - Many-Core Engine for Perl. Provides parallel processing cabilities.

=head1 VERSION

This document describes MCE version 1.005

=head1 SYNOPSIS

 use MCE;

 ## A new instance shown with all available options.

 my $mce = MCE->new(

    tmp_dir      => $tmp_dir,

        ## Default is $MCE::Signal::tmp_dir which points to
        ## $ENV{TEMP} if defined. Otherwise, tmp_dir points
        ## to /tmp.

    input_data   => $input_file,    ## Default is undef

        ## input_data => '/path/to/file' for input file
        ## input_data => \@array for input array
        ## input_data => \*FILE_HNDL for file handle
        ## input_data => \$scalar to treat like file

    chunk_size   => 2000,           ## Default is 500

        ## Less than or equal to 8192 is number of records.
        ## Greater than 8192 is number of bytes. MCE reads
        ## till the end of record before calling user_func.

        ## chunk_size =>     1,     ## Consists of 1 record
        ## chunk_size =>  1000,     ## Consists of 1000 records
        ## chunk_size => 16384,     ## Approximate 16384 bytes
        ## chunk_size => 50000,     ## Approximate 50000 bytes

    max_workers  => 8,              ## Default is 2
    use_slurpio  => 1,              ## Default is 0
    use_threads  => 1,              ## Default is 0 or 1

        ## Number of workers to spawn, whether or not to enable
        ## slurpio when reading files (passes raw chunk to user
        ## function), and whether or not to use threads versus
        ## forking new workers. Use_threads defaults to 1 only
        ## when script contains 'use threads' or 'use forks'
        ## and 'use_threads' is not specified. when wanting
        ## threads, please include thread modules prior to
        ## MCE. Load MCE as the very last module.

    job_delay    => 0.035,          ## Default is undef
    spawn_delay  => 0.150,          ## Default is undef
    submit_delay => 0.001,          ## Default is undef

        ## How long to wait before spawning, job (run),
        ## and params submission to workers.

    user_begin   => \&user_begin,   ## Default is undef
    user_func    => \&user_func,    ## Default is undef
    user_end     => \&user_end,     ## Default is undef

        ## Think of user_begin, user_func, user_end like the awk
        ## scripting language:
        ##    awk 'BEGIN { ... } { ... } END { ... }'

        ## MCE workers calls user_begin once per job, then
        ## calls user_func repeatedly until no chunks remain.
        ## Afterwards, user_end is called.

    user_error   => \&user_error,   ## Default is undef
    user_output  => \&user_output,  ## Default is undef

        ## When workers call the following functions, MCE will
        ## pass the data to user_error/user_output if defined.
        ## $self->sendto('stderr', 'Sending to STDERR');
        ## $self->sendto('stdout', 'Sending to STDOUT');

    stderr_file  => 'err_file',     ## Default is STDERR
    stdout_file  => 'out_file',     ## Default is STDOUT

        ## Or to file. User_error/user_output take precedence.

    flush_stderr => 1,              ## Default is 0
    flush_stdout => 1,              ## Default is 0

        ## Flush standard error or output immediately.
 );

 ## Run calls spawn, kicks off job, workers call user_begin,
 ## user_func, user_end. Run shuts down workers afterwards.
 ## The run method can take an optional argument. Default is
 ## 1 to auto-shutdown workers after processing.

 $mce->run();
 $mce->run(0);                      ## 0 disables auto-shutdown

 ## Or, spawn workers early.

 $mce->spawn();

 ## Acquire data arrays and/or input_files. The same pool of
 ## workers are used. Process calls run(0).

 $mce->process(\@input_data_1);     ## Process arrays
 $mce->process(\@input_data_2);
 $mce->process(\@input_data_n);

 $mce->process('input_file_1');     ## Process files
 $mce->process('input_file_2');
 $mce->process('input_file_n');

 ## Shutdown workers afterwards.

 $mce->shutdown();

=head2 SYNTAX FOR USER_BEGIN & USER_END

 ## Both user_begin and user_end functions, if specified, behave
 ## similarly to awk 'BEGIN { ... } { ... } END { ... }'.

 ## Each worker calls this once prior to processing.

 sub user_begin {                   ## Optional via user_begin option

    my $self = shift;

    $self->{wk_total_rows} = 0;     ## Prefix variables with wk_
 }

 ## And once after completion.

 sub user_end {                     ## Optional via user_end option

    my $self = shift;

    printf "## %d: Processed %d rows\n",
       $self->wid(), $self->{wk_total_rows};
 }

=head2 SYNTAX FOR USER_FUNC (with use_slurpio => 0 option)

 ## MCE passes a reference to an array containing the chunk data.

 sub user_func {

    my ($self, $chunk_ref, $chunk_id) = @_;

    foreach my $row ( @{ $chunk_ref } ) {
       print $row;
       $self->{wk_total_rows} += 1;
    }
 }

=head2 SYNTAX FOR USER_FUNC (with use_slurpio => 1 option)

 ## MCE passes a reference to a scalar containing the raw chunk data.

 sub user_func {

    my ($self, $chunk_ref, $chunk_id) = @_;

    my $count = () = $$chunk_ref =~ /abc/;
 }

=head2 SYNTAX FOR USER_ERROR & USER_OUTPUT

 ## MCE will direct $self->sendto('stderr/out', ...) calls to these
 ## functions in a serialized fashion. This is handy if one wants to
 ## filter, modify, and/or send the data elsewhere.

 sub user_error {                   ## Optional via user_error option

    my $error = shift;

    print LOGERR $error;
 }

 sub user_output {                  ## Optional via user_output option

    my $output = shift;

    print LOGOUT $output;
 }

=head1 DESCRIPTION

Many-core Engine (MCE) for Perl helps enable a new level of performance by
maximizing all available cores. One immediate benefit is that MCE does not
fork a new worker process per each element in an array. Instead, MCE follows
a bank queuing model. Imagine the line being the data and bank-tellers the
parallel workers. MCE enhances that model by adding the ability to chunk
the next n elements from the input stream to the next available worker.

=head2 MCE EXAMPLE WITH CHUNK_SIZE => 1

 ## Imagine a long running process and wanting to parallelize an array
 ## against a pool of workers.

 my @input_data  = (0 .. 18000 - 1);
 my $max_workers = 3;
 my $order_id    = 1;
 my %result;

 ## Callback function for displaying results. The logic below shows how
 ## one can display results immediately while still preserving output
 ## order. The %result hash is a temporary cache to store results
 ## for out-of-order replies.

 sub display_result {

    my ($wk_result, $chunk_id) = @_;
    $result{$chunk_id} = $wk_result;

    while (1) {
       last unless exists $result{$order_id};

       printf "i: %d sqrt(i): %f\n",
          $input_data[$order_id - 1], $result{$order_id};

       delete $result{$order_id};
       $order_id++;
    }
 }

 ## Compute via MCE.

 my $mce = MCE->new(
    input_data  => \@input_data,
    max_workers => $max_workers,
    chunk_size  => 1,

    user_func => sub {

       my ($self, $chunk_ref, $chunk_id) = @_;
       my $wk_result = sqrt($chunk_ref->[0]);

       $self->do('display_result', $wk_result, $chunk_id);
    }
 );

 $mce->run();

=head2 FOREACH SUGAR METHOD

 ## Compute via MCE. Foreach implies chunk_size => 1.

 my $mce = MCE->new(
    max_workers => 3
 );

 ## Worker calls code block passing a reference to an array containing
 ## one item. Use $chunk_ref->[0] to retrieve the single element.

 $mce->foreach(\@input_data, sub {

    my ($self, $chunk_ref, $chunk_id) = @_;
    my $wk_result = sqrt($chunk_ref->[0]);

    $self->do('display_result', $wk_result, $chunk_id);
 });

=head2 MCE EXAMPLE WITH CHUNK_SIZE => 500

 ## Chunking reduces overhead many folds. Instead of passing a single
 ## item from @input_data, a chunk of $chunk_size is sent instead to
 ## the next available worker.

 my @input_data  = (0 .. 385000 - 1);
 my $max_workers = 3;
 my $chunk_size  = 500;
 my $order_id    = 1;
 my %result;

 ## Callback function for displaying results.

 sub display_result {

    my ($wk_result, $chunk_id) = @_;
    $result{$chunk_id} = $wk_result;

    while (1) {
       last unless exists $result{$order_id};
       my $i = ($order_id - 1) * $chunk_size;

       for ( @{ $result{$order_id} } ) {
          printf "i: %d sqrt(i): %f\n", $input_data[$i++], $_;
       }

       delete $result{$order_id};
       $order_id++;
    }
 }

 ## Compute via MCE.

 my $mce = MCE->new(
    input_data  => \@input_data,
    max_workers => $max_workers,
    chunk_size  => $chunk_size,

    user_func => sub {

       my ($self, $chunk_ref, $chunk_id) = @_;
       my @wk_result;

       for ( @{ $chunk_ref } ) {
          push @wk_result, sqrt($_);
       }

       $self->do('display_result', \@wk_result, $chunk_id);
    }
 );

 $mce->run();

=head2 FORCHUNK SUGAR METHOD

 ## Compute via MCE.

 my $mce = MCE->new(
    max_workers => $max_workers,
    chunk_size  => $chunk_size
 );

 ## Below, $chunk_ref is a reference to an array containing the next
 ## $chunk_size items from @input_data.

 $mce->forchunk(\@input_data, sub {

    my ($self, $chunk_ref, $chunk_id) = @_;
    my @wk_result;

    for ( @{ $chunk_ref } ) {
       push @wk_result, sqrt($_);
    }

    $self->do('display_result', \@wk_result, $chunk_id);
 });

=head2 LAST & NEXT METHODS

 ## Both last and next methods work inside foreach, forchunk,
 ## and user_func code blocks.

 ## ->last: Worker immediately exits the chunking loop

 my @list = (1..80);

 $mce->forchunk(\@list, { chunk_size => 2 }, sub {

    my ($self, $chunk_ref, $chunk_id) = @_;

    $self->last if ($chunk_id > 4);

    my @output = ();

    for my $rec ( @{ $chunk_ref } ) {
       push @output, $rec, "\n";
    }

    $self->sendto('stdout', \@output);
 });

 -- Output (each chunk above consists of 2 elements)

 1
 2
 3
 4
 5
 6
 7
 8

 ## ->next: Worker starts the next iteration of the chunking loop

 my @list = (1..80);

 $mce->forchunk(\@list, { chunk_size => 4 }, sub {

    my ($self, $chunk_ref, $chunk_id) = @_;

    $self->next if ($chunk_id < 20);

    my @output = ();

    for my $rec ( @{ $chunk_ref } ) {
       push @output, $rec, "\n";
    }

    $self->sendto('stdout', \@output);
 });

 -- Output (each chunk above consists of 4 elements)

 77
 78
 79
 80

=head2 MISCELLANEOUS METHODS

 ## Notifies workers to abort after processing the current chunk. The
 ## abort method is only meaningful when processing input_data.

    $self->abort();

 ## Worker exits current job.

    $self->exit();

 ## Returns worker ID of worker.

    $self->wid();

=head2 DO & SENDTO METHODS

MCE can serialized data transfers from worker processes via helper functions.
The main MCE thread will process these in a serial fashion. This utilizes the
Storable Perl module for passing data from a worker process to the main MCE
thread. In addition, the callback function can optionally return a reply.

 [ $reply = ] $self->do('callback_func' [, $arg1, $arg2, ...]);

 ## Passing arguments to a callback function using references & scalar:

 sub callback_func {  
    my ($array_ref, $hash_ref, $scalar_ref, $scalar) = @_;
    ...
 }

 $self->do('main::callback_func', \@a, \%h, \$s, 'hello');

 ## MCE knows if wanting void, a list, a hash, or a scalar return value.

 $self->do('callback_func' [, ...]);
 my @array  = $self->do('callback_func' [, ...]);
 my %hash   = $self->do('callback_func' [, ...]);
 my $scalar = $self->do('callback_func' [, ...]);

 ## Display content to STDOUT or STDERR. Same as above, supports only 1
 ## level scalar values for arrays.

 $self->sendto('stdout', \@array);
 $self->sendto('stdout', \$scalar);
 $self->sendto('stdout', $scalar);

 $self->sendto('stderr', \@array);
 $self->sendto('stderr', \$scalar);
 $self->sendto('stderr', $scalar);

 ## Append content to the end of file. Supports 1 level scalar values for
 ## arrays.

 $self->sendto('file', \@array, '/path/to/file');
 $self->sendto('file', \$scalar, '/path/to/file');
 $self->sendto('file', $scalar, '/path/to/file');

=head1 EXAMPLES

MCE comes with various examples showing real-world use case scenarios on
parallelizing something as small as cat (try with -n) to greping for
patterns and word count aggregation.

 cat.pl    Concatenation script, similar to the cat binary.
 egrep.pl  Egrep script, similar to the egrep binary.
 wc.pl     Word count script, similar to the wc binary.

 findnull.pl
           A parallel driven script to report lines containing
           null fields. It's many times faster than the binary
           egrep command. Try against a large file containing
           very long lines.

 scaling_pings.pl
           Perform ping test and report back failing IPs to
           standard output.

 tbray/wf_mce1.pl, wf_mce2.pl, wf_mce3.pl
           An implementation of wide finder utilizing MCE.
           As fast as MMAP IO when file resides in OS FS cache.
           2x ~ 3x faster when reading directly from disk.

 foreach.pl
 forchunk.pl
           These take the same sqrt example from Parallel::Loops
           and measures the overhead of the engine. The number
           indicates the size of @input which can be submitted
           and results displayed in 1 second.

           Parallel::Loops:     600  Forking each @input is expensive
           MCE foreach....:  18,000  Sends result after each @input
           MCE forchunk...: 385,000  Chunking reduces overhead 

=head1 REQUIREMENTS

Perl 5.8.0 or later

=head1 SEE ALSO

L<MCE::Signal>

=head1 SOURCE

The source is hosted at: L<http://code.google.com/p/many-core-engine-perl/>

=head1 AUTHOR

Mario E. Roy, S<E<lt>marioeroy AT gmail DOT comE<gt>>

=head1 COPYRIGHT AND LICENSE

Copyright (C) 2012 by Mario E. Roy

MCE is free software; you can redistribute it and/or modify it under the same
terms as Perl itself.

=cut
