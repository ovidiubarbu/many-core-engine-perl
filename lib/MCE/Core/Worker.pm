###############################################################################
## ----------------------------------------------------------------------------
## MCE::Core::Worker - Core methods for the worker process.
##
## This package provides main, loop, and relevant methods used internally by
## the worker process.
##
## There is no public API.
##
###############################################################################

package MCE::Core::Worker;

use strict;
use warnings;

our $VERSION = '1.699';

## Items below are folded into MCE.

package MCE;

no warnings 'threads';
no warnings 'recursion';
no warnings 'uninitialized';

use bytes;

###############################################################################
## ----------------------------------------------------------------------------
## Internal do, gather and send related functions for serializing data to
## destination. User functions for handling gather, queue or void.
##
###############################################################################

{
   my (
      $_dest, $_len, $_task_id, $_user_func, $_value, $_want_id, $_tag,
      $_DAT_LOCK, $_DAT_W_SOCK, $_DAU_W_SOCK, $_chn, $_dat_ex, $_dat_un,
      $_GAT_LOCK, $_GAT_W_SOCK, $_GAU_W_SOCK, $_ghn, $_gat_ex, $_gat_un
   );

   ## Create array structure containing various send functions.
   my @_dest_function = ();

   $_dest_function[SENDTO_FILEV2] = sub {         ## Content >> File

      return unless (defined $_value);
      local $\ = undef if (defined $\);

      if (length ${ $_[0] }) {
         $_dat_ex->();
         print {$_DAT_W_SOCK} OUTPUT_F_SND . $LF . $_chn . $LF;
         print {$_DAU_W_SOCK} $_value . $LF . length(${ $_[0] }) . $LF;
         print {$_DAU_W_SOCK} ${ $_[0] };
         $_dat_un->();
      }

      return;
   };

   $_dest_function[SENDTO_FD] = sub {             ## Content >> File descriptor

      return unless (defined $_value);
      local $\ = undef if (defined $\);

      if (length ${ $_[0] }) {
         $_dat_ex->();
         print {$_DAT_W_SOCK} OUTPUT_D_SND . $LF . $_chn . $LF;
         print {$_DAU_W_SOCK} $_value . $LF . length(${ $_[0] }) . $LF;
         print {$_DAU_W_SOCK} ${ $_[0] };
         $_dat_un->();
      }

      return;
   };

   $_dest_function[SENDTO_STDOUT] = sub {         ## Content >> STDOUT

      local $\ = undef if (defined $\);

      if (length ${ $_[0] }) {
         $_dat_ex->();
         print {$_DAT_W_SOCK} OUTPUT_O_SND . $LF . $_chn . $LF;
         print {$_DAU_W_SOCK} length(${ $_[0] }) . $LF;
         print {$_DAU_W_SOCK} ${ $_[0] };
         $_dat_un->();
      }

      return;
   };

   $_dest_function[SENDTO_STDERR] = sub {         ## Content >> STDERR

      local $\ = undef if (defined $\);

      if (length ${ $_[0] }) {
         $_dat_ex->();
         print {$_DAT_W_SOCK} OUTPUT_E_SND . $LF . $_chn . $LF;
         print {$_DAU_W_SOCK} length(${ $_[0] }) . $LF;
         print {$_DAU_W_SOCK} ${ $_[0] };
         $_dat_un->();
      }

      return;
   };

   ## -------------------------------------------------------------------------

   sub _do_callback {

      my ($self, $_buf, $_aref);  ($self, $_value, $_aref) = @_;

      unless (defined wantarray) {
         $_want_id = WANTS_UNDEF;
      } elsif (wantarray) {
         $_want_id = WANTS_ARRAY;
      } else {
         $_want_id = WANTS_SCALAR;
      }

      ## Crossover: Send arguments

      if (scalar @{ $_aref } > 0) {               ## Multiple Args >> Callback
         if (scalar @{ $_aref } > 1 || ref $_aref->[0]) {
            $_tag = OUTPUT_A_CBK;
            $_buf = $self->{freeze}($_aref);
            $_len = length $_buf; local $\ = undef if (defined $\);

            $_dat_ex->();
            print {$_DAT_W_SOCK} $_tag . $LF . $_chn . $LF;
            print {$_DAU_W_SOCK} $_want_id . $LF . $_value . $LF . $_len . $LF;
            print {$_DAU_W_SOCK} $_buf;

         }
         else {                                   ## Scalar >> Callback
            $_tag = OUTPUT_S_CBK;
            $_len = length $_aref->[0]; local $\ = undef if (defined $\);

            $_dat_ex->();
            print {$_DAT_W_SOCK} $_tag . $LF . $_chn . $LF;
            print {$_DAU_W_SOCK} $_want_id . $LF . $_value . $LF . $_len . $LF;
            print {$_DAU_W_SOCK} $_aref->[0];
         }
      }
      else {                                      ## No Args >> Callback
         $_tag = OUTPUT_N_CBK;
         local $\ = undef if (defined $\);

         $_dat_ex->();
         print {$_DAT_W_SOCK} $_tag . $LF . $_chn . $LF;
         print {$_DAU_W_SOCK} $_want_id . $LF . $_value . $LF;
      }

      ## Crossover: Receive return value

      if ($_want_id == WANTS_UNDEF) {
         $_dat_un->();
         return;
      }
      elsif ($_want_id == WANTS_ARRAY) {
         local $/ = $LF if (!$/ || $/ ne $LF);
         chomp($_len = <$_DAU_W_SOCK>);

         read($_DAU_W_SOCK, $_buf, $_len || 0);
         $_dat_un->();

         return @{ $self->{thaw}($_buf) };
      }
      else {
         local $/ = $LF if (!$/ || $/ ne $LF);
         chomp($_want_id = <$_DAU_W_SOCK>);
         chomp($_len     = <$_DAU_W_SOCK>);

         if ($_len >= 0) {
            read($_DAU_W_SOCK, $_buf, $_len || 0);
            $_dat_un->();

            return $_buf if ($_want_id == WANTS_SCALAR);
            return $self->{thaw}($_buf);
         }
         else {
            $_dat_un->();
            return;
         }
      }
   }

   ## -------------------------------------------------------------------------

   sub _do_gather {

      my $_buf; my ($self, $_aref) = @_;

      return unless (scalar @{ $_aref });

      if (scalar @{ $_aref } > 1) {
         $_tag = OUTPUT_A_GTR;
         $_buf = $self->{freeze}($_aref);
         $_len = length $_buf;
      }
      elsif (ref $_aref->[0]) {
         $_tag = OUTPUT_R_GTR;
         $_buf = $self->{freeze}($_aref->[0]);
         $_len = length $_buf;
      }
      else {
         $_tag = OUTPUT_S_GTR;
         if (defined $_aref->[0]) {
            $_len = length $_aref->[0]; local $\ = undef if (defined $\);

            $_gat_ex->();
            print {$_GAT_W_SOCK} $_tag . $LF . $_ghn . $LF;
            print {$_GAU_W_SOCK} $_task_id . $LF . $_len . $LF;
            print {$_GAU_W_SOCK} $_aref->[0];
            $_gat_un->();

            return;
         }
         else {
            $_buf = '';
            $_len = -1;
         }
      }

      local $\ = undef if (defined $\);

      $_gat_ex->();
      print {$_GAT_W_SOCK} $_tag . $LF . $_ghn . $LF;
      print {$_GAU_W_SOCK} $_task_id . $LF . $_len . $LF;
      print {$_GAU_W_SOCK} $_buf if (length $_buf);
      $_gat_un->();

      return;
   }

   ## -------------------------------------------------------------------------

   sub _do_send {

      my $_data_ref; my $self = shift;

      $_dest = shift; $_value = shift;

      if (scalar @_ > 1) {
         $_data_ref = \join('', @_);
      }
      elsif (my $_ref = ref $_[0]) {
         if ($_ref eq 'SCALAR') {
            $_data_ref = $_[0];
         }
         elsif ($_ref eq 'ARRAY') {
            $_data_ref = \join('', @{ $_[0] });
         }
         elsif ($_ref eq 'HASH') {
            $_data_ref = \join('', %{ $_[0] });
         }
         else {
            $_data_ref = \join('', @_);
         }
      }
      else {
         $_data_ref = \$_[0];
      }

      $_dest_function[$_dest]($_data_ref);

      return;
   }

   sub _do_send_glob {

      my ($self, $_glob, $_fd, $_data_ref) = @_;

      if ($self->{_wid} > 0) {
         if ($_fd == 1) {
            _do_send($self, SENDTO_STDOUT, undef, $_data_ref);
         }
         elsif ($_fd == 2) {
            _do_send($self, SENDTO_STDERR, undef, $_data_ref);
         }
         else {
            _do_send($self, SENDTO_FD, $_fd, $_data_ref);
         }
      }
      else {
         my $_fh = qualify_to_ref($_glob, caller);
         local $\ = undef if (defined $\);
         print {$_fh} ${ $_data_ref };
      }

      return;
   }

   ## -------------------------------------------------------------------------

   sub _do_send_init {

      my ($self) = @_;

      $_task_id = $self->{_task_id};

      $_ghn        = $self->{_chn};
      $_GAT_LOCK   = $self->{_dat_lock};
      $_GAT_W_SOCK = $self->{_dat_w_sock}->[0];
      $_GAU_W_SOCK = $self->{_dat_w_sock}->[$_ghn];

      $_gat_ex = sub { sysread(  $_GAT_LOCK->{_r_sock}, my $_b, 1 ) };
      $_gat_un = sub { syswrite( $_GAT_LOCK->{_w_sock}, '0' ) };

      if (!defined $MCE::Shared::_HDLR ||
            refaddr($self) == refaddr($MCE::Shared::_HDLR)) {

         ( $_chn, $_DAT_LOCK, $_DAT_W_SOCK, $_DAU_W_SOCK ) =
         ( $_ghn, $_GAT_LOCK, $_GAT_W_SOCK, $_GAU_W_SOCK );

         ( $_dat_ex, $_dat_un ) = ( $_gat_ex, $_gat_un );
      }
      else {
         $_chn = $self->{_wid} % $MCE::Shared::_HDLR->{_data_channels} + 1;

         $_DAT_LOCK   = $MCE::Shared::_HDLR->{'_mutex_'.$_chn};
         $_DAT_W_SOCK = $MCE::Shared::_HDLR->{_dat_w_sock}->[0];
         $_DAU_W_SOCK = $MCE::Shared::_HDLR->{_dat_w_sock}->[$_chn];

         $_dat_ex = sub { sysread(  $_DAT_LOCK->{_r_sock}, my $_b, 1 ) };
         $_dat_un = sub { syswrite( $_DAT_LOCK->{_w_sock}, '0' ) };
      }

      local ($!, $@);

      eval { select STDERR; $| = 1 };
      eval { select STDOUT; $| = 1 };

      return;
   }

   sub _do_send_clear {

      my ($self) = @_;

      $_dest = $_len = $_task_id = $_user_func = $_value = $_want_id = undef;
      $_dat_ex = $_dat_un = $_gat_ex = $_gat_un = $_tag = undef;
      $_DAT_LOCK = $_DAT_W_SOCK = $_DAU_W_SOCK = $_chn = undef;
      $_GAT_LOCK = $_GAT_W_SOCK = $_GAU_W_SOCK = $_ghn = undef;

      return;
   }

   ## -------------------------------------------------------------------------

   sub _do_user_func {

      my ($self, $_chunk, $_chunk_id) = @_;

      $self->{_chunk_id} = $_chunk_id;
      $_user_func->($self, $_chunk, $_chunk_id);

      return;
   }

   sub _do_user_func_init {

      my ($self) = @_;

      $_user_func = $self->{user_func};

      return;
   }
}

###############################################################################
## ----------------------------------------------------------------------------
## Worker process -- Do.
##
###############################################################################

sub _worker_do {

   my ($self, $_params_ref) = @_;

   @_ = ();

   ## Set options.
   $self->{_abort_msg}  = $_params_ref->{_abort_msg};
   $self->{_run_mode}   = $_params_ref->{_run_mode};
   $self->{_single_dim} = $_params_ref->{_single_dim};
   $self->{use_slurpio} = $_params_ref->{_use_slurpio};
   $self->{parallel_io} = $_params_ref->{_parallel_io};
   $self->{RS}          = $_params_ref->{_RS};

   _do_user_func_init($self);

   ## Init local vars.
   my $_chn        = $self->{_chn};
   my $_DAT_LOCK   = $self->{_dat_lock};
   my $_DAT_W_SOCK = $self->{_dat_w_sock}->[0];
   my $_DAU_W_SOCK = $self->{_dat_w_sock}->[$_chn];
   my $_run_mode   = $self->{_run_mode};
   my $_task_id    = $self->{_task_id};
   my $_task_name  = $self->{task_name};

   ## Do not override params if defined in user_tasks during instantiation.
   for my $_p (qw(bounds_only chunk_size interval sequence user_args)) {
      if (defined $_params_ref->{"_${_p}"}) {
         $self->{$_p} = $_params_ref->{"_${_p}"}
            unless (defined $self->{_task}->{$_p});
      }
   }

   ## Assign user function.
   $self->{_wuf} = \&_do_user_func;

   ## Set time_block & start_time values for interval.
   if (defined $self->{interval}) {
      my $_i     = $self->{interval};
      my $_delay = $_i->{delay} * $_i->{max_nodes};

      $self->{_i_app_tb} = $_delay * $self->{max_workers};

      $self->{_i_app_st} =
         $_i->{_time} + ($_delay / $_i->{max_nodes} * $_i->{node_id});

      $self->{_i_wrk_st} =
         ($self->{_task_wid} - 1) * $_delay + $self->{_i_app_st};
   }

   ## Call user_begin if defined.
   if (defined $self->{user_begin}) {
      $self->{user_begin}($self, $_task_id, $_task_name);
   }

   ## Call worker function.
   if ($_run_mode eq 'sequence') {
      require MCE::Core::Input::Sequence
         unless (defined $MCE::Core::Input::Sequence::VERSION);
      _worker_sequence_queue($self);
   }
   elsif (defined $self->{_task}->{sequence}) {
      require MCE::Core::Input::Generator
         unless (defined $MCE::Core::Input::Generator::VERSION);
      _worker_sequence_generator($self);
   }
   elsif ($_run_mode eq 'array') {
      require MCE::Core::Input::Request
         unless (defined $MCE::Core::Input::Request::VERSION);
      _worker_request_chunk($self, REQUEST_ARRAY);
   }
   elsif ($_run_mode eq 'glob') {
      require MCE::Core::Input::Request
         unless (defined $MCE::Core::Input::Request::VERSION);
      _worker_request_chunk($self, REQUEST_GLOB);
   }
   elsif ($_run_mode eq 'iterator') {
      require MCE::Core::Input::Iterator
         unless (defined $MCE::Core::Input::Iterator::VERSION);
      _worker_user_iterator($self);
   }
   elsif ($_run_mode eq 'file') {
      require MCE::Core::Input::Handle
         unless (defined $MCE::Core::Input::Handle::VERSION);
      _worker_read_handle($self, READ_FILE, $_params_ref->{_input_file});
   }
   elsif ($_run_mode eq 'memory') {
      require MCE::Core::Input::Handle
         unless (defined $MCE::Core::Input::Handle::VERSION);
      _worker_read_handle($self, READ_MEMORY, $self->{input_data});
   }
   elsif (defined $self->{user_func}) {
      $self->{_chunk_id} = $self->{_task_wid};
      $self->{user_func}->($self);
   }

   undef $self->{_next_jmp} if (defined $self->{_next_jmp});
   undef $self->{_last_jmp} if (defined $self->{_last_jmp});
   undef $self->{user_data} if (defined $self->{user_data});

   ## Call user_end if defined.
   if (defined $self->{user_end}) {
      $self->{user_end}($self, $_task_id, $_task_name);
   }

   ## Notify the main process a worker has completed.
   local $\ = undef if (defined $\);

   $_DAT_LOCK->lock();

   if (exists $self->{_rla_return}) {
      print {$_DAT_W_SOCK} OUTPUT_W_RLA . $LF . $_chn . $LF;
      print {$_DAU_W_SOCK} (delete $self->{_rla_return}) . $LF;
   }

   print {$_DAT_W_SOCK} OUTPUT_W_DNE . $LF . $_chn . $LF;
   print {$_DAU_W_SOCK} $_task_id . $LF;

   $_DAT_LOCK->unlock();

   return;
}

###############################################################################
## ----------------------------------------------------------------------------
## Worker process -- Loop.
##
###############################################################################

sub _worker_loop {

   my ($self) = @_;

   @_ = ();

   my ($_response, $_len, $_buf, $_params_ref);

   my $_COM_LOCK   = $self->{_com_lock};
   my $_COM_W_SOCK = $self->{_com_w_sock};
   my $_job_delay  = $self->{job_delay};
   my $_wid        = $self->{_wid};

   while (1) {

      {
         local $\ = undef; local $/ = $LF;
         $_COM_LOCK->lock();

         ## Wait until next job request.
         $_response = <$_COM_W_SOCK>;
         print {$_COM_W_SOCK} $_wid . $LF;

         last unless (defined $_response);
         chomp $_response;

         ## End loop if an invalid reply.
         last if ($_response !~ /\A(?:\d+|_data|_exit)\z/);

         if ($_response eq '_data') {
            ## Acquire and process user data.
            chomp($_len = <$_COM_W_SOCK>);
            read $_COM_W_SOCK, $_buf, $_len;

            print {$_COM_W_SOCK} $_wid . $LF;
            $_COM_LOCK->unlock();

            $self->{user_data} = $self->{thaw}($_buf);
            undef $_buf;

            if (defined $_job_delay && $_job_delay > 0.0) {
               sleep $_job_delay * $_wid;
            }

            _worker_do($self, { });
         }
         else {
            ## Return to caller if instructed to exit.
            if ($_response eq '_exit') {
               $_COM_LOCK->unlock();
               return;
            }

            ## Retrieve params data.
            chomp($_len = <$_COM_W_SOCK>);
            read $_COM_W_SOCK, $_buf, $_len;

            print {$_COM_W_SOCK} $_wid . $LF;
            $_COM_LOCK->unlock();

            $_params_ref = $self->{thaw}($_buf);
            undef $_buf;
         }
      }

      ## Start over if the last response was for processing user data.
      next if ($_response eq '_data');

      ## Wait until MCE completes params submission to all workers.
      sysread $self->{_bse_r_sock}, (my $_c), 1;

      if (defined $_job_delay && $_job_delay > 0.0) {
         sleep $_job_delay * $_wid;
      }

      _worker_do($self, $_params_ref);

      undef $_params_ref;
   }

   ## Notify the main process a worker has ended. The following is executed
   ## when an invalid reply was received above (not likely to occur).

   $_COM_LOCK->unlock();

   die "worker $self->{_wid} has ended prematurely";
}

###############################################################################
## ----------------------------------------------------------------------------
## Worker process -- Main.
##
###############################################################################

sub _worker_main {

   my ( $self, $_wid, $_task, $_task_id, $_task_wid, $_params,
        $_plugin_worker_init, $_is_winenv ) = @_;

   @_ = ();

   if (exists $self->{input_data}) {
      my $_ref = ref $self->{input_data};
      delete $self->{input_data} if ($_ref && $_ref ne 'SCALAR');
   }

   ## Define DIE handler.
   local $SIG{__DIE__} = sub {
      if (!defined $^S || $^S) {
         if ( ($INC{'threads.pm'} && threads->tid() != 0) ||
               $ENV{'PERL_IPERL_RUNNING'}
         ) {
            # thread env or running inside IPerl, check stack trace
            my  $_t = Carp::longmess();
            if ($_t =~ /^[^\n]+\n\teval / || $_t =~ /\n\teval [^\n]+\n\tTry/) {
               CORE::die(@_);
            }
         }
         else {
            # normal env, trust $^S
            CORE::die(@_);
         }
      }
      my $_die_msg = (defined $_[0]) ? $_[0] : '';
      local $SIG{__DIE__} = sub { }; local $\ = undef;
      print {*STDERR} $_die_msg;
      $self->exit(255, $_die_msg);
   };

   ## Define status ID.
   my $_use_threads = (defined $_task->{use_threads})
      ? $_task->{use_threads} : $self->{use_threads};

   if ($INC{'threads.pm'} && $_use_threads) {
      $self->{_exit_pid} = 'TID_' . threads->tid();
   } else {
      $self->{_exit_pid} = 'PID_' . $$;
   }

   ## Use options from user_tasks if defined.
   $self->{max_workers} = $_task->{max_workers} if ($_task->{max_workers});
   $self->{chunk_size}  = $_task->{chunk_size}  if ($_task->{chunk_size});
   $self->{gather}      = $_task->{gather}      if ($_task->{gather});
   $self->{interval}    = $_task->{interval}    if ($_task->{interval});
   $self->{sequence}    = $_task->{sequence}    if ($_task->{sequence});
   $self->{task_name}   = $_task->{task_name}   if ($_task->{task_name});
   $self->{user_args}   = $_task->{user_args}   if ($_task->{user_args});
   $self->{user_begin}  = $_task->{user_begin}  if ($_task->{user_begin});
   $self->{user_func}   = $_task->{user_func}   if ($_task->{user_func});
   $self->{user_end}    = $_task->{user_end}    if ($_task->{user_end});

   ## Init runtime vars. Obtain handle to lock files.
   my $_mce_sid  = $self->{_mce_sid};
   my $_sess_dir = $self->{_sess_dir};
   my $_chn;

   if (defined $_params && exists $_params->{_chn}) {
      $_chn = $self->{_chn} = delete $_params->{_chn};
   } else {
      $_chn = $self->{_chn} = $_wid % $self->{_data_channels} + 1;
   }

   $self->{_task_id}  = (defined $_task_id ) ? $_task_id  : 0;
   $self->{_task_wid} = (defined $_task_wid) ? $_task_wid : $_wid;
   $self->{_task}     = $_task;
   $self->{_wid}      = $_wid;

   ## Choose locks for DATA channels.
   $self->{_com_lock} = $self->{'_mutex_0'};
   $self->{_dat_lock} = $self->{'_mutex_'.$_chn};

   ## Delete attributes no longer required after being spawned.
   delete @{ $self }{ qw(
      flush_file flush_stderr flush_stdout stderr_file stdout_file
      on_post_exit on_post_run user_data user_error user_output
      _pids _state _status _thrs _tids
   ) };

   MCE::_clean_sessions($_mce_sid);

   ## Call module's worker_init routine for modules plugged into MCE.
   for my $_p (@{ $_plugin_worker_init }) { $_p->($self); }

   _do_send_init($self);

   ## Begin processing if worker was added during processing. Otherwise,
   ## respond back to the main process if the last worker spawned.
   if (defined $_params) {
      sleep 0.002; _worker_do($self, $_params); undef $_params;
   } else {
      lock $MCE::_WIN_LOCK if ($_is_winenv);
   }

   ## Enter worker loop. Clear worker session after running.
   _worker_loop($self); _do_send_clear($self);

   $self->{_com_lock} = undef;
   $self->{_dat_lock} = undef;

   MCE::_clear_session($_mce_sid);

   ## Wait until MCE completes exit notification.
   local $@; eval {
      sysread $self->{_bse_r_sock}, (my $_c), 1;
   };

   return;
}

1;

