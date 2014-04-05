###############################################################################
## ----------------------------------------------------------------------------
## MCE::Core::Manager - Core methods for the manager process.
##
## This package provides the loop and relevant methods used internally by the
## manager process.
##
## There is no public API.
##
###############################################################################

package MCE::Core::Manager;

our $VERSION = '1.511'; $VERSION = eval $VERSION;

## Items below are folded into MCE.

package MCE;

use strict;
use warnings;

use bytes;

## Warnings are disabled to minimize bits of noise when user or OS signals
## the script to exit. e.g. MCE_script.pl < infile | head

no warnings 'threads'; no warnings 'uninitialized';

###############################################################################
## ----------------------------------------------------------------------------
## Call on task_end after task completion.
##
###############################################################################

sub _task_end {

   my MCE $self = $_[0]; my $_task_id = $_[1];

   if (defined $self->{user_tasks}) {
      my $_task_end = (exists $self->{user_tasks}->[$_task_id]->{task_end})
         ? $self->{user_tasks}->[$_task_id]->{task_end}
         : $self->{task_end};

      if (defined $_task_end) {
         my $_task_name = (exists $self->{user_tasks}->[$_task_id]->{task_name})
            ? $self->{user_tasks}->[$_task_id]->{task_name}
            : $self->{task_name};

         $_task_end->($self, $_task_id, $_task_name);
      }
   }

   return;
}

###############################################################################
## ----------------------------------------------------------------------------
## Process output.
##
## Awaits and processes events from workers. The sendto/do methods tag the
## output accordingly. The hash structure below is key-driven.
##
###############################################################################

sub _output_loop {

   my MCE $self = $_[0]; my $_input_data = $_[1]; my $_input_glob = $_[2];

   my $_plugin_function   = $_[3];
   my $_plugin_loop_begin = $_[4];
   my $_plugin_loop_end   = $_[5];

   @_ = ();

   die "Private method called" unless (caller)[0]->isa( ref($self) );

   my ($_aborted, $_eof_flag, $_syn_flag, %_sendto_fhs, $_value, $_want_id);
   my ($_callback, $_chunk_id, $_chunk_size, $_fd, $_file, $_flush_file);
   my (@_is_c_ref, @_is_h_ref, @_is_q_ref, $_on_post_exit, $_on_post_run);
   my ($_has_user_tasks, $_sess_dir, $_task_id, $_user_error, $_user_output);
   my ($_input_size, $_len, $_offset_pos, $_single_dim, @_gather);
   my ($_exit_id, $_exit_pid, $_exit_status, $_exit_wid, $_sync_cnt);

   my ($_BSB_W_SOCK, $_BSE_W_SOCK, $_DAT_R_SOCK, $_DAU_R_SOCK, $_MCE_STDERR);
   my ($_I_FLG, $_O_FLG, $_I_SEP, $_O_SEP, $_RS, $_RS_FLG, $_MCE_STDOUT);

   ## -------------------------------------------------------------------------
   ## Create hash structure containing various output functions.

   my %_core_output_function = (

      OUTPUT_W_ABT.$LF => sub {                   ## Worker has aborted
         $_aborted = 1;
         return;
      },

      OUTPUT_W_DNE.$LF => sub {                   ## Worker has completed
         chomp($_task_id = <$_DAU_R_SOCK>);
         $self->{_total_running} -= 1;

         if ($_has_user_tasks && $_task_id >= 0) {
            $self->{_task}->[$_task_id]->{_total_running} -= 1;
         }

         my $_total_running = ($_has_user_tasks)
            ? $self->{_task}->[$_task_id]->{_total_running}
            : $self->{_total_running};

         if ($_task_id == 0 && defined $_syn_flag && $_sync_cnt) {
            if ($_sync_cnt == $_total_running) {
               syswrite $_BSB_W_SOCK, $LF for (1 .. $_total_running);
               undef $_syn_flag;
            }
         }

         _task_end($self, $_task_id) unless ($_total_running);

         return;
      },

      ## ----------------------------------------------------------------------

      OUTPUT_W_EXT.$LF => sub {                   ## Worker has exited
         chomp($_task_id = <$_DAU_R_SOCK>);

         $self->{_total_exited}  += 1;
         $self->{_total_running} -= 1;
         $self->{_total_workers} -= 1;

         if ($_has_user_tasks && $_task_id >= 0) {
            $self->{_task}->[$_task_id]->{_total_running} -= 1;
            $self->{_task}->[$_task_id]->{_total_workers} -= 1;
         }

         my $_total_running = ($_has_user_tasks)
            ? $self->{_task}->[$_task_id]->{_total_running}
            : $self->{_total_running};

         if ($_task_id == 0 && defined $_syn_flag && $_sync_cnt) {
            if ($_sync_cnt == $_total_running) {
               syswrite $_BSB_W_SOCK, $LF for (1 .. $_total_running);
               undef $_syn_flag;
            }
         }

         my $_exit_msg = '';

         chomp($_exit_wid    = <$_DAU_R_SOCK>);
         chomp($_exit_pid    = <$_DAU_R_SOCK>);
         chomp($_exit_status = <$_DAU_R_SOCK>);
         chomp($_exit_id     = <$_DAU_R_SOCK>);
         chomp($_len         = <$_DAU_R_SOCK>);

         read($_DAU_R_SOCK, $_exit_msg, $_len) if ($_len);

         $self->{_wrk_status} = $_exit_status
            if (abs($_exit_status) > abs($self->{_wrk_status}));

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
            $self->{_exited_wid} = $_exit_wid;

            $_on_post_exit->($self, {
               wid => $_exit_wid, pid => $_exit_pid, status => $_exit_status,
               msg => $_exit_msg, id  => $_exit_id
            });

            delete $self->{_exited_wid};
         }
         elsif (defined $_on_post_run) {
            push @{ $self->{_status} }, {
               wid => $_exit_wid, pid => $_exit_pid, status => $_exit_status,
               msg => $_exit_msg, id  => $_exit_id
            };
         }

         _task_end($self, $_task_id) unless ($_total_running);

         return;
      },

      ## ----------------------------------------------------------------------

      OUTPUT_A_ARY.$LF => sub {                   ## Array << Array
         my $_buffer;

         if ($_offset_pos >= $_input_size || $_aborted) {
            local $\ = undef if (defined $\);
            print $_DAU_R_SOCK '0' . $LF;
            return;
         }

         if ($_single_dim && $_chunk_size == 1) {
            $_buffer = $_input_data->[$_offset_pos];
         }
         else {
            if ($_offset_pos + $_chunk_size - 1 < $_input_size) {
               $_buffer = $self->{freeze}( [ @$_input_data[
                  $_offset_pos .. $_offset_pos + $_chunk_size - 1
               ] ] );
            }
            else {
               $_buffer = $self->{freeze}( [ @$_input_data[
                  $_offset_pos .. $_input_size - 1
               ] ] );
            }
         }

         local $\ = undef if (defined $\); $_len = length $_buffer;
         print $_DAU_R_SOCK $_len . $LF . (++$_chunk_id) . $LF . $_buffer;

         $_offset_pos += $_chunk_size;

         return;
      },

      OUTPUT_S_GLB.$LF => sub {                   ## Scalar << Glob FH
         my $_buffer;

         ## The logic below honors ('Ctrl/Z' in Windows, 'Ctrl/D' in Unix)
         ## when reading from standard input. No output will be lost as
         ## far as what was previously read into the buffer.

         if ($_eof_flag || $_aborted) {
            local $\ = undef if (defined $\);
            print $_DAU_R_SOCK '0' . $LF;
            return;
         }

         {
            local $/ = $_RS if ($_RS_FLG);

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
                  } else {
                     $_eof_flag = 1;
                  }
               }
            }
         }

         local $\ = undef if (defined $\); $_len = length $_buffer;

         print $_DAU_R_SOCK ($_len)
            ? $_len . $LF . (++$_chunk_id) . $LF . $_buffer
            : '0' . $LF;

         return;
      },

      OUTPUT_U_ITR.$LF => sub {                   ## User << Iterator
         my $_buffer;

         if ($_aborted) {
            local $\ = undef if (defined $\);
            print $_DAU_R_SOCK '-1' . $LF;
            return;
         }

         if (my @_ret_a = $_input_data->($_chunk_size)) {
            if (@_ret_a > 1 || ref $_ret_a[0]) {
               $_buffer = $self->{freeze}( [ @_ret_a ] );
               local $\ = undef if (defined $\); $_len = length $_buffer;

               print $_DAU_R_SOCK $_len . '1' . $LF . (++$_chunk_id) . $LF .
                  $_buffer;

               return;
            }
            elsif (defined $_ret_a[0]) {
               local $\ = undef if (defined $\); $_len = length $_ret_a[0];

               print $_DAU_R_SOCK $_len . '0' . $LF . (++$_chunk_id) . $LF .
                  $_ret_a[0];

               return;
            }
         }

         local $\ = undef if (defined $\);
         print $_DAU_R_SOCK '-1' . $LF;
         $_aborted = 1;

         return;
      },

      ## ----------------------------------------------------------------------

      OUTPUT_A_CBK.$LF => sub {                   ## Callback w/ multiple args
         my $_buffer;

         chomp($_want_id  = <$_DAU_R_SOCK>);
         chomp($_callback = <$_DAU_R_SOCK>);
         chomp($_len      = <$_DAU_R_SOCK>);
         read $_DAU_R_SOCK, $_buffer, $_len;

         my $_data_ref = $self->{thaw}($_buffer);
         undef $_buffer;

         local $\ = $_O_SEP if ($_O_FLG); local $/ = $_I_SEP if ($_I_FLG);
         no strict 'refs';

         if ($_want_id == WANTS_UNDEF) {
            $_callback->(@{ $_data_ref });
         }
         elsif ($_want_id == WANTS_ARRAY) {
            my @_ret_a = $_callback->(@{ $_data_ref });
            $_buffer = $self->{freeze}(\@_ret_a);
            local $\ = undef if (defined $\); $_len = length $_buffer;
            print $_DAU_R_SOCK $_len . $LF . $_buffer;
         }
         else {
            my $_ret_s = $_callback->(@{ $_data_ref });
            unless (ref $_ret_s) {
               local $\ = undef if (defined $\);
               $_len = (defined $_ret_s) ? length $_ret_s : -1;
               print $_DAU_R_SOCK WANTS_SCALAR . $LF . $_len . $LF . $_ret_s;
            }
            else {
               $_buffer = $self->{freeze}($_ret_s);
               local $\ = undef if (defined $\); $_len = length $_buffer;
               print $_DAU_R_SOCK WANTS_REF . $LF . $_len . $LF . $_buffer;
            }
         }

         return;
      },

      OUTPUT_S_CBK.$LF => sub {                   ## Callback w/ 1 scalar arg
         my $_buffer;

         chomp($_want_id  = <$_DAU_R_SOCK>);
         chomp($_callback = <$_DAU_R_SOCK>);
         chomp($_len      = <$_DAU_R_SOCK>);
         read $_DAU_R_SOCK, $_buffer, $_len;

         local $\ = $_O_SEP if ($_O_FLG); local $/ = $_I_SEP if ($_I_FLG);
         no strict 'refs';

         if ($_want_id == WANTS_UNDEF) {
            $_callback->($_buffer);
         }
         elsif ($_want_id == WANTS_ARRAY) {
            my @_ret_a = $_callback->($_buffer);
            $_buffer = $self->{freeze}(\@_ret_a);
            local $\ = undef if (defined $\); $_len = length $_buffer;
            print $_DAU_R_SOCK $_len . $LF . $_buffer;
         }
         else {
            my $_ret_s = $_callback->($_buffer);
            unless (ref $_ret_s) {
               local $\ = undef if (defined $\);
               $_len = (defined $_ret_s) ? length $_ret_s : -1;
               print $_DAU_R_SOCK WANTS_SCALAR . $LF . $_len . $LF . $_ret_s;
            }
            else {
               $_buffer = $self->{freeze}($_ret_s);
               local $\ = undef if (defined $\); $_len = length $_buffer;
               print $_DAU_R_SOCK WANTS_REF . $LF . $_len . $LF . $_buffer;
            }
         }

         return;
      },

      OUTPUT_N_CBK.$LF => sub {                   ## Callback w/ no args
         my $_buffer;

         chomp($_want_id  = <$_DAU_R_SOCK>);
         chomp($_callback = <$_DAU_R_SOCK>);

         local $\ = $_O_SEP if ($_O_FLG); local $/ = $_I_SEP if ($_I_FLG);
         no strict 'refs';

         if ($_want_id == WANTS_UNDEF) {
            $_callback->();
         }
         elsif ($_want_id == WANTS_ARRAY) {
            my @_ret_a = $_callback->();
            $_buffer = $self->{freeze}(\@_ret_a);
            local $\ = undef if (defined $\); $_len = length $_buffer;
            print $_DAU_R_SOCK $_len . $LF . $_buffer;
         }
         else {
            my $_ret_s = $_callback->();
            unless (ref $_ret_s) {
               local $\ = undef if (defined $\);
               $_len = (defined $_ret_s) ? length $_ret_s : -1;
               print $_DAU_R_SOCK WANTS_SCALAR . $LF . $_len . $LF . $_ret_s;
            }
            else {
               $_buffer = $self->{freeze}($_ret_s);
               local $\ = undef if (defined $\); $_len = length $_buffer;
               print $_DAU_R_SOCK WANTS_REF . $LF . $_len . $LF . $_buffer;
            }
         }

         return;
      },

      ## ----------------------------------------------------------------------

      OUTPUT_A_GTR.$LF => sub {                   ## Gather w/ multiple args
         my $_buffer;

         chomp($_task_id = <$_DAU_R_SOCK>);
         chomp($_len     = <$_DAU_R_SOCK>);
         read $_DAU_R_SOCK, $_buffer, $_len;

         if ($_is_c_ref[$_task_id]) {
            local $_ = $self->{thaw}($_buffer);
            $_gather[$_task_id]->(@{ $_ });
         }
         elsif ($_is_h_ref[$_task_id]) {
            local $_ = $self->{thaw}($_buffer);
            while (1) {
               my $_key = shift @{ $_ }; my $_val = shift @{ $_ };
               $_gather[$_task_id]->{$_key} = $_val;
               last unless (@{ $_ });
            }
         }
         elsif ($_is_q_ref[$_task_id]) {
            $_gather[$_task_id]->enqueue( @{ $self->{thaw}($_buffer) } );
         }
         else {
            push @{ $_gather[$_task_id] }, @{ $self->{thaw}($_buffer) };
         }

         return;
      },

      OUTPUT_R_GTR.$LF => sub {                   ## Gather w/ 1 reference arg
         my $_buffer;

         chomp($_task_id = <$_DAU_R_SOCK>);
         chomp($_len     = <$_DAU_R_SOCK>);
         read $_DAU_R_SOCK, $_buffer, $_len;

         if ($_is_c_ref[$_task_id]) {
            local $_ = $self->{thaw}($_buffer);
            $_gather[$_task_id]->($_);
         }
         elsif ($_is_h_ref[$_task_id]) {
            local $_ = $self->{thaw}($_buffer);
            $_gather[$_task_id]->{$_} = undef;
         }
         elsif ($_is_q_ref[$_task_id]) {
            $_gather[$_task_id]->enqueue( $self->{thaw}($_buffer) );
         }
         else {
            push @{ $_gather[$_task_id] }, $self->{thaw}($_buffer);
         }

         return;
      },

      OUTPUT_S_GTR.$LF => sub {                   ## Gather w/ 1 scalar arg
         my $_buffer;

         chomp($_task_id = <$_DAU_R_SOCK>);
         chomp($_len     = <$_DAU_R_SOCK>);
         read $_DAU_R_SOCK, $_buffer, $_len if ($_len >= 0);

         if ($_is_c_ref[$_task_id]) {
            local $_ = $_buffer;
            $_gather[$_task_id]->($_);
         }
         elsif ($_is_h_ref[$_task_id]) {
            $_gather[$_task_id]->{$_buffer} = undef;
         }
         elsif ($_is_q_ref[$_task_id]) {
            $_gather[$_task_id]->enqueue( $_buffer );
         }
         else {
            push @{ $_gather[$_task_id] }, $_buffer;
         }

         return;
      },

      ## ----------------------------------------------------------------------

      OUTPUT_O_SND.$LF => sub {                   ## Send >> STDOUT
         my $_buffer;

         chomp($_len = <$_DAU_R_SOCK>);
         read $_DAU_R_SOCK, $_buffer, $_len;

         if (defined $_user_output) {
            $_user_output->($_buffer);
         } else {
            print $_MCE_STDOUT $_buffer;
         }

         return;
      },

      OUTPUT_E_SND.$LF => sub {                   ## Send >> STDERR
         my $_buffer;

         chomp($_len = <$_DAU_R_SOCK>);
         read $_DAU_R_SOCK, $_buffer, $_len;

         if (defined $_user_error) {
            $_user_error->($_buffer);
         } else {
            print $_MCE_STDERR $_buffer;
         }

         return;
      },

      OUTPUT_F_SND.$LF => sub {                   ## Send >> File
         my ($_buffer, $_OUT_FILE);

         chomp($_file = <$_DAU_R_SOCK>);
         chomp($_len  = <$_DAU_R_SOCK>);
         read $_DAU_R_SOCK, $_buffer, $_len;

         unless (exists $_sendto_fhs{$_file}) {
            open $_sendto_fhs{$_file}, '>>', $_file
               or _croak "Cannot open file for writing ($_file): $!";

            binmode $_sendto_fhs{$_file};

            ## Select new FH, turn on autoflush, restore the old FH.
            select((select($_sendto_fhs{$_file}), $| = 1)[0])
               if ($_flush_file);
         }

         $_OUT_FILE = $_sendto_fhs{$_file};
         print $_OUT_FILE $_buffer;

         return;
      },

      OUTPUT_D_SND.$LF => sub {                   ## Send >> File descriptor
         my ($_buffer, $_OUT_FILE);

         chomp($_fd  = <$_DAU_R_SOCK>);
         chomp($_len = <$_DAU_R_SOCK>);
         read $_DAU_R_SOCK, $_buffer, $_len;

         unless (exists $_sendto_fhs{$_fd}) {
            require IO::Handle unless (defined $IO::Handle::VERSION);

            $_sendto_fhs{$_fd} = IO::Handle->new();
            $_sendto_fhs{$_fd}->fdopen($_fd, 'w')
               or _croak "Cannot open file descriptor ($_fd): $!";

            binmode $_sendto_fhs{$_fd};

            ## Select new FH, turn on autoflush, restore the old FH.
            select((select($_sendto_fhs{$_fd}), $| = 1)[0])
               if ($_flush_file);
         }

         $_OUT_FILE = $_sendto_fhs{$_fd};
         print $_OUT_FILE $_buffer;

         return;
      },

      ## ----------------------------------------------------------------------

      OUTPUT_B_SYN.$LF => sub {                   ## Barrier sync - begin

         if (!defined $_sync_cnt || $_sync_cnt == 0) {
            $_syn_flag = 1;
            $_sync_cnt = 0;
         }

         my $_total_running = ($_has_user_tasks)
            ? $self->{_task}->[0]->{_total_running}
            : $self->{_total_running};

         if (++$_sync_cnt == $_total_running) {
            syswrite $_BSB_W_SOCK, $LF for (1 .. $_total_running);
            undef $_syn_flag;
         }

         return;
      },

      OUTPUT_E_SYN.$LF => sub {                   ## Barrier sync - end

         if (--$_sync_cnt == 0) {
            my $_total_running = ($_has_user_tasks)
               ? $self->{_task}->[0]->{_total_running}
               : $self->{_total_running};

            syswrite $_BSE_W_SOCK, $LF for (1 .. $_total_running);
         }

         return;
      }

   );

   ## -------------------------------------------------------------------------

   $_has_user_tasks = (defined $self->{user_tasks});
   $_aborted = $_chunk_id = $_eof_flag = 0;

   $_on_post_exit = $self->{on_post_exit};
   $_on_post_run  = $self->{on_post_run};
   $_chunk_size   = $self->{chunk_size};
   $_flush_file   = $self->{flush_file};
   $_user_output  = $self->{user_output};
   $_user_error   = $self->{user_error};
   $_single_dim   = $self->{_single_dim};
   $_sess_dir     = $self->{_sess_dir};

   $_chunk_size = $self->{user_tasks}->[0]->{chunk_size}
      if ($_has_user_tasks && $self->{user_tasks}->[0]->{chunk_size});

   if ($_has_user_tasks) {
      foreach (0 .. @{ $self->{user_tasks} } - 1) {
         $_gather[$_] = (defined $self->{user_tasks}->[$_]->{gather})
            ? $self->{user_tasks}->[$_]->{gather} : $self->{gather};

         $_is_c_ref[$_] = ( ref $_gather[$_] eq 'CODE' ) ? 1 : 0;
         $_is_h_ref[$_] = ( ref $_gather[$_] eq 'HASH' ) ? 1 : 0;

         $_is_q_ref[$_] = (
            ref $_gather[$_] eq 'MCE::Queue' ||
            ref $_gather[$_] eq 'Thread::Queue' ) ? 1 : 0;
      }
   }

   if (defined $self->{gather}) {
      $_gather[0] = $self->{gather};

      $_is_c_ref[0] = ( ref $_gather[0] eq 'CODE' ) ? 1 : 0;
      $_is_h_ref[0] = ( ref $_gather[0] eq 'HASH' ) ? 1 : 0;

      $_is_q_ref[0] = (
         ref $_gather[0] eq 'MCE::Queue' ||
         ref $_gather[0] eq 'Thread::Queue' ) ? 1 : 0;
   }

   if (defined $_input_data && ref $_input_data eq 'ARRAY') {
      $_input_size = @$_input_data;
      $_offset_pos = 0;
   } else {
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

   ## -------------------------------------------------------------------------
   ## Output event loop.

   my ($_func, $_chn); my $_channels = $self->{_dat_r_sock};

   $_BSB_W_SOCK = $self->{_bsb_w_sock};        ## For IPC
   $_BSE_W_SOCK = $self->{_bse_w_sock};
   $_DAT_R_SOCK = $self->{_dat_r_sock}->[0];

   $_RS    = $self->{RS} || $/;    $_RS_FLG = (!$_RS || $_RS ne $LF);
   $_O_SEP = $\; local $\ = undef; $_O_FLG  = defined $_O_SEP;
   $_I_SEP = $/; local $/ = $LF;   $_I_FLG  = (!$_I_SEP || $_I_SEP ne $LF);

   ## Call module's loop_begin routine for modules plugged into MCE.
   $_->($self, \$_DAU_R_SOCK) for (@{ $_plugin_loop_begin });

   ## Call on hash function. Exit loop when workers have completed.
   local $!;

   while (1) {
      $_func = <$_DAT_R_SOCK>;
      next unless (defined $_func);

      $_DAU_R_SOCK = $_channels->[ <$_DAT_R_SOCK> ];

      if (exists $_core_output_function{$_func}) {
         $_core_output_function{$_func}();
      }
      elsif (exists $_plugin_function->{$_func}) {
         $_plugin_function->{$_func}();
      }

      last unless ($self->{_total_running});
   }

   ## Call module's loop_end routine for modules plugged into MCE.
   $_->($self) for (@{ $_plugin_loop_end });

   ## Call on_post_run callback.
   $_on_post_run->($self, $self->{_status})
      if (defined $_on_post_run);

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

1;

