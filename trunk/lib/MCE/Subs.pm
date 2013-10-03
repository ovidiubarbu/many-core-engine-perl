###############################################################################
## ----------------------------------------------------------------------------
## MCE::Subs - Exports funtions mapped directly to MCE's methods.
##
###############################################################################

package MCE::Subs;

use strict;
use warnings;

use MCE;

our $VERSION = '1.499_003'; $VERSION = eval $VERSION;

###############################################################################
## ----------------------------------------------------------------------------
## Import routine.
##
###############################################################################

my $_loaded;

sub import {

   my $class = shift;
   return if ($_loaded++);

   my $_g_flg = 0; my $_m_flg = 0; my $_w_flg = 0;
   my $_flag = sub { 1 }; my $_package = caller();

   ## Process module arguments.
   while (my $_arg = shift) {
      $_g_flg = $_flag->() and next if ( $_arg =~ /^:getter$/i );
      $_m_flg = $_flag->() and next if ( $_arg =~ /^:manager$/i );
      $_w_flg = $_flag->() and next if ( $_arg =~ /^:worker$/i );

      _croak("MCE::Subs::import: '$_arg' is not a valid module argument");
   }

   $_m_flg = $_w_flg = 1 if ($_m_flg + $_w_flg == 0);

   _export_subs($_package, $_g_flg, $_m_flg, $_w_flg);

   return;
}

###############################################################################
## ----------------------------------------------------------------------------
## Define functions.
##
###############################################################################

## Callable by the manager process only.

sub mce_restart_worker (@) {
   return $MCE::MCE->restart_worker(@_);
}

sub mce_forchunk    (@) { return $MCE::MCE->forchunk(@_); }
sub mce_foreach     (@) { return $MCE::MCE->foreach(@_);  }
sub mce_forseq      (@) { return $MCE::MCE->forseq(@_); }
sub mce_process     (@) { return $MCE::MCE->process(@_); }
sub mce_run         (@) { return $MCE::MCE->run(@_); }
sub mce_send        (@) { return $MCE::MCE->send(@_); }
sub mce_shutdown    ( ) { return $MCE::MCE->shutdown(); }
sub mce_spawn       ( ) { return $MCE::MCE->spawn(); }
sub mce_status      ( ) { return $MCE::MCE->status(); }

## Callable by the worker process only.

sub mce_do          (@) { return $MCE::MCE->do(@_); }
sub mce_exit        (@) { return $MCE::MCE->exit(@_); }
sub mce_gather      (@) { return $MCE::MCE->gather(@_); }
sub mce_last        ( ) { return $MCE::MCE->last(); }
sub mce_next        ( ) { return $MCE::MCE->next(); }
sub mce_sendto    (;*@) { return $MCE::MCE->sendto(@_); }
sub mce_sync        ( ) { return $MCE::MCE->sync(); }
sub mce_yield       ( ) { return $MCE::MCE->yield(); }

## Callable by both the manager and worker processes.

sub mce_abort       ( ) { return $MCE::MCE->abort(); }
sub mce_freeze      (@) { return $MCE::MCE->{freeze}(@_); }
sub mce_print     (;*@) { return $MCE::MCE->print(@_); }
sub mce_printf    (;*@) { return $MCE::MCE->printf(@_); }
sub mce_say       (;*@) { return $MCE::MCE->say(@_); }
sub mce_thaw        (@) { return $MCE::MCE->{thaw}(@_); }

## Callable by both the manager and worker processes.

sub mce_chunk_id    ( ) { return $MCE::MCE->chunk_id(); }
sub mce_chunk_size  ( ) { return $MCE::MCE->chunk_size(); }
sub mce_max_workers ( ) { return $MCE::MCE->max_workers(); }
sub mce_sess_dir    ( ) { return $MCE::MCE->sess_dir(); }
sub mce_task_id     ( ) { return $MCE::MCE->task_id(); }
sub mce_task_name   ( ) { return $MCE::MCE->task_name(); }
sub mce_task_wid    ( ) { return $MCE::MCE->task_wid(); }
sub mce_tmp_dir     ( ) { return $MCE::MCE->tmp_dir(); }
sub mce_user_args   ( ) { return $MCE::MCE->user_args(); }
sub mce_wid         ( ) { return $MCE::MCE->wid(); }

###############################################################################
## ----------------------------------------------------------------------------
## Private methods.
##
###############################################################################

sub _croak {

   goto &MCE::_croak;

   return;
}

sub _export_subs {

   my ($_package, $_g_flg, $_m_flg, $_w_flg) = @_;

   no strict 'refs'; no warnings 'redefine';

   ## Callable by the manager process only.

   if ($_m_flg) {
      *{ $_package . '::mce_restart_worker' } = \&mce_restart_worker;

      *{ $_package . '::mce_forchunk'    } = \&mce_forchunk;
      *{ $_package . '::mce_foreach'     } = \&mce_foreach;
      *{ $_package . '::mce_forseq'      } = \&mce_forseq;
      *{ $_package . '::mce_process'     } = \&mce_process;
      *{ $_package . '::mce_run'         } = \&mce_run;
      *{ $_package . '::mce_send'        } = \&mce_send;
      *{ $_package . '::mce_shutdown'    } = \&mce_shutdown;
      *{ $_package . '::mce_spawn'       } = \&mce_spawn;
      *{ $_package . '::mce_status'      } = \&mce_status;
   }

   ## Callable by the worker process only.

   if ($_w_flg) {
      *{ $_package . '::mce_do'          } = \&mce_do;
      *{ $_package . '::mce_exit'        } = \&mce_exit;
      *{ $_package . '::mce_gather'      } = \&mce_gather;
      *{ $_package . '::mce_last'        } = \&mce_last;
      *{ $_package . '::mce_next'        } = \&mce_next;
      *{ $_package . '::mce_sendto'      } = \&mce_sendto;
      *{ $_package . '::mce_sync'        } = \&mce_sync;
      *{ $_package . '::mce_yield'       } = \&mce_yield;
   }

   ## Callable by both the manager and worker processes.

   if ($_m_flg || $_w_flg) {
      *{ $_package . '::mce_abort'       } = \&mce_abort;
      *{ $_package . '::mce_freeze'      } = \&mce_freeze;
      *{ $_package . '::mce_print'       } = \&mce_print;
      *{ $_package . '::mce_printf'      } = \&mce_printf;
      *{ $_package . '::mce_say'         } = \&mce_say;
      *{ $_package . '::mce_thaw'        } = \&mce_thaw;
   }

   ## Callable by both the manager and worker processes.

   if ($_g_flg) {
      *{ $_package . '::mce_chunk_id'    } = \&mce_chunk_id;
      *{ $_package . '::mce_chunk_size'  } = \&mce_chunk_size;
      *{ $_package . '::mce_max_workers' } = \&mce_max_workers;
      *{ $_package . '::mce_sess_dir'    } = \&mce_sess_dir;
      *{ $_package . '::mce_task_id'     } = \&mce_task_id;
      *{ $_package . '::mce_task_name'   } = \&mce_task_name;
      *{ $_package . '::mce_task_wid'    } = \&mce_task_wid;
      *{ $_package . '::mce_tmp_dir'     } = \&mce_tmp_dir;
      *{ $_package . '::mce_user_args'   } = \&mce_user_args;
      *{ $_package . '::mce_wid'         } = \&mce_wid;
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

MCE::Subs - Exports funtions mapped directly to MCE's methods

=head1 VERSION

This document describes MCE::Subs version 1.499_003

=head1 SYNOPSIS

   use MCE::Subs;  ## Exports manager and worker functions only
                   ## Getter functions are not exported by default

   use MCE::Subs qw( :getter  );  ## All, including getter functions
   use MCE::Subs qw( :manager );  ## Exports manager functions only
   use MCE::Subs qw( :worker  );  ## Exports worker functions only

   use MCE::Subs qw( :getter :worker );  ## Excludes manager functions

=head1 DESCRIPTION

This module exports functions mapped to MCE methods. All exported functions
are prototyped, therefore allowing one to call them without using parentheses.

   use MCE::Subs qw( :worker );

   ## barrier synchronization among workers

   sub user_func {
      my $wid = MCE->wid;

      mce_say "A: $wid";
      mce_sync;

      mce_say "B: $wid";
      mce_sync;

      mce_say "C: $wid";
      mce_sync;

      return;
   }

   MCE->new(
      max_workers => 24, user_func => \&user_func
   );

   mce_run 0 for (1..100);   ## 0 means do not shutdown after running

For the next example, we only want the worker functions to be exported due
to using MCE::Map, which takes care of creating a MCE instance and running.

   use MCE::Map;
   use MCE::Subs qw( :worker );

   ## The following will serialize output to STDOUT as well as gather
   ## to @a. mce_say displays $_ when called without arguments.

   my @a = mce_map { mce_say; $_ } 1 .. 100;

   print scalar @a, "\n";

Unlike the native Perl functions, print, printf, and say methods require the
comma after the filehandle.

   MCE->print("STDERR", $error_msg, "\n");  ## Requires quotes around
   MCE->say("STDERR", $error_msg);          ## the bareword FH.
   MCE->say($fh, $error_msg);

   mce_print STDERR, $error_msg, "\n";      ## Quotes can be ommitted
   mce_say STDERR, $error_msg;              ## around the bareword FH.
   mce_say $fh, $error_msg;

=head1 FUNCTIONS for the MANAGER PROCESS via ( :manager )

=over

=item mce_abort

=item mce_forchunk

=item mce_foreach

=item mce_forseq

=item mce_freeze

=item mce_process

=item mce_restart_worker

=item mce_run

=item mce_print

=item mce_printf

=item mce_say

=item mce_send

=item mce_shutdown

=item mce_spawn

=item mce_status

=item mce_thaw

=back

=head1 FUNCTIONS for MCE WORKERS via ( :worker )

=over

=item mce_abort

=item mce_do

=item mce_exit

=item mce_freeze

=item mce_gather

=item mce_last

=item mce_next

=item mce_print

=item mce_printf

=item mce_say

=item mce_sendto

=item mce_sync

=item mce_thaw

=item mce_yield

=back

=head1 GETTERS for MCE ATTRIBUTES via ( :getter )

=over

=item mce_chunk_id

=item mce_chunk_size

=item mce_max_workers

=item mce_sess_dir

=item mce_task_id

=item mce_task_name

=item mce_task_wid

=item mce_tmp_dir

=item mce_user_args

=item mce_wid

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

