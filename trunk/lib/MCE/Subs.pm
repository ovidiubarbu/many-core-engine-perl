###############################################################################
## ----------------------------------------------------------------------------
## MCE::Subs
## -- Imports funtions mapped directly to Many-Core Engine methods.
##
###############################################################################

package MCE::Subs;

use strict;
use warnings;

use MCE 1.499;

our $VERSION = '1.499_001'; $VERSION = eval $VERSION;

###############################################################################
## ----------------------------------------------------------------------------
## Import routine.
##
###############################################################################

my $_loaded;

sub import {

   my $class = shift;
   return if ($_loaded++);

   my $_flg = sub { 1 }; my $_m_flg = 0; my $_w_flg = 0;
   my $_package = caller();

   ## Process module arguments.
   while (my $_arg = shift) {
      $_m_flg = $_flg->() and next if ( $_arg =~ /^manager$/i );
      $_w_flg = $_flg->() and next if ( $_arg =~ /^worker$/i );

      _croak("MCE::import: '$_arg' is not a valid module argument");
   }

   $_m_flg = $_w_flg = 1 if ($_m_flg == 0 && $_w_flg == 0);
   _import_subs($_package, $_m_flg, $_w_flg);

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

sub mce_forchunk (@) { return $MCE::MCE->forchunk(@_); }
sub mce_foreach  (@) { return $MCE::MCE->foreach(@_);  }
sub mce_forseq   (@) { return $MCE::MCE->forseq(@_); }
sub mce_process  (@) { return $MCE::MCE->process(@_); }
sub mce_run      (@) { return $MCE::MCE->run(@_); }
sub mce_send     (@) { return $MCE::MCE->send(@_); }
sub mce_shutdown ( ) { return $MCE::MCE->shutdown(); }
sub mce_spawn    ( ) { return $MCE::MCE->spawn(); }

## Callable by the worker process only.

sub mce_do       (@) { return $MCE::MCE->do(@_); }
sub mce_exit     (@) { return $MCE::MCE->exit(@_); }
sub mce_gather   (@) { return $MCE::MCE->gather(@_); }
sub mce_last     ( ) { return $MCE::MCE->last(); }
sub mce_next     ( ) { return $MCE::MCE->next(); }
sub mce_sendto   (@) { return $MCE::MCE->sendto(@_); }
sub mce_sync     ( ) { return $MCE::MCE->sync(); }
sub mce_yield    ( ) { return $MCE::MCE->yield(); }

## Callable by both the manager and worker processes.

sub mce_abort    ( ) { return $MCE::MCE->abort(); }
sub mce_freeze   (@) { return $MCE::MCE->{freeze}(@_); }
sub mce_print  (;*@) { return $MCE::MCE->print(@_); }
sub mce_printf (;*@) { return $MCE::MCE->printf(@_); }
sub mce_say    (;*@) { return $MCE::MCE->say(@_); }
sub mce_thaw     (@) { return $MCE::MCE->{thaw}(@_); }

###############################################################################
## ----------------------------------------------------------------------------
## Private methods.
##
###############################################################################

sub _croak {

   goto &MCE::_croak;

   return;
}

sub _import_subs {

   my ($_package, $_m_flg, $_w_flg) = @_;

   no strict 'refs'; no warnings 'redefine';

   ## Callable by the manager process only.

   if ($_m_flg) {
      *{ $_package . '::mce_restart_worker' } = \&mce_restart_worker;

      *{ $_package . '::mce_forchunk' } = \&mce_forchunk;
      *{ $_package . '::mce_foreach'  } = \&mce_foreach;
      *{ $_package . '::mce_forseq'   } = \&mce_forseq;
      *{ $_package . '::mce_process'  } = \&mce_process;
      *{ $_package . '::mce_run'      } = \&mce_run;
      *{ $_package . '::mce_send'     } = \&mce_send;
      *{ $_package . '::mce_shutdown' } = \&mce_shutdown;
      *{ $_package . '::mce_spawn'    } = \&mce_spawn;
   }

   ## Callable by the worker process only.

   if ($_w_flg) {
      *{ $_package . '::mce_do'       } = \&mce_do;
      *{ $_package . '::mce_exit'     } = \&mce_exit;
      *{ $_package . '::mce_gather'   } = \&mce_gather;
      *{ $_package . '::mce_last'     } = \&mce_last;
      *{ $_package . '::mce_next'     } = \&mce_next;
      *{ $_package . '::mce_sendto'   } = \&mce_sendto;
      *{ $_package . '::mce_sync'     } = \&mce_sync;
      *{ $_package . '::mce_yield'    } = \&mce_yield;
   }

   ## Callable by both the manager and worker processes.

   if ($_m_flg || $_w_flg) {
      *{ $_package . '::mce_abort'    } = \&mce_abort;
      *{ $_package . '::mce_freeze'   } = \&mce_freeze;
      *{ $_package . '::mce_print'    } = \&mce_print;
      *{ $_package . '::mce_printf'   } = \&mce_printf;
      *{ $_package . '::mce_say'      } = \&mce_say;
      *{ $_package . '::mce_thaw'     } = \&mce_thaw;
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

MCE::Subs - Imports funtions mapped directly to Many-Core Engine methods.

=head1 VERSION

This document describes MCE::Subs version 1.499_001

=head1 SYNOPSIS

   use MCE::Subs;              ## Imports functions for manager and workers

   use MCE::Subs qw(manager);  ## Imports functions for manager only
   use MCE::Subs qw(worker);   ## Imports functions for workers only

=head1 DESCRIPTION

This module imports functions which are mapped to MCE methods. All imported
functions are prototyped, therefore allowing one to call them without using
parenthesis.

   use MCE::Subs;

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

For the next example, we only want the worker functions to be imported due
to using MCE::Map, which takes care of creating a MCE instance and running.

   use MCE::Map;
   use MCE::Subs qw(worker);

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

=head1 FUNCTIONS for the MANAGER PROCESS

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

=item mce_thaw

=item mce_spawn

=back

=head1 FUNCTIONS for WORKERS

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

=head1 SEE ALSO

L<MCE>, L<MCE::Flow>, L<MCE::Grep>, L<MCE::Loop>, L<MCE::Map>,
L<MCE::Queue>, L<MCE::Signal>, L<MCE::Stream>, L<MCE::Util>

=head1 AUTHOR

Mario E. Roy, S<E<lt>marioeroy AT gmail DOT comE<gt>>

=head1 LICENSE

This program is free software; you can redistribute it and/or modify it
under the terms of either: the GNU General Public License as published
by the Free Software Foundation; or the Artistic License.

See L<http://dev.perl.org/licenses/> for more information.

=cut

