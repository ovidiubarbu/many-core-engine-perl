###############################################################################
## ----------------------------------------------------------------------------
## MCE::Subs
## -- Imports subroutines wrapping around Many-Core Engine's methods.
##
###############################################################################

package MCE::Subs;

use strict;
use warnings;

use MCE 1.499;

our $VERSION = '1.499_001'; $VERSION = eval $VERSION;

###############################################################################
## ----------------------------------------------------------------------------
## Define subroutines wrapping around MCE's methods.
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
sub mce_sendto   (@) { return $MCE::MCE->sendto(@_); }
sub mce_last     ( ) { return $MCE::MCE->last(); }
sub mce_next     ( ) { return $MCE::MCE->next(); }
sub mce_sync     ( ) { return $MCE::MCE->sync(); }
sub mce_yield    ( ) { return $MCE::MCE->yield(); }

## Callable by both the manager and worker processes.

sub mce_print  (;*@) { return $MCE::MCE->print(@_); }
sub mce_printf (;*@) { return $MCE::MCE->printf(@_); }
sub mce_say    (;*@) { return $MCE::MCE->say(@_); }
sub mce_freeze   (@) { return $MCE::MCE->{freeze}(@_); }
sub mce_thaw     (@) { return $MCE::MCE->{thaw}(@_); }
sub mce_abort    ( ) { return $MCE::MCE->abort(); }

###############################################################################
## ----------------------------------------------------------------------------
## Import routine.
##
###############################################################################

my $_loaded;

sub import {

   my $class = shift;
   return if ($_loaded++);

   my $_package = caller();

   ## Process module arguments.
   while (my $_arg = shift) {

      $MCE::MAX_WORKERS = shift and next if ( $_arg =~ /^max_workers$/i );
      $MCE::CHUNK_SIZE  = shift and next if ( $_arg =~ /^chunk_size$/i );
      $MCE::TMP_DIR     = shift and next if ( $_arg =~ /^tmp_dir$/i );
      $MCE::FREEZE      = shift and next if ( $_arg =~ /^freeze$/i );
      $MCE::THAW        = shift and next if ( $_arg =~ /^thaw$/i );

      if ( $_arg =~ /^sereal$/i ) {
         if (shift) {
            local $@; eval 'use Sereal qw(encode_sereal decode_sereal)';
            unless ($@) {
               $MCE::FREEZE = \&encode_sereal;
               $MCE::THAW   = \&decode_sereal;
            }
         }
         next;
      }

      ## MCE 1.4 supported EXPORT_CONST and CONST as arguments.
      ## Oops! This should have been named IMPORT_CONSTS instead.

      if ( $_arg =~ /^(?:import_consts|export_const|const)$/i ) {
         _import_consts($_package) if (shift eq '1');
         next;
      }

      _croak("MCE::import: '$_arg' is not a valid module argument");
   }

   _import_subs($_package);

   return;
}

###############################################################################
## ----------------------------------------------------------------------------
## Private methods.
##
###############################################################################

sub _croak {

   goto &MCE::_croak;

   return;
}

sub _import_consts {

   my $_package = shift;

   no strict 'refs'; no warnings 'redefine';

   *{ $_package . '::SELF'  } = \&MCE::SELF;
   *{ $_package . '::CHUNK' } = \&MCE::CHUNK;
   *{ $_package . '::CID'   } = \&MCE::CID;

   return;
}

sub _import_subs {

   my $_package = shift;

   no strict 'refs'; no warnings 'redefine';

   ## Callable by the manager process only.

   *{ $_package . '::mce_restart_worker' } = \&mce_restart_worker;

   *{ $_package . '::mce_forchunk' } = \&mce_forchunk;
   *{ $_package . '::mce_foreach'  } = \&mce_foreach;
   *{ $_package . '::mce_forseq'   } = \&mce_forseq;
   *{ $_package . '::mce_process'  } = \&mce_process;
   *{ $_package . '::mce_run'      } = \&mce_run;
   *{ $_package . '::mce_send'     } = \&mce_send;
   *{ $_package . '::mce_shutdown' } = \&mce_shutdown;
   *{ $_package . '::mce_spawn'    } = \&mce_spawn;

   ## Callable by the worker process only.

   *{ $_package . '::mce_do'       } = \&mce_do;
   *{ $_package . '::mce_exit'     } = \&mce_exit;
   *{ $_package . '::mce_gather'   } = \&mce_gather;
   *{ $_package . '::mce_sendto'   } = \&mce_sendto;
   *{ $_package . '::mce_last'     } = \&mce_last;
   *{ $_package . '::mce_next'     } = \&mce_next;
   *{ $_package . '::mce_sync'     } = \&mce_sync;
   *{ $_package . '::mce_yield'    } = \&mce_yield;

   ## Callable by both the manager and worker processes.

   *{ $_package . '::mce_freeze'   } = \&mce_freeze;
   *{ $_package . '::mce_thaw'     } = \&mce_thaw;
   *{ $_package . '::mce_print'    } = \&mce_print;
   *{ $_package . '::mce_printf'   } = \&mce_printf;
   *{ $_package . '::mce_say'      } = \&mce_say;
   *{ $_package . '::mce_abort'    } = \&mce_abort;

   return;
}

1;

__END__

=head1 NAME

MCE::Subs - Imports subroutines wrapping around Many-Core Engine's methods.

=head1 VERSION

This document describes MCE::Subs version 1.499_001

=head1 SYNOPSIS

   use MCE::Subs;

=head1 DESCRIPTION

This will import the functions listed below into the calling script. The
functions are prototyped, therefore allowing one to call these without using
parenthesis.

   use MCE::Subs;

   sub user_func {

      my $wid = MCE->wid;

      mce_say "a: $wid";
      mce_sync;

      mce_say "b: $wid";
      mce_sync;

      mce_say "c: $wid";
      mce_sync;

      return;
   }

   MCE->new( max_workers => 24, user_func => \&user_func );

   mce_run 0 for (1..100);

Note that print, printf, and say functions require the comma after the
bareword filehandle.

   mce_print STDERR, $error_msg, "\n";
   mce_say STDERR, $error_msg;

Plese see L<MCE> for explanation of the functions listed below.

=head1 FUNCTIONS for MANAGER PROCESS only

=over

=item mce_restart_worker

=item mce_forchunk

=item mce_foreach

=item mce_forseq

=item mce_process

=item mce_run

=item mce_send

=item mce_shutdown

=item mce_spawn

=back

=head1 FUNCTIONS for WORKERS only

=over

=item mce_do

=item mce_exit

=item mce_gather

=item mce_sendto

=item mce_last

=item mce_next

=item mce_sync

=item mce_yield

=back

=head1 FUNCTIONS for MANAGER PROCESS and WORKERS

=over

=item mce_print

=item mce_printf

=item mce_say

=item mce_freeze

=item mce_thaw

=item mce_abort

=back

=head1 SEE ALSO

L<MCE>, L<MCE::Queue>, L<MCE::Signal>, L<MCE::Util>

=head1 AUTHOR

Mario E. Roy, S<E<lt>marioeroy AT gmail DOT comE<gt>>

=head1 LICENSE

This program is free software; you can redistribute it and/or modify it
under the terms of either: the GNU General Public License as published
by the Free Software Foundation; or the Artistic License.

See L<http://dev.perl.org/licenses/> for more information.

=cut

