###############################################################################
## ----------------------------------------------------------------------------
## MCE::Signal
## -- Provides tmp_dir creation & signal handling for Many-Core Engine.
##
## First modular release in a Perl package format
## -- Created by Mario Roy, 11/05/2012
##
###############################################################################

package MCE::Signal;

our ($_has_threads, $_main_proc_id);
my $_prog_name;

BEGIN {
   $_prog_name = $0;
   $_prog_name =~ s{^.*[\\/]}{}g;
   $_main_proc_id = $$;
}

use strict;
use warnings;

our $VERSION = '1.003';
$VERSION = eval $VERSION;

use Fcntl qw( :flock );
use File::Path qw( rmtree );
use base 'Exporter';

use constant { ILLEGAL_STATE => 'ILLEGAL_STATE' };

our $tmp_dir = undef;
our @EXPORT_OK = qw( $tmp_dir sys_cmd stop_and_exit );
our %EXPORT_TAGS = (
   all     => \@EXPORT_OK,
   tmp_dir => [ qw( $tmp_dir ) ]
);

###############################################################################
## ----------------------------------------------------------------------------
## Process import, export, & module arguments.
##
###############################################################################

sub _croak { require Carp; goto &Carp::croak; }
sub _usage { _croak 'usage: [-use_dev_shm] [-keep_tmp_dir]'; }
sub _flag  { 1; }

my $_keep_tmp_dir = 0;
my $_loaded;

sub import {

   my $class = shift;
   return if ($_loaded++);

   my $_use_dev_shm = 0;
   my @_export_args = ();

   while (my $_arg = shift) {
      $_use_dev_shm  = _flag() and next if ($_arg eq '-use_dev_shm');
      $_keep_tmp_dir = _flag() and next if ($_arg eq '-keep_tmp_dir');
      _usage() if ($_arg =~ /^-/);
      push @_export_args, $_arg;
   }

   local $Exporter::ExportLevel = 1;
   Exporter::import($class, @_export_args);

   my ($_tmp_dir_base, $_count);

   if (exists $ENV{TEMP}) {
      if ($^O eq 'MSWin32') {
         $_tmp_dir_base = $ENV{TEMP} . '/mce';
         mkdir $_tmp_dir_base unless (-d $_tmp_dir_base);
      }
      else {
         $_tmp_dir_base = $ENV{TEMP};
      }
   }
   else {
      $_tmp_dir_base = ($_use_dev_shm && -d '/dev/shm') ? '/dev/shm' : '/tmp';
   }

   $_count = 0;
   $tmp_dir = "$_tmp_dir_base/$_prog_name.$$.$_count";

   while ( !(mkdir "$tmp_dir", 0770) ) {
      $tmp_dir = "$_tmp_dir_base/$_prog_name.$$." . (++$_count);
   }
}

###############################################################################
## ----------------------------------------------------------------------------
## Configure signal handling.
##
###############################################################################

## Set traps to catch signals.
$SIG{__DIE__}  = \&_sig_handler_die;
$SIG{__WARN__} = \&_sig_handler_warn;

$SIG{XCPU} = \&stop_and_exit if (exists $SIG{XCPU});   ## UNIX SIG 24
$SIG{XFSZ} = \&stop_and_exit if (exists $SIG{XFSZ});   ## UNIX SIG 25

$SIG{HUP}  = \&stop_and_exit;                          ## UNIX SIG  1
$SIG{INT}  = \&stop_and_exit;                          ## UNIX SIG  2
$SIG{PIPE} = \&stop_and_exit;                          ## UNIX SIG 13
$SIG{QUIT} = \&stop_and_exit;                          ## UNIX SIG  3
$SIG{TERM} = \&stop_and_exit;                          ## UNIX SIG 15

## For a more reliable MCE, $SIG{CHLD} is set to 'DEFAULT'. MCE handles
## the reaping of it's children, especially when running multiple MCEs
## simultaneously.
##
$SIG{CHLD} = 'DEFAULT' if ($^O ne 'MSWin32');

###############################################################################
## ----------------------------------------------------------------------------
## Call stop_and_exit when exiting the script.
##
###############################################################################

our $_mce_spawned_ref = undef;

END {
   MCE::Signal->_shutdown_mce();
   MCE::Signal->stop_and_exit($?) if ($$ == $_main_proc_id || $? != 0);
}

###############################################################################
## ----------------------------------------------------------------------------
## Run command via the system(...) function.
##
## The system function in Perl ignores SIGINT and SIGQUIT.  These 2 signals
## are sent to the command being executed via system() but not back to
## the underlying Perl script.  The code below will ensure the Perl script
## receives the same signal in order to raise an exception immediately
## after the system call.
##
## Returns the actual exit status.
##
###############################################################################

sub sys_cmd {

   shift @_ if (defined $_[0] && $_[0] eq 'MCE::Signal');
   _croak(ILLEGAL_STATE . ": no arguments was specified") if (@_ == 0);

   my $_status = system(@_);
   my $_sig_no = $_status & 127;
   my $_exit_status = $_status >> 8;

   ## Kill this process if command caught SIGINT or SIGQUIT.
   kill('INT',  $$) if $_sig_no == 2;
   kill('QUIT', $$) if $_sig_no == 3;

   return $_exit_status;
}

###############################################################################
## ----------------------------------------------------------------------------
## Stops execution, removes temp directory and exits cleanly.
##
## Provides safe reentrant logic for both parent and child processes.
## The $_main_proc_id variable is defined above.
##
###############################################################################

{
   my $_handler_cnt = 0;

   my %_sig_name_lkup = map { $_ => 1 } qw(
      HUP INT PIPE QUIT TERM CHLD XCPU XFSZ
   );

   sub stop_and_exit {

      shift @_ if (defined $_[0] && $_[0] eq 'MCE::Signal');

      my $_sig_name    = $_[0] || 0;
      my $_exit_status = $?;
      my $_is_sig      = 0;

      if (exists $_sig_name_lkup{$_sig_name}) {
         $SIG{$_sig_name} = sub { };
         $_exit_status = $_is_sig = 1;
      }
      else {
         $_exit_status = $_sig_name if ($_sig_name ne '0');
      }

      $SIG{TERM}     = sub { } if ($_sig_name ne 'TERM');
      $SIG{__DIE__}  = sub { };
      $SIG{__WARN__} = sub { };

      ## ----------------------------------------------------------------------

      ## For main thread / parent process.
      if ($$ == $_main_proc_id) {

         $_handler_cnt += 1;

         if ($_handler_cnt == 1 && ! -e "$tmp_dir/stopped") {
            open FH, "> $tmp_dir/stopped"; close FH;

            local $\ = undef;

            ## Display message and kill process group if signaled.
            if ($_is_sig == 1) {
               my $_err_msg = undef;

               if ($_sig_name eq 'XCPU') {
                  $_err_msg = "exceeded CPU time limit, exiting";
               }
               elsif ($_sig_name eq 'XFSZ') {
                  $_err_msg = "exceeded file size limit, exiting";
               }
               elsif ($_sig_name eq 'TERM' && -f "$tmp_dir/died") {
                  $_err_msg = "caught signal '__DIE__', exiting";
               }
               elsif ($_sig_name ne 'PIPE') {
                  $_err_msg = "caught signal '$_sig_name', exiting";
               }

               ## Display error message.
               print STDERR "\n## $_prog_name: $_err_msg\n" if ($_err_msg);
               open FH, "> $tmp_dir/killed"; close FH;

               ## Signal process group to terminate.
               kill('TERM', -$$) unless (-f "$tmp_dir/died");

               ## Pause a bit.
               if ($_sig_name ne 'PIPE') {
                  select(undef, undef, undef, 0.066) for (1..3);
               }
            }

            ## Remove temp directory.
            if (defined $tmp_dir && $tmp_dir ne '' && -d $tmp_dir) {
               if ($_keep_tmp_dir == 1) {
                  print STDERR "$_prog_name: saved tmp_dir = $tmp_dir\n";
               }
               else {
                  if ($tmp_dir ne '/tmp' && $tmp_dir ne '/var/tmp') {
                     rmtree($tmp_dir);
                  }
               }
               $tmp_dir = undef;
            }

            ## Signal process group to die.
            if ($_is_sig == 1) {
               print STDERR "\n" if ($_sig_name ne 'PIPE');
               kill('KILL', -$$, $_main_proc_id);
            }
         }
      }

      ## ----------------------------------------------------------------------

      ## For child processes.
      if ($$ != $_main_proc_id && $_is_sig == 1) {

         ## Obtain lock.
         open my $CHILD_LOCK, '+>>', "$tmp_dir/child.lock";
         flock $CHILD_LOCK, LOCK_EX;

         $_handler_cnt += 1;

         ## Signal process group to terminate.
         if ($_handler_cnt == 1) {

            ## Signal process group to terminate.
            if (! -f "$tmp_dir/killed" && ! -f "$tmp_dir/stopped") {
               open FH, "> $tmp_dir/killed"; close FH;
               kill('TERM', -$$, $_main_proc_id);
            }
         }

         ## Release lock.
         flock $CHILD_LOCK, LOCK_UN;
         close $CHILD_LOCK;
      }

      ## ----------------------------------------------------------------------

      ## Exit thread/process with status.
      if ($_is_sig == 1) {
         select(undef, undef, undef, 0.133) for (1..3);
      }

      threads->exit($_exit_status) if ($_has_threads && threads->can('exit'));
      exit $_exit_status;
   }
}

###############################################################################
## ----------------------------------------------------------------------------
## Shutdown MCEs that were previously initiated by this process ID and are
## still running.
##
###############################################################################

sub _shutdown_mce {

   if (defined $_mce_spawned_ref) {
      my $_tid = ($_has_threads) ? threads->tid() : '';
      $_tid = '' unless defined $_tid;

      foreach my $_mce_id (keys %{ $_mce_spawned_ref }) {
         if ($_mce_id =~ /\A$$\.$_tid\./) {
            $_mce_spawned_ref->{$_mce_id}->shutdown();
            delete $_mce_spawned_ref->{$_mce_id};
         }
      }
   }
}

###############################################################################
## ----------------------------------------------------------------------------
## Provides signal handler for the Perl __WARN__ exception.
##
###############################################################################

sub _sig_handler_warn {

   ## Display warning message with localtime. Ignore thread exiting messages
   ## coming from the user or OS signaling the script to exit.

   unless ($_[0] =~ /^A thread exited while \d+ threads were running/) {
      unless ($_[0] =~ /^Perl exited with active threads/) {
         local $\ = undef;
         my $_time_stamp = localtime();
         print STDERR "## $_time_stamp: $_prog_name: WARNING: $_[0]";
      }
   }
}

###############################################################################
## ----------------------------------------------------------------------------
## Provides signal handler for the Perl __DIE__ exception.
##
###############################################################################

{
   my $_handler_cnt = 0;

   sub _sig_handler_die {

      my $_exit_status = $?;
      return unless (defined $tmp_dir);

      local $SIG{__DIE__} = sub { };

      ## Obtain lock.
      open my $DIE_LOCK, '+>>', "$tmp_dir/die.lock";
      flock $DIE_LOCK, LOCK_EX;

      $_handler_cnt += 1;

      ## Display warning message with localtime. Signal main process to quit.
      if ($_handler_cnt == 1 && ! -e "$tmp_dir/died") {
         open FH, "> $tmp_dir/died"; close FH;

         local $\ = undef;
         my $_time_stamp = localtime();
         print STDERR "## $_time_stamp: $_prog_name: ERROR: $_[0]";
   
         MCE::Signal->stop_and_exit('TERM');
      }

      ## Release lock.
      flock $DIE_LOCK, LOCK_UN;
      close $DIE_LOCK;

      ## The main process will kill the process group before this expires.
      select(undef, undef, undef, 0.133) for (1..3);

      threads->exit($_exit_status) if ($_has_threads && threads->can('exit'));
      exit $_exit_status;
   }
}

1;

__END__

###############################################################################
## ----------------------------------------------------------------------------
## Module usage.
##
###############################################################################

=head1 NAME

MCE::Signal - Provides tmp_dir creation & signal handling for Many-Core Engine.

=head1 VERSION

This document describes MCE::Signal version 1.003

=head1 SYNOPSIS

 use MCE::Signal qw( [-use_dev_shm] [-keep_tmp_dir] );

Nothing is exported by default. Exportable are 1 variable and 2 subroutines:

 $tmp_dir          - Path to temp dir

 sys_cmd           - Execute cmd and return the actual exit status
 stop_and_exit     - Stops execution, removes tmp directory and exits

=head1 EXAMPLES

 ## Creates tmp_dir under /tmp
 use MCE::Signal;
 use MCE::Signal qw( :all );

 ## Attempt to create tmp_dir under /dev/shm, otherwise under /tmp
 use MCE::Signal qw( -use_dev_shm );

 ## Keep tmp_dir after script terminates
 use MCE::Signal qw( -keep_tmp_dir );
 use MCE::Signal qw( -use_dev_shm -keep_tmp_dir );

=head1 REQUIREMENTS

Perl 5.8.0 or later

=head1 SOURCE

The source is hosted at: L<http://code.google.com/p/many-core-engine-perl/>

=head1 AUTHOR

Mario E. Roy, S<E<lt>marioeroy AT gmail DOT comE<gt>>

=head1 COPYRIGHT AND LICENSE

Copyright (C) 2012 by Mario E. Roy

MCE::Signal is free software; you can redistribute it and/or modify it under
the same terms as Perl itself.

=cut
