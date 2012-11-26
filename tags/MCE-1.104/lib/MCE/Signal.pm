###############################################################################
## ----------------------------------------------------------------------------
## MCE::Signal
## -- Provides tmp_dir creation & signal handling for Many-Core Engine.
##
###############################################################################

package MCE::Signal;

our ($has_threads, $main_proc_id, $prog_name);

BEGIN {
   $main_proc_id = $$; $prog_name = $0; $prog_name =~ s{^.*[\\/]}{}g;
}

use strict;
use warnings;

our $VERSION = '1.104';
$VERSION = eval $VERSION;

use Fcntl qw( :flock );
use File::Path qw( rmtree );
use base 'Exporter';

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
      $_tmp_dir_base = ($_use_dev_shm && -d '/dev/shm' && -w '/dev/shm')
         ? '/dev/shm' : '/tmp';
   }

   _croak("MCE::Signal::import: '$_tmp_dir_base' is not writeable")
      unless (-w $_tmp_dir_base);

   $_count = 0;
   $tmp_dir = "$_tmp_dir_base/$prog_name.$$.$_count";

   while ( !(mkdir "$tmp_dir", 0770) ) {
      $tmp_dir = "$_tmp_dir_base/$prog_name.$$." . (++$_count);
   }
}

###############################################################################
## ----------------------------------------------------------------------------
## Configure signal handling.
##
###############################################################################

## Set traps to catch signals.
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

our $mce_spawned_ref = undef;

END {
   MCE::Signal->shutdown_mce();
   MCE::Signal->stop_and_exit($?) if ($$ == $main_proc_id || $? != 0);
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
   _croak("MCE::Signal::sys_cmd: no arguments was specified") if (@_ == 0);

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
## The $main_proc_id variable is defined above.
##
###############################################################################

{
   my $_handler_cnt = 0;

   my %_sig_name_lkup = map { $_ => 1 } qw(
      __DIE__ __WARN__ HUP INT PIPE QUIT TERM CHLD XCPU XFSZ
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
      if ($$ == $main_proc_id) {

         $_handler_cnt += 1;

         if ($_handler_cnt == 1 && ! -e "$tmp_dir/stopped") {
            open my $_FH, "> $tmp_dir/stopped"; close $_FH;

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
               print STDERR "\n## $prog_name: $_err_msg\n" if ($_err_msg);
               open my $_FH, "> $tmp_dir/killed"; close $_FH;

               ## Signal process group to terminate.
               kill('TERM', -$$);

               ## Pause a bit.
               if ($_sig_name ne 'PIPE') {
                  select(undef, undef, undef, 0.066) for (1..3);
               }
            }

            ## Remove temp directory.
            if (defined $tmp_dir && $tmp_dir ne '' && -d $tmp_dir) {
               if ($_keep_tmp_dir == 1) {
                  print STDERR "$prog_name: saved tmp_dir = $tmp_dir\n";
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
               kill('KILL', -$$, $main_proc_id);
            }
         }
      }

      ## ----------------------------------------------------------------------

      ## For child processes.
      if ($$ != $main_proc_id && $_is_sig == 1) {

         ## Obtain lock.
         open my $CHILD_LOCK, '+>>', "$tmp_dir/child.lock";
         flock $CHILD_LOCK, LOCK_EX;

         $_handler_cnt += 1;

         ## Signal process group to terminate.
         if ($_handler_cnt == 1) {

            ## Notify the main process that I've died.
            if ($_sig_name eq '__DIE__') {
               open my $_FH, "> $tmp_dir/died"; close $_FH;
            }

            ## Signal process group to terminate.
            if (! -f "$tmp_dir/killed" && ! -f "$tmp_dir/stopped") {
               open my $_FH, "> $tmp_dir/killed"; close $_FH;
               kill('TERM', -$$, $main_proc_id);
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

      threads->exit($_exit_status) if ($has_threads && threads->can('exit'));
      exit $_exit_status;
   }
}

###############################################################################
## ----------------------------------------------------------------------------
## Shutdown MCEs that were previously initiated by this process ID and are
## still running.
##
###############################################################################

sub shutdown_mce {

   if (defined $mce_spawned_ref) {
      my $_tid = ($has_threads) ? threads->tid() : '';
      $_tid = '' unless defined $_tid;

      foreach my $_mce_sid (keys %{ $mce_spawned_ref }) {
         if ($_mce_sid =~ /\A$$\.$_tid\./) {
            $mce_spawned_ref->{$_mce_sid}->shutdown();
            delete $mce_spawned_ref->{$_mce_sid};
         }
      }
   }
}

###############################################################################
## ----------------------------------------------------------------------------
## Signal handlers for __DIE__ & __WARN__ utilized by MCE.
##
###############################################################################

sub die_handler {

   shift @_ if (defined $_[0] && $_[0] eq 'MCE::Signal');

   local $SIG{__DIE__} = sub { };

   ## Display die message with localtime.

   local $\ = undef; my $_time_stamp = localtime();
   print STDERR "## $_time_stamp: $prog_name: ERROR:\n", $_[0];

   MCE::Signal->stop_and_exit('__DIE__');
}

sub warn_handler {

   shift @_ if (defined $_[0] && $_[0] eq 'MCE::Signal');

   ## Display warning message with localtime. Ignore thread exiting messages
   ## coming from the user or OS signaling the script to exit.

   unless ($_[0] =~ /^A thread exited while \d+ threads were running/) {
      unless ($_[0] =~ /^Perl exited with active threads/) {
         local $\ = undef; my $_time_stamp = localtime();
         print STDERR "## $_time_stamp: $prog_name: WARNING:\n", $_[0];
      }
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

This document describes MCE::Signal version 1.104

=head1 SYNOPSIS

 use MCE::Signal qw( [-use_dev_shm] [-keep_tmp_dir] );

=head1 DESCRIPTION

This package configures $SIG{HUP,INT,PIPE,QUIT,TERM,XCPU,XFSZ} to point to
stop_and_exit and creates a temporary directory. The main process or workers
receiving said signal calls stop_and_exit, which signals all workers to
terminate, removes the temporary directory unless -keep_tmp_dir is specified,
and terminates itself.

The location of tmp dir resides under $ENV{TEMP} if configured, otherwise
/dev/shm if writeable and -use_dev_shm is specified, or /tmp.

Tmp dir resides under $ENV{TEMP}/mce/ when running Perl on Microsoft Windows.

Nothing is exported by default. Exportable are 1 variable and 2 subroutines:

 $tmp_dir          - Path to temporary directory.

 sys_cmd           - Execute command and return the actual exit status.
 stop_and_exit     - Stops execution, removes tmp directory and exits
                     the entire application.

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
the same terms as Perl itself L<http://dev.perl.org/licenses/>.

=cut
