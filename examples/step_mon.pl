#!/usr/bin/perl
###############################################################################
##
## Monitor script contributed by George Bouras (Greece 02/2015).
## A demonstration for MCE->step_to(...) in MCE 1.601.
##
###############################################################################

use strict;
use warnings;

use Cwd 'abs_path'; ## Insert lib-path at the head of @INC.
use lib abs_path($0 =~ m{^(.*)[\\/]} && $1 || abs_path) . '/../lib';

use Time::HiRes 'sleep';
use MCE::Step fast => 1;

my %Monitors = (
   mon_cpu => [ qw/ srvC1 srvC2 srvC3 srvC4 srvC5 srvC6 srvC7 / ],
   mon_dsk => [ qw/ srvD1 srvD2 srvD3 srvD4 srvD5 srvD6 srvD7 / ],
   mon_mem => [ qw/ srvM1 srvM2 srvM3 srvC4 srvM5 srvM6 srvM7 / ]
);

## Checking frequency (in seconds) of every monitor.
## Using fractional seconds for this demonstration.

my %Schedules = (
   0.15 => [ qw/ mon_cpu mon_dsk / ],
   0.35 => [ qw/ mon_mem         / ]
);

###############################################################################
##
## Run 4 sub-tasks simultaneously using one MCE instance
##
###############################################################################

MCE::Step::run({
   task_name    => [ qw/ scheduler mon_cpu mon_dsk mon_mem / ],
   max_workers  => [ scalar keys %Schedules, 10, 10, 10 ],
   input_data   => [ sort { $a <=> $b } keys %Schedules ],
   chunk_size   => 1,

   user_begin   => sub { my ($mce, $task_id, $task_name) = @_; },
   user_end     => sub { my ($mce, $task_id, $task_name) = @_; },
   user_output  => sub { print STDOUT "$_[0]\n" },
   user_error   => sub { print STDERR "$_[0]\n" },

   spawn_delay  => 0,
   submit_delay => 0,
   job_delay    => 0,

}, \&Scheduler, \&Monitor, \&Monitor, \&Monitor);

MCE::Step::finish;

###############################################################################
##
## Sub-tasks for Many-Core Engine.
##
###############################################################################

sub Scheduler
{
   my $mce_shedule = shift;
   my $chunk_ref   = shift;
   my $chunk_id    = shift;
   my $worker_id   = $mce_shedule->wid;
   my $seconds     = $chunk_ref->[0];
   my @WorkLoad;
   my $work_ref;

   ## change to while ('FOR EVER') if desired
   for (1 .. 10) {
      foreach my $mon (@{ $Schedules{$seconds} }) {

         MCE->print(
            "interval $seconds sec, starting monitors : " .
            "@{ $Schedules{$seconds} }"
         );

         foreach my $srv (@{ $Monitors{$mon} }) {
            MCE->step_to($mon, $srv);
         }

         sleep $seconds;

         ## Wait until processing has completed for submitted work
         MCE->await($mon, 10);  # blocks until 10 or less remaining
      }
   }

   return;
}

sub Monitor
{
   my $mce_monitor = shift;
   my $server      = shift;
   my $monitor     = $mce_monitor->task_name;
   my $worker_id   = $mce_monitor->wid;

   MCE->print("monitor=$monitor , server=$server");

   return;
}

