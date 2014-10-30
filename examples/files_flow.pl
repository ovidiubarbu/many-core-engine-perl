#!/usr/bin/env perl

use strict; use warnings;

use Cwd qw(abs_path);
use lib abs_path . "/../lib";
use Time::HiRes "sleep";

## Same logic as in files_mce.pl, but with the MCE::Flow model.
## usage: ./files_flow.pl [ startdir [0|1] ]

use MCE::Flow;
use MCE::Queue;

my $D = MCE::Queue->new(queue => [ $ARGV[0] || '.' ]);
my $F = MCE::Queue->new(fast => defined $ARGV[1] ? $ARGV[1] : 1);

my $providers = 3;
my $consumers = 8;

MCE::Flow::init {
   task_end => sub {
      my ($mce, $task_id, $task_name) = @_;

      $F->enqueue((undef) x $consumers)
         if $task_name eq 'dir';
   }
};

## Override any MCE options and run. Notice how max_workers and
## task_name take an anonymous array to configure both tasks.

mce_flow {
   max_workers => [ $providers, $consumers ],
   task_name   => [ 'dir', 'file' ]
},
sub {
   ## Dir Task. Allow time for wid 1 to enqueue any dir entries.
   ## Otherwise, workers (wid 2+) may terminate early.
   sleep 0.1 if MCE->task_wid > 1;

   while (defined (my $dir = $D->dequeue_nb)) {
      my (@files, @dirs); foreach (glob("$dir/*")) {
         if (-d $_) { push @dirs, $_; next; }
         push @files, $_;
      }
      $D->enqueue(@dirs ) if scalar @dirs;
      $F->enqueue(@files) if scalar @files;
   }
},
sub {
   ## File Task.
   while (defined (my $file = $F->dequeue)) {
      MCE->say($file);
   }
};

## Workers persist in models. This may be ommitted. It will run
## automatically during exiting if not already called.

MCE::Flow::finish;

