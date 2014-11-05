#!/usr/bin/env perl

use strict; use warnings;

use Cwd qw(abs_path);
use lib abs_path . "/../lib";
use Time::HiRes "sleep";

## usage: ./files_thr.pl [ startdir ]

use threads;
use threads::shared;

use MCE;
use Thread::Queue;

my $D = Thread::Queue->new($ARGV[0] || '.');
my $F = Thread::Queue->new();

## Glob() is not thread-safe in Perl 5.16.x; okay < 5.16; fixed in 5.18.2.
## Run with perl5.12 on Mavericks. Not all OS vendors have patched 5.16.x.
my $providers = ($] < 5.016000 || $] >= 5.018002) ? 3 : 1;
my $consumers = 8;

my $mce = MCE->new(

   task_end => sub {
      my ($mce, $task_id, $task_name) = @_;

      $F->enqueue((undef) x $consumers)
         if $task_name eq 'dir';
   },

   user_tasks => [{
      max_workers => $providers, task_name => 'dir',

      user_func => sub {
         ## Allow time for wid 1 to enqueue any dir entries.
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
      }
   },{
      max_workers => $consumers, task_name => 'file',

      user_func => sub {
         while (defined (my $file = $F->dequeue)) {
            MCE->say($file);
         }
      }
   }]

)->run;

