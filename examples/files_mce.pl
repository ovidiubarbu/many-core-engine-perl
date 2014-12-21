#!/usr/bin/env perl

use strict;
use warnings;

use Cwd 'abs_path'; ## Insert lib-path at the head of @INC.
use lib abs_path($0 =~ m{^(.*)[\\/]} && $1 || abs_path) . '/../lib';

## usage: ./files_mce.pl [ startdir [0|1] ]

use Time::HiRes 'sleep';

use MCE;
use MCE::Queue;

my $D = MCE::Queue->new(queue => [ $ARGV[0] || '.' ]);
my $F = MCE::Queue->new(fast => defined $ARGV[1] ? $ARGV[1] : 1);

my $providers = 3;
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

