#!/usr/bin/env perl

use strict; use warnings;

use Cwd qw(abs_path);
use lib abs_path . "/../lib";

## usage: ./files_mce.pl [ startdir [0|1] ]

use MCE;
use MCE::Queue;

my $F = MCE::Queue->new(fast => defined $ARGV[1] ? $ARGV[1] : 1);
my $consumers = 8;

my $mce = MCE->new(

   task_end => sub {
      my ($mce, $task_id, $task_name) = @_;

      $F->enqueue((undef) x $consumers)
         if $task_name eq 'dir';
   },

   user_tasks => [{
      max_workers => 1, task_name => 'dir',

      user_func => sub {
         my $D = MCE::Queue->new(queue => [ MCE->user_args->[0] ]);

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

)->run({ user_args => [ $ARGV[0] || '.' ] });

