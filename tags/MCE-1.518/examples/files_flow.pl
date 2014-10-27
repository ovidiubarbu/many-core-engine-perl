#!/usr/bin/env perl -s

use strict; use warnings;

use Cwd qw(abs_path);
use lib abs_path . "/../lib";

## Same logic as files_mce.pl; via the MCE::Flow model.
## usage: ./files_flow.pl [ startdir [0|1] ]

use MCE::Flow;
use MCE::Queue;

my $F = MCE::Queue->new(fast => defined $ARGV[1] ? $ARGV[1] : 1);
my $consumers = 8;

MCE::Flow::init {
   task_end => sub {
      my ($mce, $task_id, $task_name) = @_;

      $F->enqueue((undef) x $consumers)
         if $task_name eq 'dir';
   }
};

mce_flow {
   ## MCE options may be specified here or MCE::Flow::init above
   max_workers => [ 1, $consumers ], task_name => [ 'dir', 'file' ],
   user_args => [ $ARGV[0] || '.' ]
},
sub {
   ## User task: dir
   my $D = MCE::Queue->new(queue => [ MCE->user_args->[0] ]);

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
   ## User task: file
   while (defined (my $file = $F->dequeue)) {
      MCE->say($file);
   }
};

MCE::Flow::finish;

