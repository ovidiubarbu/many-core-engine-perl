#!/usr/bin/env perl

use strict;
use warnings;

use FindBin;
use lib "$FindBin::Bin/../lib";

##
## A demonstration applying sequence with user_tasks. Each task can be
## configured with a unique sequence specification. Look at the forseq.pl
## example as well.
##
## Run with seq_demo.pl | sort
##

use MCE;

sub user_func {
   my ($self, $seq_i, $chunk_id) = @_;

   my $wid      = $self->wid();
   my $task_id  = $self->task_id();
   my $task_wid = $self->task_wid();

   printf(
      "task_id %d: seq_i %s: chunk_id %d: wid %d: task_wid %d\n",
      $task_id,    $seq_i,   $chunk_id,   $wid,   $task_wid
   );
}

my $mce = MCE->new(
   user_tasks => [{
      max_workers => 2,
      sequence => { begin => 11, end => 19, step => 1 },
      user_func => \&user_func
   },{
      max_workers => 2,
      sequence => { begin => 21, end => 29, step => 1 },
      user_func => \&user_func
   },{
      max_workers => 2,
      sequence => { begin => 31, end => 39, step => 1 },
      user_func => \&user_func
   }]
);

$mce->run();

