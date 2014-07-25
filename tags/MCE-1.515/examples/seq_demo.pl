#!/usr/bin/env perl

use strict;
use warnings;

use Cwd qw(abs_path);
use lib abs_path . "/../lib";

use MCE;

## A demonstration applying sequences with user_tasks.
## Chunking can also be configured independently as well.

## Run with seq_demo.pl | sort

sub user_func {
   my ($mce, $seq_n, $chunk_id) = @_;

   my $wid      = $mce->wid();
   my $task_id  = $mce->task_id();
   my $task_wid = $mce->task_wid();

   if (ref $seq_n eq 'ARRAY') {
      ## Received the next "chunked" sequence of numbers
      ## e.g. when chunk_size > 1, $seq_n will be an array ref above

      foreach (@{ $seq_n }) {
         $mce->sendto('STDOUT', sprintf(
            "task_id %d: seq_n %s: chunk_id %d: wid %d: task_wid %d\n",
            $task_id,    $_,       $chunk_id,   $wid,   $task_wid
         ));
      }
   }
   else {
      $mce->sendto('STDOUT', sprintf(
         "task_id %d: seq_n %s: chunk_id %d: wid %d: task_wid %d\n",
         $task_id,    $seq_n,   $chunk_id,   $wid,   $task_wid
      ));
   }
}

## Each task can be configured independently.

my $mce = MCE->new(
   user_tasks => [{
      max_workers => 2,
      chunk_size  => 1,
      sequence    => { begin => 11, end => 19, step => 1 },
      user_func   => \&user_func
   },{
      max_workers => 2,
      chunk_size  => 5,
      sequence    => { begin => 21, end => 29, step => 1 },
      user_func   => \&user_func
   },{
      max_workers => 2,
      chunk_size  => 3,
      sequence    => { begin => 31, end => 39, step => 1 },
      user_func   => \&user_func
   }]
);

$mce->run();

