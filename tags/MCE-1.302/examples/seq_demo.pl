#!/usr/bin/env perl

use strict;
use warnings;

use FindBin;
use lib "$FindBin::Bin/../lib";

use MCE;

## A demonstration applying sequences with user_tasks.
## Both non-chunking and chunking of sequences are supported.

## Run with seq_demo.pl | sort

sub user_func {
   my ($self, $seq_ref, $chunk_id) = @_;

   my $wid      = $self->wid();
   my $task_id  = $self->task_id();
   my $task_wid = $self->task_wid();

   if (ref $seq_ref eq 'ARRAY') {
      foreach my $seq_n (@{ $seq_ref }) {
         printf(
            "task_id %d: seq_n %s: chunk_id %d: wid %d: task_wid %d\n",
            $task_id,    $seq_n,   $chunk_id,   $wid,   $task_wid
         );
      }
   }
   else {
      my $seq_n = $seq_ref;
      printf(
         "task_id %d: seq_n %s: chunk_id %d: wid %d: task_wid %d\n",
         $task_id,    $seq_n,   $chunk_id,   $wid,   $task_wid
      );
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

