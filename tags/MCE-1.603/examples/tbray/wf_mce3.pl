#!/usr/bin/env perl -s

##
## Part 3 of 3 with slurpio => 1.
##
## usage:
##    perl -s wf_mce3.pl -J=$N -C=$N $LOGFILE
##
##    where $N is the number of processes, $C is the chunk size,
##    and $LOGFILE is the target
##
##    defaults: -J=8 -C=2000000
##

use strict;
use warnings;

use Cwd 'abs_path'; ## Insert lib-path at the head of @INC.
use lib abs_path($0 =~ m{^(.*)[\\/]} && $1 || abs_path) . '/../../lib';

use Time::HiRes qw(time);
use MCE;

our $C ||= 2000000;
our $J ||= 8;

my $logfile = shift;
my %count = ();

## Callback function for aggregating total counted.

sub store_result {
   my $count_ref = shift;
   $count{$_} += $count_ref->{$_} for (keys %$count_ref);
   return;
}

## Parallelize via MCE.

my $start = time;

my $mce = MCE->new(
   chunk_size  => $C,
   max_workers => $J,
   input_data  => $logfile,
   use_slurpio => 1,

   user_begin => sub {
      my $self = shift;
      $self->{wk_count} = {};
      $self->{wk_rx} = qr{GET /ongoing/When/\d\d\dx/(\d\d\d\d/\d\d/\d\d/[^ .]+) };
   },

   user_func => sub {
      my ($self, $chunk_ref, $chunk_id) = @_;
      $self->{wk_count}{$1}++ while ( $$chunk_ref =~ /$self->{wk_rx}/go );
   },

   user_end => sub {
      my $self = shift;
      $self->do('store_result', $self->{wk_count});
   }
);

$mce->run;

my $end = time;

## Display the top 10 hits.

print "$count{$_}\t$_\n"
   for (sort { $count{$b} <=> $count{$a} } keys %count)[ 0 .. 9 ];

printf "\n## Compute time: %0.03f\n\n",  $end - $start;

