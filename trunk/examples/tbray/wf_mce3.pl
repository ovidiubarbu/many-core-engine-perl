#!/usr/bin/perl -s

##
## Part 3 of 3.
##
## usage:
##    perl -s wf_mce3.pl -J=$N -C=$N $LOGFILE
##
##    where $N is the number of processes, $C is the chunk size,
##    and $LOGFILE is the target
##
##    defaults: -J=8 -C=2000000
##

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
}

## Parallelize via MCE. Think of user_begin, user_func, user_end like the awk
## scripting language: awk 'BEGIN { ... } { ... } END { ... }'. All workers
## submit their counts once versus per each chunk.

my $start = time();

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

   user_end => sub {
      my $self = shift;
      $self->do('store_result', $self->{wk_count});
   },

   user_func => sub {
      my ($self, $chunk_ref, $chunk_id) = @_;
      my $rx = $self->{wk_rx};
      $self->{wk_count}{$1}++ while ( $$chunk_ref =~ /$rx/go );
   }
);

$mce->run();

my $end = time();

## Display the top 10 hits.

print "$count{$_}\t$_\n"
   for (sort { $count{$b} <=> $count{$a} } keys %count)[ 0 .. 9 ];

printf "\n## Compute time: %0.03f\n\n",  $end - $start;

