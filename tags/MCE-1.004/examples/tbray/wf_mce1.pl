#!/usr/bin/perl -s

##
## Parallizing baseline code via MCE -- Part 1 of 3.
##
## usage:
##    perl -s wf_mce1.pl -J=$N -C=$N $LOGFILE
##
##    where $N is the number of processes, $C is the chunk size,
##    and $LOGFILE is the target
##
##    defaults: -J=8 -C=200000
##

use Time::HiRes qw(time);
use MCE;

our $C ||= 200000;
our $J ||= 8;

my $logfile = shift;
my %count = ();

## Callback function for aggregating total counted.

sub store_result {
   my $count_ref = shift;
   $count{$_} += $count_ref->{$_} for (keys %$count_ref);
}

## Parallelize via MCE.

my $start = time();

my $mce = MCE->new(
   chunk_size  => $C,
   max_workers => $J,
   input_data  => $logfile,

   user_func => sub {
      my ($self, $chunk_ref, $chunk_id) = @_;

      my $rx = qr{GET /ongoing/When/\d\d\dx/(\d\d\d\d/\d\d/\d\d/[^ .]+) };
      my %count = ();

      for ( @$chunk_ref ) {
         next unless $_ =~ /$rx/o;
         $count{$1}++;
      }

      $self->do('store_result', \%count);
   }
);

$mce->run();

my $end = time();

## Display the top 10 hits.

print "$count{$_}\t$_\n"
   for (sort { $count{$b} <=> $count{$a} } keys %count)[ 0 .. 9 ];

printf "\n## Compute time: %0.03f\n\n",  $end - $start;

