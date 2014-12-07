#!/usr/bin/env perl

##
## usage: tbray_baseline2.pl < logfile
##
## This runs faster than baseline1 -- regex optimization is in effect here
## whereas not the case in baseline1.
##

use strict;
use warnings;

use Time::HiRes qw(time);

my $start = time;

my $rx = qr|GET /ongoing/When/\d\d\dx/(\d\d\d\d/\d\d/\d\d/[^ .]+) |;
my %count;
while (<>) {
    next unless $_ =~ /$rx/o;
    $count{$1}++;
}

my $end = time;

print "$count{$_}\t$_\n"
  for ( sort { $count{$b} <=> $count{$a} } keys %count )[ 0 .. 9 ];

printf "\n## Compute time: %0.03f\n\n",  $end - $start;

