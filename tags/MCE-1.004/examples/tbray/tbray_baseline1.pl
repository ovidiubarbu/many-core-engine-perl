#!/usr/bin/perl

##
## usage: tbray_baseline1.pl < logfile
##

use Time::HiRes qw(time);

my $start = time();

my $rx = qr|GET /ongoing/When/\d\d\dx/(\d\d\d\d/\d\d/\d\d/[^ .]+) |o;
my %count;
while (<>) {
    next unless $_ =~ $rx;
    $count{$1}++;
}

my $end = time();

print "$count{$_}\t$_\n"
  for ( sort { $count{$b} <=> $count{$a} } keys %count )[ 0 .. 9 ];

printf "\n## Compute time: %0.03f\n\n",  $end - $start;

