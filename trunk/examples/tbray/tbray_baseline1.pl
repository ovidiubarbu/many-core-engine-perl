#!/usr/bin/perl

##
## http://www.tbray.org/ongoing/When/200x/2007/09/20/Wide-Finder
## requires the data at http://www.tbray.org/tmp/o10k.ap
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

