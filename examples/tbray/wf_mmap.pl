#!/usr/bin/env perl -s
## wf.pl -- an implementation of the "wide finder" benchmark.
## Sean O'Rourke, 2007, public domain.
##
## Usage: perl -s wf.pl -J=$N $LOGFILE
##     where $N is the number of processes, and $LOGFILE is the target.
##
## This code depends on Sys::Mmap, which is available on CPAN.

use strict qw(subs refs);
use warnings;

## no critic (InputOutput::ProhibitBarewordFileHandles)
## no critic (InputOutput::ProhibitTwoArgOpen)

use Cwd 'abs_path';  ## Remove taintedness from path
use lib ($_) = (abs_path().'/../../lib') =~ /(.*)/;

use Time::HiRes qw(time);
use Sys::Mmap;

$J ||= 8;

my $file = shift;

open IN, '<', $file or die $!;
my $str;

mmap $str, 0, PROT_READ, MAP_SHARED, IN;

my %h;
my $n = 0;

my $start = time;

unless ($J) {
    ## serial
    $h{$1}++ while $str =~ m{GET /ongoing/When/\d\d\dx/(\d\d\d\d/\d\d/\d\d/[^ .]+) }g;
} else {
    local $|=1;
    ## parallel -- ugh.
    my $size = -s IN;
    my $nperj = int(($size + $J - 1) / $J);
    my @fhs;
    use Storable qw(store_fd fd_retrieve);
    for my $i (0..$J-1) {
        my $pid = open my $fh, "-|";
        die unless defined $pid;
        if ($pid) {
            push @fhs, $fh;
        } else {
            pos($str) = $i ? rindex($str, "\n", $nperj * $i) || 0 : 0;
            my $end = ($i+1) * $nperj;
            $h{$1}++ while pos($str) < $end &&
                $str =~ m{GET /ongoing/When/\d\d\dx/(\d\d\d\d/\d\d/\d\d/[^ .]+) }g;
            store_fd \%h, \*STDOUT or die "$i can't store!\n";
            exit 0;
        }
    }
    for (0..$#fhs) {
        my $h = fd_retrieve $fhs[$_] or die "I can't load $_\n";
        while (my ($k, $v) = each %$h) {
            $h{$k} += $v;
        }
        close $fhs[$_] or warn "$_ exited weirdly.";
    }
}

my $end = time;

for (sort { $h{$b} <=> $h{$a} } keys %h) {
    print "$h{$_}\t$_\n";
    last if ++$n >= 10;
}

printf "\n## Compute time: %0.03f\n\n",  $end - $start;

