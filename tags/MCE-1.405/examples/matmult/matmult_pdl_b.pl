#!/usr/bin/env perl

##
## Usage:
##    perl matmult_pdl_b.pl 1024         ## Default size 512
##

use strict;
use warnings;

use Cwd qw(abs_path);
use lib abs_path . "/../../lib";

my $prog_name = $0; $prog_name =~ s{^.*[\\/]}{}g;

use Time::HiRes qw(time);

use PDL;

###############################################################################
 # * # * # * # * # * # * # * # * # * # * # * # * # * # * # * # * # * # * # * #
###############################################################################

my $tam = shift;
   $tam = 512 unless (defined $tam);

unless ($tam > 1) {
   print STDERR "Error: $tam must be an integer greater than 1. Exiting.\n";
   exit 1;
}

my $a = sequence $tam,$tam;
my $b = sequence $tam,$tam;

my $start = time();

my $c = $a x $b;                         ## Performs matrix multiplication

my $end = time();

printf STDERR "\n## $prog_name $tam: compute time: %0.03f secs\n\n",
   $end - $start;

my $dim_1 = $tam - 1;

print "## (0,0) ", $c->at(0,0), "  ($dim_1,$dim_1) ", $c->at($dim_1,$dim_1);
print "\n\n";

###############################################################################
 # * # * # * # * # * # * # * # * # * # * # * # * # * # * # * # * # * # * # * #
###############################################################################

