#!/usr/bin/env perl
###############################################################################
## ----------------------------------------------------------------------------
## UTF-8 example.
##
###############################################################################

use strict;
use warnings;

use Cwd qw(abs_path);
use lib abs_path . "/../lib";

use MCE::Loop chunk_size => 'auto', max_workers => 'auto';

use utf8;
use open qw(:utf8 :std);

###############################################################################
## ----------------------------------------------------------------------------
## UTF-8 example. This is based on a sample code sent to me by Marcus Smith.
##
###############################################################################

## Iterate over @list and output to STDOUT three times:
## - Once from a normal for-loop,
## - Once after fetching results from MCE->do()
## - Once after fetching results from MCE->gather()

## Some Unicode characters from Basic Latin, Latin-1, and beyond.
## my @list = (qw(U Ö Å Ǣ Ȝ), "\N{INTERROBANG}");

my @list = qw(U Ö Å Ǣ Ȝ);

print "0: for-loop: $_\n" for (@list);
print "\n";

MCE::Loop::init { gather => sub { print shift() } };

sub callback {
   print $_[0];
}

mce_loop {
   my $wid = MCE->wid();

   for (@{ $_ }) {
      MCE->do("callback", "$wid: MCE->do: $_\n");
   }

   MCE->sync();

   for (@{ $_ }) {
      MCE->gather("$wid: MCE->gather: $_\n");
   }

} @list;

print "\n";

###############################################################################
## ----------------------------------------------------------------------------
## Process a scalar like a file. Setting chunk_size to 1 for the demonstration.
##
###############################################################################

my $unicode = join("\n", @list) . "\n";

MCE::Loop::init { chunk_size => 1 };

mce_loop_f {
   my $wid = MCE->wid();
   MCE->print("$wid: MCE->print: $_");

} \$unicode;

print "\n";

