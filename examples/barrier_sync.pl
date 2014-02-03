#!/usr/bin/env perl
###############################################################################
## ----------------------------------------------------------------------------
## Barrier synchronization example.
## http://en.wikipedia.org/wiki/Barrier_(computer_science)
##
###############################################################################

use strict;
use warnings;

use Cwd qw(abs_path);
use lib abs_path . "/../lib";

use MCE;

sub user_func {

   my ($mce) = @_; 
   my $wid = MCE->wid();

   MCE->sendto("STDOUT", "a: $wid\n");
   MCE->sync;

   MCE->sendto("STDOUT", "b: $wid\n");
   MCE->sync;

   MCE->sendto("STDOUT", "c: $wid\n");
   MCE->sync;
}

my $mce = MCE->new(
   max_workers => 4, user_func => \&user_func

)->run();

