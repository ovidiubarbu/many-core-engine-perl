#!/usr/bin/env perl
###############################################################################
## ----------------------------------------------------------------------------
## Barrier synchronization example.
## http://en.wikipedia.org/wiki/Barrier_(computer_science)
##
###############################################################################

use strict;
use warnings;

use Cwd 'abs_path'; ## Insert lib-path at the head of @INC.
use lib abs_path($0 =~ m{^(.*)[\\/]} && $1 || abs_path) . '/../lib';

use MCE;

sub user_func {

   my ($mce) = @_; 
   my $wid = MCE->wid;

   MCE->sendto("STDOUT", "a: $wid\n");   ## MCE 1.0+
   MCE->sync;

   MCE->sendto(\*STDOUT, "b: $wid\n");   ## MCE 1.5+
   MCE->sync;

   MCE->print("c: $wid\n");              ## MCE 1.5+
   MCE->sync;

   return;
}

my $mce = MCE->new(
   max_workers => 4, user_func => \&user_func
)->run;

