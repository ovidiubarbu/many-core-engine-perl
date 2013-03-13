#!/usr/bin/env perl
###############################################################################
## ----------------------------------------------------------------------------
## Barrier synchronization example. Requires MCE 1.406 to work.
##
## http://en.wikipedia.org/wiki/Barrier_(computer_science)
##
###############################################################################

use strict;
use warnings;

use Cwd qw(abs_path);
use lib abs_path . "/../lib";

use MCE 1.406;

sub user_func {

   my ($self) = @_;
   my $wid = $self->wid();

   $self->sendto('stdout', "a: $wid\n");
   $self->sync;

   $self->sendto('stdout', "b: $wid\n");
   $self->sync;

   $self->sendto('stdout', "c: $wid\n");
   $self->sync;
}

my $mce = MCE->new(
   max_workers => 4,
   user_func   => \&user_func
);

$mce->run();

