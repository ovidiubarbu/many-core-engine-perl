#!/usr/bin/env perl

## Receiving and passing on information demonstration.
## Also, see findnull.pl for another use case.

use strict;
use warnings;

use Cwd 'abs_path'; ## Insert lib-path at the head of @INC.
use lib abs_path($0 =~ m{^(.*)[\\/]} && $1 || abs_path) . '/../lib';

use MCE::Flow;

## Relaying is orderly and driven by chunk_id when processing data, otherwise
## task_wid. Only the first sub-task is allowed to relay information.
##
##    use MCE::Subs;
##    my $val = mce_relay { $_ * $num };

mce_flow {
   max_workers => 4, init_relay => 2,
},
sub {
   my $wid = MCE->wid;

   ## Do work.

   my $num = $wid * 2;

   ## Relay the next value. $_ is same as $val inside the block.
   ## my $val = MCE->relay( sub { $_ * $num } );

   my $val = MCE::relay { $_ * $num };

   print "$wid : $num : $val\n";
};

