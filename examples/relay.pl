#!/usr/bin/env perl

use strict;
use warnings;

use Cwd 'abs_path'; ## Insert lib-path at the head of @INC.
use lib abs_path($0 =~ m{^(.*)[\\/]} && $1 || abs_path) . '/../lib';

use MCE::Flow;

## Receiving and passing on information.

mce_flow {
   max_workers => 4, init_relay => 2,
},
sub {
   my $wid = MCE->wid;

   ## do work

   my $num = $wid * 2;

   ## relay next value; $_ is the same as $val

   my $val = MCE->relay( sub { $_ * $num } );

   print "$wid : $num : $val\n";
};

