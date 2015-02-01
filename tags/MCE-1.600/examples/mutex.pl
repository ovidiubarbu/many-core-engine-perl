#!/usr/bin/env perl

use strict;
use warnings;

use Cwd 'abs_path'; ## Insert lib-path at the head of @INC.
use lib abs_path($0 =~ m{^(.*)[\\/]} && $1 || abs_path) . '/../lib';

use MCE::Flow max_workers => 4;
use MCE::Mutex;

print "## running a\n";
my $a = MCE::Mutex->new;

mce_flow sub {
   $a->lock;

   ## access shared resource
   my $wid = MCE->wid; MCE->say($wid); sleep 1;

   $a->unlock;
};

print "## running b\n";
my $b = MCE::Mutex->new;

mce_flow sub {
   $b->synchronize( sub {

      ## access shared resource
      my ($wid) = @_; MCE->say($wid); sleep 1;

   }, MCE->wid );
};

