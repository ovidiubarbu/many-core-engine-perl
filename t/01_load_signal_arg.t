#!/usr/bin/env perl

use Test::More tests => 3;

## Default is $MCE::Signal::tmp_dir which points to $ENV{TEMP} if defined.
## Otherwise, pass argument to module wanting /dev/shm versus /tmp for
## temporary files. MCE::Signal falls back to /tmp unless /dev/shm exists.
##
## One optional argument not tested here is -keep_tmp_dir which omits the
## removal of $tmp_dir on exit. A message is displayed by MCE::Signal stating
## the location of $tmp_dir when exiting.
##
## Always load MCE::Signal before MCE when wanting to export or pass options.

BEGIN {
   use_ok('MCE::Signal', qw( $tmp_dir -use_dev_shm ));

   if (! exists $ENV{TEMP} && -d '/dev/shm') {
      ok($tmp_dir =~ m!^/dev/shm/!, 'Check tmp_dir matches ^/dev/shm/');
   }
   else {
      ok($tmp_dir !~ m!^/dev/shm/!, 'Check tmp_dir does not match ^/dev/shm/');
   }

   use_ok('MCE');
}

