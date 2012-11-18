#!/usr/bin/env perl

use Cwd qw( abs_path );
our ($prog_name, $prog_dir, $base_dir);

BEGIN {
   $ENV{PATH} = "/usr/bin:/bin:/usr/sbin:/sbin:$ENV{PATH}";

   umask 0022;

   $prog_name = $0;                    ## prog name
   $prog_name =~ s{^.*[\\/]}{}g;
   $prog_dir  = abs_path($0);          ## prog dir
   $prog_dir  =~ s{[\\/][^\\/]*$}{};
   $base_dir  = $prog_dir;             ## base dir
   $base_dir  =~ s{[\\/][^\\/]*$}{};

   unshift @INC, "$base_dir/lib";
}

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

