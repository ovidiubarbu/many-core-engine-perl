#!/usr/bin/env perl

use Test::More tests => 5;

## Threads is not necessary for MCE to function properly.
## MCE will use threads & threads::shared if installed
## Otherwise, MCE tries to use forks & forks::shared
##
## The following are minimum Perl modules required by MCE

BEGIN { use_ok('Fcntl', qw( :flock O_CREAT O_TRUNC O_RDWR O_RDONLY )); }
BEGIN { use_ok('File::Path', qw( rmtree )); }
BEGIN { use_ok('Socket', qw( :DEFAULT :crlf )); }
BEGIN { use_ok('Storable', qw( store retrieve freeze thaw )); }
BEGIN { use_ok('Storable', 2.04); }

