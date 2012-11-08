#!/usr/bin/env perl

use Test::More;

if ($^O ne 'MSWin32') {
   plan 'tests' => 8;
}
else {
   plan 'tests' => 7;
}

## Optional signals detected by MCE::Signal and not tested here are
## $SIG{XCPU} & $SIG{XFSZ}. MCE::Signal assigns signal handlers for
## the following by default.
##
ok(exists $SIG{__DIE__ }, 'Check that $SIG{__DIE__} exists');
ok(exists $SIG{__WARN__}, 'Check that $SIG{__WARN__} exists');

ok(exists $SIG{HUP }, 'Check that $SIG{HUP} exists');
ok(exists $SIG{INT }, 'Check that $SIG{INT} exists');
ok(exists $SIG{PIPE}, 'Check that $SIG{PIPE} exists');
ok(exists $SIG{QUIT}, 'Check that $SIG{QUIT} exists');
ok(exists $SIG{TERM}, 'Check that $SIG{TERM} exists');

if ($^O ne 'MSWin32') {
   ok(exists $SIG{CHLD}, 'Check that $SIG{CHLD} exists');
}

