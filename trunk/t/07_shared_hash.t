#!/usr/bin/env perl

use strict;
use warnings;

use Test::More tests => 9;
use MCE::Flow max_workers => 1;
use MCE::Shared;

mce_share my %h1 => (k1 => 10, k2 => '', k3 => '');
mce_share my $keys;
mce_share my $e1;
mce_share my $e2;
mce_share my $d1;
mce_share my $s1;

###############################################################################

MCE::Flow::run( sub {
   $h1{k1} +=  1;  $h1{k1} += 4;
   $h1{k2} .= '';  $h1{k2} .= '';
   $h1{k3} .= 10;  $h1{k3} .= 'string';
   $keys    = join(' ', sort keys %h1);
});

MCE::Flow::finish;

is( $h1{k1}, 15, 'shared hash, check fetch, store' );
is( $h1{k2}, '', 'shared hash, check blank value' );
is( $h1{k3}, '10string', 'shared hash, check concatenation' );
is( $keys, 'k1 k2 k3', 'shared hash, check firstkey, nextkey' );

###############################################################################

MCE::Flow::run( sub {
   $e1 = exists $h1{'k2'} ? 1 : 0;
   $d1 = delete $h1{'k2'};
   $e2 = exists $h1{'k2'} ? 1 : 0;
   %h1 = (); $s1 = scalar %h1;
   $h1{ret} = [ 'wind', 'air' ];
});

MCE::Flow::finish;

is( $e1,  1, 'shared hash, check exists before delete' );
is( $d1, '', 'shared hash, check delete' );
is( $e2,  0, 'shared hash, check exists after delete' );
is( $s1,  0, 'shared hash, check clear' );
is( $h1{ret}->[1], 'air', 'shared hash, check auto freeze/thaw' );

