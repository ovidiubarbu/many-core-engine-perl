#!/usr/bin/env perl

use strict;
use warnings;

use Test::More tests => 3;
use MCE::Flow max_workers => 1;
use MCE::Shared;

my ($s1, $s2, $s3) = (10, '', '');

mce_share \$s1, \$s2, \$s3;

###############################################################################

MCE::Flow::run( sub {
   $s1 +=  1; tied($s1)->add(4);
   $s2 .= ''; tied($s2)->concat('');
   $s3 .= 10; tied($s3)->concat('string');
});

MCE::Flow::finish;

is( $s1, 15, 'shared scalar, check fetch, store' );
is( $s2, '', 'shared scalar, check blank value' );
is( $s3, '10string', 'shared scalar, check concatenation' );

