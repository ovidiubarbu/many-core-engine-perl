
# Module makefile for MCE (using ExtUtils::MakeMaker)

use 5.008000;

use strict;
use warnings;

use ExtUtils::MakeMaker;

WriteMakefile(

    NAME          => 'MCE',
    AUTHOR        => 'Mario E. Roy <marioeroy AT gmail DOT com>',
    VERSION_FROM  => 'lib/MCE.pm',
    ABSTRACT_FROM => 'lib/MCE.pm',

    PREREQ_PM     => {
        'Fcntl'      => 0,
        'Socket'     => 0,
        'Storable'   => 2.04,
        'File::Path' => 0,
        'Test::More' => 0.45,
    },

    META_MERGE    => {
        requires  => { perl => 5.008000 },
        resources => {
            homepage   => 'http://code.google.com/p/many-core-engine-perl/',
            repository => 'http://code.google.com/p/many-core-engine-perl/',
            license    => 'http://dev.perl.org/licenses/'
        }
    },

    INSTALLDIRS   => (($] < 5.011) ? 'perl' : 'site'),

    ((ExtUtils::MakeMaker->VERSION() lt '6.25') ?
        (PL_FILES => { })            : ()),
    ((ExtUtils::MakeMaker->VERSION() gt '6.30') ?
        (LICENSE  => 'perl')         : ()),
);

