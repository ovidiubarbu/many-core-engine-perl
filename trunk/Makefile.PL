
# Module makefile for MCE (using ExtUtils::MakeMaker)

use 5.008;

use strict;
use warnings;

use ExtUtils::MakeMaker;

WriteMakefile(

   ABSTRACT       => 'Many-core Engine for Perl. Provides parallel processing capabilities.',

   AUTHOR         => 'Mario E. Roy <marioeroy AT gmail DOT com>',

   NAME           => 'MCE',

   PREREQ_PM => {
      'Fcntl'        => 0,
      'File::Path'   => 0,
      'IPC::Open2'   => 0,
      'Scalar::Util' => 0,
      'Socket'       => 0,
      'Storable'     => 2.04,
      'Symbol'       => 0,
      'Time::HiRes'  => 0
   },

   VERSION        => '1.499_003',

   (($ExtUtils::MakeMaker::VERSION lt '6.25') ?
      (PL_FILES => { }) : ()),

   (($ExtUtils::MakeMaker::VERSION ge '6.30') ?
      (LICENSE => 'perl') : ()),

   (($ExtUtils::MakeMaker::VERSION ge '6.46') ?
      (META_MERGE => {
         build_requires => {
            'Test::More' => 0.45
         },
         no_index => {
            'directory' => [
               'examples',
               'images'
            ]
         },
         requires => {
            'perl' => 5.008
         },
         resources => {
            'homepage'   => 'http://code.google.com/p/many-core-engine-perl/',
            'repository' => 'http://code.google.com/p/many-core-engine-perl/',
            'license'    => 'http://dev.perl.org/licenses/'
         }
      }) : ()),

   INSTALLDIRS => (($] < 5.011) ? 'perl' : 'site')
);

