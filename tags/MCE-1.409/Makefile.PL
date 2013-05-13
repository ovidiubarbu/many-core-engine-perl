
# Module makefile for MCE (using ExtUtils::MakeMaker)

use 5.008;

use strict;
use warnings;

use ExtUtils::MakeMaker;

WriteMakefile(

   ABSTRACT       => 'Many-Core Engine for Perl. Provides parallel processing capabilities.',

   AUTHOR         => 'Mario E. Roy <marioeroy AT gmail DOT com>',

   NAME           => 'MCE',

   PREREQ_PM => {
      'Fcntl'      => 0,
      'File::Path' => 0,
      'Socket'     => 0,
      'Storable'   => 2.04
   },

   VERSION        => '1.409',

   (($ExtUtils::MakeMaker::VERSION lt '6.25') ?
      (PL_FILES => { }) : ()),

   (($ExtUtils::MakeMaker::VERSION ge '6.30') ?
      (LICENSE => 'perl') : ()),

   (($ExtUtils::MakeMaker::VERSION ge '6.46') ?
      (META_MERGE => {
         build_requires => {
            'Pod::Coverage'       => 0,
            'Test::More'          => 0.45,
            'Test::Pod'           => 0,
            'Test::Pod::Coverage' => 0
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
         },
         provides => {
            'MCE' => {
               'file'    => 'lib/MCE.pm',
               'version' => '1.409'
            },
            'MCE::Signal' => {
               'file'    => 'lib/MCE/Signal.pm',
               'version' => '1.409'
            }
         }
      }) : ()),

   INSTALLDIRS => (($] < 5.011) ? 'perl' : 'site')
);

