
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
      'Scalar::Util' => 0,
      'Socket'       => 0,
      'Storable'     => 2.04,
      'Symbol'       => 0,
      'Time::HiRes'  => 0
   },

   VERSION        => '1.501',

   (($ExtUtils::MakeMaker::VERSION lt '6.25') ?
      (PL_FILES => { }) : ()),

   (($ExtUtils::MakeMaker::VERSION ge '6.30') ?
      (LICENSE => 'perl') : ()),

   (($ExtUtils::MakeMaker::VERSION ge '6.46') ?
      (META_MERGE => {
         build_requires => {
            'ExtUtils::MakeMaker' => 0,
            'Test::More'          => 0.45
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
               'version' => '1.501'
            },
            'MCE::Core::Input::Generator' => {
               'file'    => 'lib/MCE/Core/Input/Generator.pm',
               'version' => '1.501'
            },
            'MCE::Core::Input::Handle' => {
               'file'    => 'lib/MCE/Core/Input/Handle.pm',
               'version' => '1.501'
            },
            'MCE::Core::Input::Request' => {
               'file'    => 'lib/MCE/Core/Input/Request.pm',
               'version' => '1.501'
            },
            'MCE::Core::Input::Sequence' => {
               'file'    => 'lib/MCE/Core/Input/Sequence.pm',
               'version' => '1.501'
            },
            'MCE::Core::Manager' => {
               'file'    => 'lib/MCE/Core/Manager.pm',
               'version' => '1.501'
            },
            'MCE::Core::Validation' => {
               'file'    => 'lib/MCE/Core/Validation.pm',
               'version' => '1.501'
            },
            'MCE::Core::Worker' => {
               'file'    => 'lib/MCE/Core/Worker.pm',
               'version' => '1.501'
            },
            'MCE::Flow' => {
               'file'    => 'lib/MCE/Flow.pm',
               'version' => '1.501'
            },
            'MCE::Grep' => {
               'file'    => 'lib/MCE/Grep.pm',
               'version' => '1.501'
            },
            'MCE::Loop' => {
               'file'    => 'lib/MCE/Loop.pm',
               'version' => '1.501'
            },
            'MCE::Map' => {
               'file'    => 'lib/MCE/Map.pm',
               'version' => '1.501'
            },
            'MCE::Queue' => {
               'file'    => 'lib/MCE/Queue.pm',
               'version' => '1.501'
            },
            'MCE::Signal' => {
               'file'    => 'lib/MCE/Signal.pm',
               'version' => '1.501'
            },
            'MCE::Stream' => {
               'file'    => 'lib/MCE/Stream.pm',
               'version' => '1.501'
            },
            'MCE::Subs' => {
               'file'    => 'lib/MCE/Subs.pm',
               'version' => '1.501'
            },
            'MCE::Util' => {
               'file'    => 'lib/MCE/Util.pm',
               'version' => '1.501'
            }
         }
      }) : ()),

   INSTALLDIRS => (($] < 5.011) ? 'perl' : 'site')
);

