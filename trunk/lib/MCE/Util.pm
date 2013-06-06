###############################################################################
## ----------------------------------------------------------------------------
## MCE::Util
## -- Provides utility functions for Many-Core Engine.
##
###############################################################################

package MCE::Util;

use strict;
use warnings;

use base qw( Exporter );

our $VERSION = '1.000'; $VERSION = eval $VERSION;

our @EXPORT_OK = qw( get_ncpu );
our %EXPORT_TAGS = ( all => \@EXPORT_OK );

###############################################################################
## ----------------------------------------------------------------------------
## The get_ncpu subroutine (largely borrowed from Test::Smoke::Util.pm)
## returns the number of available (online/active/enabled) CPUs.
##
## Defaults to 1. A warning is emitted to STDERR when it cannot recognize
## your operating system or the external command failed.
##
###############################################################################

sub get_ncpu {

   local $ENV{PATH} = "/usr/sbin:/sbin:/usr/bin:/bin:$ENV{PATH}";

   my $cpus = 1;

   OS_CHECK: {
      local $_ = $^O;

      /linux/i && do {
         my @output; local *PROC;
         if ( open PROC, "< /proc/stat" ) {
             @output = grep /^cpu\d/ => <PROC>;
             close PROC;
         }
         $cpus = scalar @output if @output;
         last OS_CHECK;
      };

      /(?:darwin|.*bsd)/i && do {
         chomp( my @output = `sysctl -n hw.ncpu 2>/dev/null` );
         $cpus = $output[0] if @output;
         last OS_CHECK;
      };

      /aix/i && do {
         my @output = `lsdev -C -c processor -S Available 2>/dev/null`;
         $cpus = scalar @output if @output;
         last OS_CHECK;
      };

      /hp-?ux/i && do {
         my @output = grep /^processor/ => `ioscan -fnkC processor 2>/dev/null`;
         $cpus = scalar @output if @output;
         last OS_CHECK;
      };

      /irix/i && do {
         my @output = grep /\s+processors?$/i => `hinv -c processor 2>/dev/null`;
         $cpus = (split " ", $output[0])[0] if @output;
         last OS_CHECK;
      };

      /solaris|sunos|osf/i && do {
         my @output = grep /on-line/ => `psrinfo 2>/dev/null`;
         $cpus = scalar @output if @output;
         last OS_CHECK;
      };

      /mswin32|cygwin/i && do {
         $cpus = $ENV{NUMBER_OF_PROCESSORS}
            if exists $ENV{NUMBER_OF_PROCESSORS};
         last OS_CHECK;
      };

      require Carp;
      Carp::croak(
         "MCE::Util::get_ncpu: command failed or unknown operating system\n"
      );
   }

   return $cpus;
}

1;

__END__

###############################################################################
## ----------------------------------------------------------------------------
## Module usage.
##
###############################################################################

=head1 NAME

MCE::Util - Provides utility functions for Many-Core Engine.

=head1 VERSION

This document describes MCE::Util version 1.000

=head1 SYNOPSIS

 use MCE::Util;

=head1 DESCRIPTION

Utility module for MCE. Nothing is exported by default. Exportable is get_ncpu.

=head2 get_ncpu()

Returns the number of available (online/active/enabled) CPUs.

 my $cpus = MCE::Util::get_ncpu();

Specifying 'auto' for max_workers calls MCE::Util::get_ncpu automatically.

 use MCE;

 my $mce = MCE->new(
   max_workers => 'auto-1',        ## MCE::Util::get_ncpu() - 1
   max_workers => 'auto+3',        ## MCE::Util::get_ncpu() + 3
   max_workers => 'auto',          ## MCE::Util::get_ncpu()
 );

=head1 ACKNOWLEDGEMENTS

The portable code for detecting the number of processors was borrowed from
L<Test::Smoke::SysInfo>.

=head1 AUTHOR

Mario E. Roy, S<E<lt>marioeroy AT gmail DOT comE<gt>>

=head1 COPYRIGHT AND LICENSE

Copyright (C) 2013 by Mario E. Roy

This program is free software; you can redistribute it and/or modify it
under the terms of either: the GNU General Public License as published
by the Free Software Foundation; or the Artistic License.

See L<http://dev.perl.org/licenses/> for more information.

=cut
