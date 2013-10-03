###############################################################################
## ----------------------------------------------------------------------------
## MCE - Many-core Engine for Perl.
##
###############################################################################

package MCE;

use strict;
use warnings;

use MCE::Core;

our $VERSION = '1.499_002'; $VERSION = eval $VERSION;

###############################################################################
## ----------------------------------------------------------------------------
## Import routine.
##
###############################################################################

use constant { SELF => 0, CHUNK => 1, CID => 2 };

my $_loaded;

sub import {

   my $class = shift; return if ($_loaded++);

   ## Process module arguments.
   while (my $_arg = shift) {

      $MCE::MAX_WORKERS = shift and next if ( $_arg =~ /^max_workers$/i );
      $MCE::CHUNK_SIZE  = shift and next if ( $_arg =~ /^chunk_size$/i );
      $MCE::TMP_DIR     = shift and next if ( $_arg =~ /^tmp_dir$/i );
      $MCE::FREEZE      = shift and next if ( $_arg =~ /^freeze$/i );
      $MCE::THAW        = shift and next if ( $_arg =~ /^thaw$/i );

      if ( $_arg =~ /^sereal$/i ) {
         if (shift eq '1') {
            local $@; eval 'use Sereal qw(encode_sereal decode_sereal)';
            unless ($@) {
               $MCE::FREEZE = \&encode_sereal;
               $MCE::THAW   = \&decode_sereal;
            }
         }
         next;
      }

      if ( $_arg =~ /^(?:export_const|const)$/i ) {
         if (shift eq '1') {
            no strict 'refs'; no warnings 'redefine';
            my $_package = caller();
            *{ $_package . '::SELF'  } = \&SELF;
            *{ $_package . '::CHUNK' } = \&CHUNK;
            *{ $_package . '::CID'   } = \&CID;
         }
         next;
      }

      _croak("MCE::import: '$_arg' is not a valid module argument");
   }

   return;
}

1;

__END__

###############################################################################
## ----------------------------------------------------------------------------
## Module usage.
##
###############################################################################

=head1 NAME

MCE - Many-core Engine for Perl. Provides parallel processing capabilities.

=head1 VERSION

This document describes MCE version 1.499_002

=head1 DESCRIPTION

Many-core Engine (MCE) for Perl helps enable a new level of performance by
maximizing all available cores. MCE spawns a pool of workers and therefore
does not fork a new process per each element of data. Instead, MCE follows
a bank queuing model. Imagine the line being the data and bank-tellers the
parallel workers. MCE enhances that model by adding the ability to chunk
the next n elements from the input stream to the next available worker.

Chunking and input data are optional in MCE. One can simply use MCE to have
many workers run in parallel.

=head1 CORE MODULES

The core engine is made up of 3 modules. New to the 1.5 release is the
inclusion of the gather and time interval features. In addition, the core
has a faster IPC engine when compared to prior releases.

=over 5

=item L<MCE::Core>

Provides the core API for Many-core Engine.

=item L<MCE::Signal>

Temporary directory creation/cleanup & signal handling.

=item L<MCE::Util>

Public and private utility functions.

=back

=head1 MCE ADDONS

=over 5

=item L<MCE::Queue>

Provides a hybrid queuing implementation for MCE supporting normal queues and
priority queues in a single module. This module exchanges data via the core
engine to enable queues to work for both children (spawned via fork) and
threads.

=item L<MCE::Subs>

Exports funtions mapped directly to MCE's methods, e.g. mce_wid. The module
allows 3 options, e.g. :manager, :worker, :getter.

=back

=head1 MCE MODELS

MCE 1.5 introduces 5 models for even more automation behind the scene. The
models configure MCE, spawn plus shutdown workers automatically.

The models take Many-core Engine to a new level for ease of use.

=over 5

=item L<MCE::Flow>

A parallel flow model for building creative applications. This makes use of
User Tasks in MCE. The author has full control when utilizing this model.

=item L<MCE::Grep>

Provides a parallel grep implementation similar to the native grep function.

=item L<MCE::Loop>

Provides a parallel loop utilizing MCE for building creative loops.

=item L<MCE::Map>

Provides a parallel map model similar to the native map function.

=item L<MCE::Stream>

Provides an efficient parallel implementation for chaining multiple maps
and greps together through the use of User Tasks and MCE::Queue.

=back

=head1 EXAMPLES

A place-holder for examples included with the distribution.

=over 5

=item L<MCE::Examples>

=back

=head1 REQUIREMENTS

Perl 5.8.0 or later. PDL::IO::Storable is required in scripts running PDL
and using MCE for parallelization.

=head1 SOURCE

The source is hosted at L<http://code.google.com/p/many-core-engine-perl/>

=head1 AUTHOR

Mario E. Roy, S<E<lt>marioeroy AT gmail DOT comE<gt>>

=head1 COPYRIGHT AND LICENSE

Copyright (C) 2012-2013 by Mario E. Roy

This program is free software; you can redistribute it and/or modify it
under the terms of either: the GNU General Public License as published
by the Free Software Foundation; or the Artistic License.

See L<http://dev.perl.org/licenses/> for more information.

=cut

