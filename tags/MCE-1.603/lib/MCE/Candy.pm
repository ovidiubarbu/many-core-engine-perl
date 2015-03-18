###############################################################################
## ----------------------------------------------------------------------------
## MCE::Candy - Sugar methods for Many-Core Engine.
##
###############################################################################

package MCE::Candy;

use strict;
use warnings;

our $VERSION  = '1.603';

our @CARP_NOT = qw( MCE );

no warnings 'threads';
no warnings 'recursion';
no warnings 'uninitialized';

use bytes;

###############################################################################
## ----------------------------------------------------------------------------
## Forchunk, foreach, and forseq sugar methods.
##
###############################################################################

sub forchunk {

   my $x = shift; my $self = ref($x) ? $x : $MCE::MCE;
   my $_input_data = $_[0];

   MCE::_validate_runstate($self, 'MCE::forchunk');

   my ($_user_func, $_params_ref);

   if (ref $_[1] eq 'HASH') {
      $_user_func = $_[2]; $_params_ref = $_[1];
   } else {
      $_user_func = $_[1]; $_params_ref = {};
   }

   @_ = ();

   MCE::_croak('MCE::forchunk: (input_data) is not specified')
      unless (defined $_input_data);
   MCE::_croak('MCE::forchunk: (code_block) is not specified')
      unless (defined $_user_func);

   $_params_ref->{input_data} = $_input_data;
   $_params_ref->{user_func}  = $_user_func;

   $self->run(1, $_params_ref);

   return $self;
}

sub foreach {

   my $x = shift; my $self = ref($x) ? $x : $MCE::MCE;
   my $_input_data = $_[0];

   MCE::_validate_runstate($self, 'MCE::foreach');

   my ($_user_func, $_params_ref);

   if (ref $_[1] eq 'HASH') {
      $_user_func = $_[2]; $_params_ref = $_[1];
   } else {
      $_user_func = $_[1]; $_params_ref = {};
   }

   @_ = ();

   MCE::_croak('MCE::foreach: (input_data) is not specified')
      unless (defined $_input_data);
   MCE::_croak('MCE::foreach: (code_block) is not specified')
      unless (defined $_user_func);

   $_params_ref->{chunk_size} = 1;
   $_params_ref->{input_data} = $_input_data;
   $_params_ref->{user_func}  = $_user_func;

   $self->run(1, $_params_ref);

   return $self;
}

sub forseq {

   my $x = shift; my $self = ref($x) ? $x : $MCE::MCE;
   my $_sequence = $_[0];

   MCE::_validate_runstate($self, 'MCE::forseq');

   my ($_user_func, $_params_ref);

   if (ref $_[1] eq 'HASH') {
      $_user_func = $_[2]; $_params_ref = $_[1];
   } else {
      $_user_func = $_[1]; $_params_ref = {};
   }

   @_ = ();

   MCE::_croak('MCE::forseq: (sequence) is not specified')
      unless (defined $_sequence);
   MCE::_croak('MCE::forseq: (code_block) is not specified')
      unless (defined $_user_func);

   $_params_ref->{sequence}   = $_sequence;
   $_params_ref->{user_func}  = $_user_func;

   $self->run(1, $_params_ref);

   return $self;
}

1;

__END__

###############################################################################
## ----------------------------------------------------------------------------
## Module usage.
##
###############################################################################

=head1 NAME

MCE::Candy - Sugar methods for Many-Core Engine

=head1 VERSION

This document describes MCE::Candy version 1.603

=head1 DESCRIPTION

This module provides a collection of sugar methods created for MCE.

=head1 "FOR" SUGAR METHODS

The methods described below were created prior to the 1.5 release which added
MCE Models. This module is loaded automatically upon calling a "for" method.

=head2 $mce->forchunk ( $input_data [, { options } ], sub { ... } )

Forchunk, foreach, and forseq are sugar methods in MCE. Workers are
automatically spawned, the code block is executed in parallel, and shutdown
is called. Do not use these methods if workers must persist afterwards.

Specifying options is optional. Valid options are the same as for the
process method.

   ## Declare a MCE instance.

   my $mce = MCE->new(
      chunk_size  => 20,
      max_workers => $max_workers
   );

   ## Arguments inside the code block are the same as for user_func.

   $mce->forchunk(\@input_data, sub {
      my ($mce, $chunk_ref, $chunk_id) = @_;

      foreach ( @{ $chunk_ref } ) {
         MCE->print("$chunk_id: $_\n");
      }
   });

   ## Passing chunk_size as an option.

   $mce->forchunk(\@input_data, { chunk_size => 30 }, sub {
      ...
   });

=head2 $mce->foreach ( $input_data [, { options } ], sub { ... } )

Foreach implies chunk_size => 1 and cannot be overwritten. Arguments inside
the code block are the same as for user_func. This is true even if chunk_size
is set to 1. MCE is both a chunking engine plus a parallel engine all in one.
Arguments within the block are the same whether calling foreach or forchunk.

   my $mce = MCE->new(
      max_workers => $max_workers
   );

   $mce->foreach(\@input_data, sub {
      my ($mce, $chunk_ref, $chunk_id) = @_;
      my $row = $chunk_ref->[0];
      MCE->print("$chunk_id: $row\n");
   });

Below, passing an anonymous array as input data. For example, wanting to
parallelize a serial loop with MCE.

   ## Serial loops.

   for (my $i = 0; $i < $max; $i++) {
      ...  ## Runs serially
   }

   for my $i (0 .. $max - 1) {
      ...  ## Runs serially
   }

   ## Parallel loop via MCE.

   $mce->foreach([ (0 .. $max - 1) ], sub {
      my ($mce, $chunk_ref, $chunk_id) = @_;
      my $i = $chunk_ref->[0];
      ...  ## Runs in parallel
   });

=head2 $mce->forseq ( $sequence_spec [, { options } ], sub { ... } )

Sequence can be defined using an array or hash reference.

   my $mce = MCE->new(
      max_workers => 3
   );

   $mce->forseq([ 20, 40 ], sub {
      my ($mce, $n, $chunk_id) = @_;
      my $result = `ping 192.168.1.${n}`;
      ...
   });

   $mce->forseq({ begin => 15, end => 10, step => -1 }, sub {
      my ($mce, $n, $chunk_id) = @_;
      print $n, " from ", MCE->wid, "\n";
   });

When chunking, the $n_seq variable is a reference pointing to an array of
sequences. Chunking reduces IPC overhead behind the scene. Chunk size
defaults to 1 when not specified.

   $mce->forseq([ 20, 80 ], { chunk_size => 10 }, sub {
      my ($mce, $n_seq, $chunk_id) = @_;
      for my $n ( @{ $n_seq } ) {
         my $result = `ping 192.168.1.${n}`;
         ...
      }
   });

=head1 INDEX

L<MCE|MCE>

=head1 AUTHOR

Mario E. Roy, S<E<lt>marioeroy AT gmail DOT comE<gt>>

=cut

