###############################################################################
## ----------------------------------------------------------------------------
## MCE::Core::Validation - Core validation methods for Many-core Engine.
##
## This package provides validation methods used internally by the manager
## process.
##
## There is no public API.
##
###############################################################################

package MCE::Core::Validation;

our $VERSION = '1.509'; $VERSION = eval $VERSION;

## Items below are folded into MCE.

package MCE;

use strict;
use warnings;

## Warnings are disabled to minimize bits of noise when user or OS signals
## the script to exit. e.g. MCE_script.pl < infile | head

no warnings 'threads'; no warnings 'uninitialized';

###############################################################################
## ----------------------------------------------------------------------------
## Validation method (attributes allowed for top-level).
##
###############################################################################

sub _validate_args {

   my MCE $_s = $_[0];

   die "Private method called" unless (caller)[0]->isa( ref($_s) );

   my $_tag = 'MCE::_validate_args';

   if (defined $_s->{input_data} && ref $_s->{input_data} eq '') {
      _croak("$_tag: '$_s->{input_data}' does not exist")
         unless (-e $_s->{input_data});
   }

   _croak("$_tag: 'use_slurpio' is not 0 or 1")
      if ($_s->{use_slurpio} && $_s->{use_slurpio} !~ /\A[01]\z/);
   _croak("$_tag: 'job_delay' is not valid")
      if ($_s->{job_delay} && $_s->{job_delay} !~ /\A[\d\.]+\z/);
   _croak("$_tag: 'spawn_delay' is not valid")
      if ($_s->{spawn_delay} && $_s->{spawn_delay} !~ /\A[\d\.]+\z/);
   _croak("$_tag: 'submit_delay' is not valid")
      if ($_s->{submit_delay} && $_s->{submit_delay} !~ /\A[\d\.]+\z/);

   _croak("$_tag: 'freeze' is not a CODE reference")
      if ($_s->{freeze} && ref $_s->{freeze} ne 'CODE');
   _croak("$_tag: 'thaw' is not a CODE reference")
      if ($_s->{thaw} && ref $_s->{thaw} ne 'CODE');

   _croak("$_tag: 'on_post_exit' is not a CODE reference")
      if ($_s->{on_post_exit} && ref $_s->{on_post_exit} ne 'CODE');
   _croak("$_tag: 'on_post_run' is not a CODE reference")
      if ($_s->{on_post_run} && ref $_s->{on_post_run} ne 'CODE');
   _croak("$_tag: 'user_error' is not a CODE reference")
      if ($_s->{user_error} && ref $_s->{user_error} ne 'CODE');
   _croak("$_tag: 'user_output' is not a CODE reference")
      if ($_s->{user_output} && ref $_s->{user_output} ne 'CODE');

   _croak("$_tag: 'flush_file' is not 0 or 1")
      if ($_s->{flush_file} && $_s->{flush_file} !~ /\A[01]\z/);
   _croak("$_tag: 'flush_stderr' is not 0 or 1")
      if ($_s->{flush_stderr} && $_s->{flush_stderr} !~ /\A[01]\z/);
   _croak("$_tag: 'flush_stdout' is not 0 or 1")
      if ($_s->{flush_stdout} && $_s->{flush_stdout} !~ /\A[01]\z/);

   $_s->_validate_args_s();

   if (defined $_s->{user_tasks}) {
      for my $_t (@{ $_s->{user_tasks} }) {
         $_s->_validate_args_s($_t);

         _croak("$_tag: 'task_end' is not a CODE reference")
            if ($_t->{task_end} && ref $_t->{task_end} ne 'CODE');
      }
   }

   return;
}

###############################################################################
## ----------------------------------------------------------------------------
## Validation method (top-level and sub-tasks).
##
###############################################################################

sub _validate_args_s {

   my MCE $self = $_[0];
   my $_s       = $_[1] || $self;

   @_ = ();

   die "Private method called" unless (caller)[0]->isa( ref($self) );

   my $_tag = 'MCE::_validate_args_s';

   _parse_max_workers($_s);

   _croak("$_tag: 'max_workers' is not valid")
      if (defined $_s->{max_workers} && (
         $_s->{max_workers} !~ /\A\d+\z/
      ));

   if ($_s->{chunk_size} =~ /([0-9\.]+)K\z/i) {
      $_s->{chunk_size} = int($1 * 1024 + 0.5);
   }
   elsif ($_s->{chunk_size} =~ /([0-9\.]+)M\z/i) {
      $_s->{chunk_size} = int($1 * 1024 * 1024 + 0.5);
   }

   _croak("$_tag: 'chunk_size' is not valid")
      if (defined $_s->{chunk_size} && (
         $_s->{chunk_size} !~ /\A\d+\z/ or $_s->{chunk_size} == 0
      ));

   _croak("$_tag: 'RS' is not valid")
      if ($_s->{RS} && ref $_s->{RS} ne '');
   _croak("$_tag: 'use_threads' is not 0 or 1")
      if ($_s->{use_threads} && $_s->{use_threads} !~ /\A[01]\z/);
   _croak("$_tag: 'user_begin' is not a CODE reference")
      if ($_s->{user_begin} && ref $_s->{user_begin} ne 'CODE');
   _croak("$_tag: 'user_func' is not a CODE reference")
      if ($_s->{user_func} && ref $_s->{user_func} ne 'CODE');
   _croak("$_tag: 'user_end' is not a CODE reference")
      if ($_s->{user_end} && ref $_s->{user_end} ne 'CODE');

   if (defined $_s->{gather}) {
      my $_ref = ref $_s->{gather};
      _croak("$_tag: 'gather' is not a valid reference")
         if ( $_ref ne 'MCE::Queue' && $_ref ne 'Thread::Queue' &&
              $_ref ne 'ARRAY' && $_ref ne 'HASH' && $_ref ne 'CODE' );
   }

   if (defined $_s->{sequence}) {
      my $_seq = $_s->{sequence};

      if (ref $_seq eq 'ARRAY') {
         my ($_begin, $_end, $_step, $_fmt) = @{ $_seq };
         $_seq = {
            begin => $_begin, end => $_end, step => $_step, format => $_fmt
         };
      }
      else {
         _croak("$_tag: 'sequence' is not a HASH or ARRAY reference")
            if (ref $_seq ne 'HASH');
      }

      _croak("$_tag: 'begin' is not defined for sequence")
         unless (defined $_seq->{begin});
      _croak("$_tag: 'end' is not defined for sequence")
         unless (defined $_seq->{end});

      for (qw(begin end step)) {
         _croak("$_tag: '$_' is not valid for sequence")
            if (defined $_seq->{$_} && (
               $_seq->{$_} eq '' || $_seq->{$_} !~ /\A-?\d*\.?\d*\z/
            ));
      }

      unless (defined $_seq->{step}) {
         $_seq->{step} = ($_seq->{begin} < $_seq->{end}) ? 1 : -1;
         if (ref $_s->{sequence} eq 'ARRAY') {
            $_s->{sequence}->[2] = $_seq->{step};
         }
      }
      if ( ($_seq->{step} < 0 && $_seq->{begin} < $_seq->{end}) ||
           ($_seq->{step} > 0 && $_seq->{begin} > $_seq->{end}) ||
           ($_seq->{step} == 0)
      ) {
         _croak("$_tag: impossible 'step' size for sequence");
      }
   }

   if (defined $_s->{interval}) {
      $_s->{interval} = { delay => $_s->{interval} }
         if (ref $_s->{interval} eq '');

      my $_i = $_s->{interval};

      _croak("$_tag: 'interval' is not a HASH reference")
         if (ref $_i ne 'HASH');
      _croak("$_tag: 'delay' is not defined for interval")
         unless (defined $_i->{delay});

      for (qw(delay max_nodes node_id)) {
         _croak("$_tag: '$_' is not valid for interval")
            if (defined $_i->{$_} && (
               $_i->{$_} eq '' ||
               $_i->{$_} !~ /\A\-?\d*(?:e\-|\.)?\d*\z/ ||
               $_i->{$_} == 0 || $_i->{$_} < 0
            ));
      }
      $_i->{max_nodes} = 1 unless (exists $_i->{max_nodes});
      $_i->{node_id}   = 1 unless (exists $_i->{node_id});
      $_i->{_time}     = time();
   }

   return;
}

###############################################################################
## ----------------------------------------------------------------------------
## Validation method (run state).
##
###############################################################################

sub _validate_runstate {

   my MCE $self = $_[0]; my $_tag = $_[1];

   _croak("$_tag: method cannot be called by the worker process")
      if ($self->{_wid});
   _croak("$_tag: method cannot be called while processing")
      if ($self->{_send_cnt});
   _croak("$_tag: method cannot be called while running")
      if ($self->{_total_running});

   return;
}

1;

