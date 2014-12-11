#!/usr/bin/env perl
###############################################################################
## ----------------------------------------------------------------------------
## Cat script similar to the cat binary.
##
## The logic below only supports -n -u options. The focus is demonstrating
## Many-Core Engine for Perl.
##
## This script was created to show how order can be preserved even though there
## are only 4 shared socket pairs in MCE no matter the number of workers.
##
## Try running with -n option against a large file with long lines. This
## script will out-perform the cat binary in that case.
##
## The usage description was largely ripped off from the cat man page.
##
###############################################################################

use strict;
use warnings;

use Cwd 'abs_path';  ## Remove taintedness from path
use lib ($_) = (abs_path().'/../lib') =~ /(.*)/;

my $prog_name = $0; $prog_name =~ s{^.*[\\/]}{}g;

sub INIT {
   ## Provide file globbing support under Windows similar to Unix.
   @ARGV = <@ARGV> if ($^O eq 'MSWin32');
}

use MCE;

###############################################################################
## ----------------------------------------------------------------------------
## Display usage and exit.
##
###############################################################################

sub usage {

   print <<"::_USAGE_BLOCK_END_::";

NAME
   $prog_name -- concatenate and print files

SYNOPSIS
   $prog_name [-nu] [file ...]

DESCRIPTION
   The $prog_name utility reads files sequentially, writing them to the
   standard output. The file operands are processed in command-line
   order. If file is a single dash ('-') or absent, $prog_name reads
   the standard input.

   The following options are available:

   --max-workers MAX_WORKERS
          Specify number of workers for MCE   -- default: 3

   --chunk-size CHUNK_SIZE
          Specify chunk size for MCE          -- default: 500K

   -n     Number the output lines, starting at 1
   -u     Disable output buffering

EXIT STATUS
   The $prog_name utility exits 0 on success, and >0 if an error occurs.

EXAMPLES
   The command:

         $prog_name file1

   will print the contents of file1 to the standard output.

   The command:

         $prog_name file1 file2 > file3

   will sequentially print the contents of file1 and file2 to the file
   file3, truncating file3 if it already exists.

   The command:

         $prog_name file1 - file2 - file3

   will print the contents of file1, print data it receives from the stan-
   dard input until it receives an EOF (typing 'Ctrl/Z' in Windows, 'Ctrl/D'
   in UNIX), print the contents of file2, read and output contents of the
   standard input again, then finally output the contents of file3.

::_USAGE_BLOCK_END_::

   exit 1
}

###############################################################################
## ----------------------------------------------------------------------------
## Define defaults and process command-line arguments.
##
###############################################################################

my $flag = sub { 1; };
my $isOk = sub { (@ARGV == 0 or $ARGV[0] =~ /^-/) ? usage() : shift @ARGV; };

my $chunk_size  = '500K';
my $max_workers = 3;
my $skip_args   = 0;

my $n_flag = 0;
my $u_flag = 0;

my @files = ();

while ( my $arg = shift @ARGV ) {
   unless ($skip_args) {
      if ($arg eq '-') {
         push @files, $arg;
         next;
      }
      if ($arg =~ m/^-[nu]+$/) {
         while ($arg) {
            my $a = chop($arg);
            $n_flag = $flag->() and next if ($a eq 'n');
            $u_flag = $flag->() and next if ($a eq 'u');
         }
         next;
      }

      $skip_args   = $flag->() and next if ($arg eq '--');
      $max_workers = $isOk->() and next if ($arg =~ /^--max[-_]workers$/);
      $chunk_size  = $isOk->() and next if ($arg =~ /^--chunk[-_]size$/);

      if ($arg =~ /^--max[-_]workers=(.+)/) {
         $max_workers = $1;
         next;
      }
      if ($arg =~ /^--chunk[-_]size=(.+)/) {
         $chunk_size = $1;
         next;
      }

      usage() if ($arg =~ /^-/);
   }

   push @files, $arg;
}

###############################################################################
## ----------------------------------------------------------------------------
## Launch Many-Core Engine.
##
###############################################################################

my $mce = MCE->new(

   chunk_size  => $chunk_size, max_workers => $max_workers,
   use_slurpio => 1,

   user_func => sub {
      my ($mce, $chunk_ref, $chunk_id) = @_;

      MCE->do('display_chunk', $$chunk_ref .':'. $chunk_id);

      return;
   }

)->spawn;

local $| = 1 if $u_flag;

###############################################################################
## ----------------------------------------------------------------------------
## Concatenate and print files
##
###############################################################################

my ($order_id, $lines, %result);
my $exit_status = 0;

sub display_chunk {

   my $chunk_id = substr($_[0], rindex($_[0], ':') + 1);

   chop $_[0] for (1 .. length($chunk_id) + 1);

   $result{$chunk_id} = $_[0];

   if ($n_flag) {
      while (1) {
         last unless exists $result{$order_id};

         open my $fh, '<', \$result{$order_id};
         printf "%6d\t%s", ++$lines, $_ while (<$fh>);
         close $fh;

         delete $result{$order_id};
         $order_id++;
      }
   }
   else {
      while (1) {
         last unless exists $result{$order_id};

         print $result{$order_id};
         delete $result{$order_id};
         $order_id++;
      }
   }

   return;
}

## Process files, otherwise read from standard input.

if (@files > 0) {
   foreach my $file (@files) {
      $order_id = 1; $lines = 0;
      if ($file eq '-') {
         open(STDIN, '<', ($^O eq 'MSWin32') ? 'CON' : '/dev/tty') or die $!;
         $mce->process(\*STDIN);
      }
      elsif (! -e $file) {
         print STDERR "$prog_name: $file: No such file or directory\n";
         $exit_status = 2;
      }
      elsif (-d $file) {
         print STDERR "$prog_name: $file: Is a directory\n";
         $exit_status = 1;
      }
      else {
         $mce->process($file);
      }
   }
}
else {
   $order_id = 1; $lines = 0;
   $mce->process(\*STDIN);
}

## Shutdown Many-Core Engine and exit.

$mce->shutdown();
exit $exit_status;

