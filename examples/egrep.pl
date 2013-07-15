#!/usr/bin/env perl
###############################################################################
## ----------------------------------------------------------------------------
## Egrep script similar to the egrep binary.
##
## The logic below supports -c -e -h -i -m -n -q -v options. The main focus is
## demonstrating Many-core Engine for Perl. Use this script for large file(s).
##
## This script was created to show how order can be preserved even though there
## are only 4 shared socket pairs in MCE no matter the number of workers.
##
## The usage description was largely ripped off from the egrep man page.
##
###############################################################################

use strict;
use warnings;

use Cwd qw(abs_path);
use lib abs_path . "/../lib";

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
   $prog_name -- print lines matching a pattern

SYNOPSIS
   $prog_name [options] PATTERN [FILE ...]
   $prog_name [options] [-e PATTERN] [FILE ...]

DESCRIPTION
   The $prog_name script searches the named input FILEs (or standard input
   if no files are named, or the file name - is given) for lines containing
   a match to the given PATTERN. By default, $prog_name prints the
   matching lines.

   The following options are available:

   --chunk_size CHUNK_SIZE
          Specify chunk size for MCE          -- default: 300000

   --max_workers MAX_WORKERS
          Specify number of workers for MCE   -- default: 8

   -c     Suppress normal output; instead print a count of matching lines
          for each input file. With the -v option (see below), count
          non-matching lines.

   -e PATTERN
          Use PATTERN as the pattern; useful to protect patterns beginning
          with -.

   -h     Suppress the prefixing of filenames on output when multiple files
          are searched.

   -i     Ignore case distinctions.

   -m     Stop reading a file after NUM matching lines.

   -n     Prefix each line of output with the line number within its input
          file.

   -q     Quiet; do not write anything to standard output. Exit immediately
          with zero status if any match is found, even if an error was
          detected.

   -v     Invert the sense of matching, to select non-matching lines.

EXIT STATUS
   The $prog_name utility exits 0 on success, and >0 if an error occurs or
   no match was found.

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

my $chunk_size  = 300000;
my $max_workers = 8;
my $skip_args   = 0;

my ($c_flag, $h_flag, $i_flag, $n_flag, $q_flag, $v_flag);
my ($multiple_files, $m_cnt);

my @files = (); my @patterns = (); my $re;

while ( my $arg = shift @ARGV ) {
   unless ($skip_args) {
      if ($arg eq '-') {
         push @files, $arg;
         next;
      }
      if ($arg =~ m/^-[chinqv]+$/) {
         while ($arg) {
            my $a = chop($arg);
            $c_flag = $flag->() and next if ($a eq 'c');
            $h_flag = $flag->() and next if ($a eq 'h');
            $i_flag = $flag->() and next if ($a eq 'i');
            $n_flag = $flag->() and next if ($a eq 'n');
            $q_flag = $flag->() and next if ($a eq 'q');
            $v_flag = $flag->() and next if ($a eq 'v');
         }
         next;
      }

      if ($arg eq '-e') {
         my $pattern = shift;
         push @patterns, $pattern if (defined $pattern);
         next;
      }

      $m_cnt       = $isOk->() and next if ($arg eq '-m');
      $chunk_size  = $isOk->() and next if ($arg eq '--chunk_size');
      $max_workers = $isOk->() and next if ($arg eq '--max_workers');
      $skip_args   = $flag->() and next if ($arg eq '--');

      usage() if ($arg =~ /^-/);
   }

   push @files, $arg;
}

push @patterns, shift @files if (@patterns == 0 && @files > 0);
usage() if (@patterns == 0);

$multiple_files = 1 if (!$h_flag && @files > 1);

if (@patterns > 1) {
   $re = '(?:' . join('|', @patterns) . ')';
}
else {
   $re = $patterns[0];
}

$re = ($i_flag) ? qr/$re/i : qr/$re/;

###############################################################################
## ----------------------------------------------------------------------------
## Launch Many-core Engine.
##
###############################################################################

## Defined user functions to run in parallel.

sub user_begin {

   my ($self) = @_;

   if ($c_flag) {
      use vars qw($match_re $eol_re $count);
      our $match_re = $re . '.*' . $/;
      our $eol_re = $/;
      our $count = 0;
   }

   return;
}

sub user_func {

   my ($self, $chunk_ref, $chunk_id) = @_;
   my ($found_match, @matches, $line_count, @lines);

   ## Count and return immediately if -c was specified.

   if ($c_flag) {
      my $match_count = 0;
      $match_count++ while ( $$chunk_ref =~ /$match_re/g );

      if ($v_flag) {
         $line_count = 0;
         $line_count++ while ( $$chunk_ref =~ /$eol_re/g );
         $count += $line_count - $match_count;
      }
      else {
         $count += $match_count;
      }

      return;
   }

   ## Quickly determine if a match is found.

   if (!$v_flag) {
      for (0 .. @patterns - 1) {
         if ($$chunk_ref =~ /$patterns[$_]/) {
            $found_match = 1;
            last;
         }
      }
   }

   ## Obtain file handle to slurped data.
   ## Collect matched data if chunk (slurped) data contains a match.

   open my $_MEM_FH, '<', $chunk_ref;
   binmode $_MEM_FH;

   if (!$v_flag && !$found_match) {
      if ($n_flag) {
         1 while (<$_MEM_FH>);
      }
   }
   else {
      if ($v_flag) {
         while (<$_MEM_FH>) {
            if ($_ !~ /$re/) {
               push @lines, $. if ($n_flag);
               push @matches, $_;
            }
         }
      }
      else {
         while (<$_MEM_FH>) {
            if ($_ =~ /$re/) {
               push @lines, $. if ($n_flag);
               push @matches, $_;
            }
         }
      }
   }

   $line_count = $.;
   close $_MEM_FH;

   ## Send result to main thread.

   my %wk_result = (
      'found_match' => scalar @matches,
      'line_count' => $line_count,
      'matches' => \@matches,
      'lines' => \@lines
   );

   $self->do('display_result', \%wk_result, $chunk_id);

   return;
}

sub user_end {

   my ($self) = @_;

   if ($c_flag) {
      $self->do('aggregate_count', $count) if ($count);
   }

   return;
}

## Instantiate Many-core Engine and spawn workers.

my $mce = MCE->new(
   chunk_size  => $chunk_size,
   max_workers => $max_workers,
   user_begin  => \&user_begin,
   user_func   => \&user_func,
   user_end    => \&user_end,
   use_slurpio => 1
);

$mce->spawn();

###############################################################################
## ----------------------------------------------------------------------------
## Report line numbers containing null values.
##
###############################################################################

my ($file, %result, $abort_all, $abort_job, $found_match);

my $exit_status   = 0;
my $total_matched = 0;
my $total_lines   = 0;
my $order_id      = 1;

keys(%result) = 4000;

## Callback function for aggregating count.

sub aggregate_count {

   my ($wk_count) = @_;

   $total_matched += $wk_count;
   $found_match = 1 if ($total_matched);

   return;
}

## Callback function for displaying results. Output order is preserved.

sub display_result {

   my ($wk_result, $chunk_id) = @_;

   return if ($abort_job);
   $result{$chunk_id} = $wk_result;

   while (1) {
      last unless exists $result{$order_id};
      my $r = $result{$order_id};

      if ($r->{found_match}) {
         $found_match = 1;

         if ($q_flag) {
            $mce->abort(); $abort_all = $abort_job = 1;
            last;
         }
         for my $i (0 .. @{ $r->{matches} } - 1) {
            $total_matched++;

            unless ($c_flag) {
               printf "%s:", $file if ($multiple_files);
               printf "%d:", $r->{lines}[$i] + $total_lines if ($n_flag);
               print $r->{matches}[$i];
            }

            if ($m_cnt && $m_cnt == $total_matched) {
               $mce->abort(); $abort_job = 1;
               last;
            }
         }
      }

      $total_lines += $r->{line_count} if ($n_flag);

      delete $result{$order_id};
      $order_id++;
   }
}

## Display total matched. Reset counters.

sub display_total_matched {

   if (!$q_flag && $c_flag) {
      printf "%s:", $file if ($multiple_files);
      print "$total_matched\n";
   }

   $total_matched = $total_lines = 0;
   $abort_job = undef;
   $order_id = 1;
}

## Process files, otherwise read from standard input.

if (@files > 0) {
   foreach (@files) {
      last if ($abort_all);

      $file = $_;

      if ($file eq '-') {
         open(STDIN, ($^O eq 'MSWin32') ? 'CON' : '/dev/tty') or die $!;
         $mce->process(\*STDIN);
         display_total_matched();
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
         display_total_matched();
      }
   }
}
else {
   $file = "(STDIN)";
   $mce->process(\*STDIN);
   display_total_matched();
}

## Shutdown Many-core Engine and exit.

$mce->shutdown();

if (!$q_flag && $exit_status) {
   exit($exit_status);
}
else {
   exit(($found_match) ? 0 : 1);
}

