#!/usr/bin/env perl
###############################################################################
## ----------------------------------------------------------------------------
## Egrep script (Perl implementation) similar to the egrep binary.
## Look at bin/mce_grep for a wrapper script around the grep binary.
##
## This script supports egrep's options [ceHhiLlmnqRrsv]. The main focus is
## demonstrating Many-core Engine for Perl. Use this script against large
## file(s).
##
## This script was created to show how output order can be preserved even
## though there are only 4 shared socket pairs in MCE no matter the number
## of workers.
##
## Which to choose (examples/egrep.pl or bin/mce_grep).
##
##   Examples/egrep.pl is a pure Perl implementation, it runs rather fast.
##   Bin/mce_grep is a wrapper script for the equivalent binary.
##
##   The egrep.pl script rus fast for -l and -r due to less overhead
##   behind the scene. The wrapper script is good for expensive pattern
##   matching -- expecially for agrep and tre-agrep. The wrapper also
##   supports most of the options due to being passed to the binary.
##
##   The chunking of data from Perl to the binary and back is efficient.
##   Try with --chunk-size=8M or higher. 
##
## The usage description was largely ripped off from the egrep man page.
##
###############################################################################

use strict;
use warnings;

use Cwd qw(abs_path);
use lib abs_path . "/../lib";

my $prog_name = $0;
   $prog_name =~ s{^.*[\\/]}{}g;
my $prog_dir  = abs_path($0);
   $prog_dir  =~ s{[\\/][^\\/]*$}{};

sub INIT {
   ## Provide file globbing support under Windows similar to Unix.
   @ARGV = <@ARGV> if ($^O eq 'MSWin32');
}

use Scalar::Util qw( looks_like_number );
use MCE;

if ($^O eq 'MSWin32') {
   $ENV{PATH} .= ';' . $prog_dir;
}

###############################################################################
## ----------------------------------------------------------------------------
## Display usage and exit.
##
###############################################################################

sub usage {

   my $exit_status = $_[0] || 0;

   print <<"::_USAGE_BLOCK_END_::";

Options for Many-core Engine:
  --max-workers=NUM         override max workers (default  8)
                              e.g. auto, auto-2 for automatic
  --chunk-size=NUM[KM]      override chunk size  (default 1M)
                              minimum: 200K  maximum: 20M

Usage: $prog_name [OPTION]... PATTERN [FILE] ...
Search for PATTERN in each FILE or standard input.
Example: $prog_name -i 'hello world' menu.h main.c

Regexp selection and interpretation:
  -e, --regexp=PATTERN      use PATTERN as a regular expression
  -i, --ignore-case         ignore case distinctions

Miscellaneous:
  -s, --no-messages         suppress error messages
  -v, --invert-match        select non-matching lines
      --help                display this help and exit

Output control:
  -m, --max-count=NUM       stop after NUM matches
  -n, --line-number         print line number with output lines
  -H, --with-filename       print the filename for each match
  -h, --no-filename         suppress the prefixing filename on output
  -q, --quiet, --silent     suppress all normal output
  -R, -r, --recursive       equivalent to --directories=recurse
      --include=PATTERN     files that match PATTERN will be examined
      --exclude=PATTERN     files that match PATTERN will be skipped
      --exclude-from=FILE   files that match PATTERN in FILE will be skipped
      --exclude-dir=PATTERN directories that match PATTERN will be skipped
                            requires a recent egrep binary for --exclude-dir
  -L, --files-without-match only print FILE names containing no match
  -l, --files-with-matches  only print FILE names containing matches
  -c, --count               only print a count of matching lines per FILE

With no FILE, or when FILE is -, read standard input. If less than
two FILEs given, assume -h. Exit status is 0 if match, 1 if no match,
and 2 if trouble.

::_USAGE_BLOCK_END_::

   exit $exit_status;
}

###############################################################################
## ----------------------------------------------------------------------------
## Define defaults and process command-line arguments.
##
###############################################################################

my ($c_flag, $H_flag, $h_flag, $i_flag, $n_flag, $q_flag, $r_flag, $v_flag);
my (@r_patn, $arg, @files, @patterns, $re, $skip_args, $w_filename);
my ($L_flag, $l_flag, $f_list);

my $max_workers = 8; my $chunk_size = '1M';
my $max_count = 0; my $no_msg = 0;
my $flag = sub { 1 };

while ( @ARGV ) {

   $arg = shift @ARGV;
   $arg =~ s/ /\\ /g;

   if ($skip_args) {
      push @files, $arg;
      next;
   }

   if (substr($arg, 0, 2) eq '--') {              ## --OPTION
      $skip_args = $flag->() and next if ($arg eq '--');

      $no_msg = $flag->() and next if ($arg eq '--no-messages');
      $c_flag = $flag->() and next if ($arg eq '--count');
      $i_flag = $flag->() and next if ($arg eq '--ignore-case');
      $L_flag = $flag->() and next if ($arg eq '--files-without-match');
      $l_flag = $flag->() and next if ($arg eq '--files-with-match');
      $n_flag = $flag->() and next if ($arg eq '--line-number');
      $q_flag = $flag->() and next if ($arg eq '--quiet');
      $q_flag = $flag->() and next if ($arg eq '--silent');
      $r_flag = $flag->() and next if ($arg eq '--recursive');
      $v_flag = $flag->() and next if ($arg eq '--invert-match');

      if ($arg eq '--help') {
         usage(0);
      }
      if ($arg eq '^--regexp=(.+)') {
         push @patterns, $1;
         next;
      }
      if ($arg =~ m/^--include=.+/) {
         push @r_patn, $arg;
         next;
      }
      if ($arg =~ m/^--exclude=.+/) {
         push @r_patn, $arg;
         next;
      }
      if ($arg =~ m/^--exclude-from=.+/) {
         push @r_patn, $arg;
         next;
      }
      if ($arg =~ m/^--exclude-dir=.+/) {
         push @r_patn, $arg;
         next;
      }
      if ($arg eq '--with-filename') {
         $H_flag = 1; $h_flag = 0;
         next;
      }
      if ($arg eq '--no-filename') {
         $H_flag = 0; $h_flag = 1;
         next;
      }

      if ($arg =~ /^--max-count=(.+)/) {
         $max_count = $1;

         unless (looks_like_number($max_count) && $max_count >= 0) {
            print STDERR "$prog_name: invalid max count\n";
            exit 2;
         }

         next;
      }

      if ($arg =~ /^--max[-_]workers=(.+)/) {
         $max_workers = $1;

         if ($max_workers !~ /^auto/) {
            unless (looks_like_number($max_workers) && $max_workers > 0) {
               print STDERR "$prog_name: invalid max workers\n";
               exit 2;
            }
         }

         next;
      }
      if ($arg =~ /^--chunk[-_]size=(.+)/) {
         $chunk_size = $1;

         if ($chunk_size =~ /^(\d+)K/i) {
            $chunk_size = $1 * 1024;
         }
         elsif ($chunk_size =~ /^(\d+)M/i) {
            $chunk_size = $1 * 1024 * 1024;
         }

         if (looks_like_number($chunk_size) && $chunk_size > 0) {
            $chunk_size = 20_971_520 if $chunk_size > 20_971_520;  ## 20M
            $chunk_size =    204_800 if $chunk_size <    204_800;  ## 200K
         }
         else {
            print STDERR "$prog_name: invalid chunk size\n";
            exit 2;
         }

         next;
      }

      usage(2);
   }

   elsif (substr($arg, 0, 1) eq '-') {            ## -OPTION

      if ($arg eq '-') {
         push @files, $arg;
         next;
      }

      if ($arg =~ m/^-([cHhiLlmnqRrsv]+)$/) {
         my $t_arg = reverse $1;

         while ($t_arg) {
            my $a = chop($t_arg);

            $no_msg = $flag->() and next if ($a eq 's');
            $c_flag = $flag->() and next if ($a eq 'c');
            $i_flag = $flag->() and next if ($a eq 'i');
            $n_flag = $flag->() and next if ($a eq 'n');
            $q_flag = $flag->() and next if ($a eq 'q');
            $r_flag = $flag->() and next if ($a eq 'R');
            $r_flag = $flag->() and next if ($a eq 'r');
            $v_flag = $flag->() and next if ($a eq 'v');

            if ($a eq 'H') {
               $H_flag = 1; $h_flag = 0;
            }
            elsif ($a eq 'h') {
               $H_flag = 0; $h_flag = 1;
            }
            elsif ($a eq 'L') {
               $L_flag = 1; $l_flag = 0;
            }
            elsif ($a eq 'l') {
               $L_flag = 0; $l_flag = 1;
            }
            elsif ($a eq 'm') {
               if (substr($arg, -1) eq 'm') {
                  $max_count = shift @ARGV;
                  if (defined $max_count) {
                     unless (looks_like_number($max_count) && $max_count >= 0) {
                        print STDERR "$prog_name: invalid max count\n";
                        exit 2;
                     }
                  }
               }
               elsif ($arg =~ /m(\d+)$/) {
                  $max_count = $1;
               }
            }
         }

         next;
      }

      if ($arg eq '-e') {
         my $pattern = shift;
         push @patterns, $pattern if (defined $pattern);
         next;
      }

      usage(2);
   }

   push @files, $arg;                             ## FILE
}

$f_list = ($L_flag || $l_flag);

push @patterns, shift @files if (@patterns == 0 && @files > 0);

usage(2) if (@patterns == 0);

if (@patterns > 1) {
   $re = '(?:' . join('|', @patterns) . ')';
}
else {
   $re = $patterns[0];
}

###############################################################################
## ----------------------------------------------------------------------------
## Callback functions for Many-core Engine.
##
###############################################################################

my ($file, %result, $abort_all, $abort_job, $found_match);

my $exit_status = 0;
my $total_found = 0;
my $total_lines = 0;
my $order_id    = 1;

keys(%result) = 4000;

sub aggregate_count {

   my ($wk_count) = @_;

   $total_found += $wk_count;
   $found_match  = 1 if ($total_found);

   return;
}

sub display_result {

   my ($result, $chunk_id) = @_;

   return if ($abort_job);
   $result{$chunk_id} = $result;

   while (1) {
      last unless exists $result{$order_id};
      my $r = $result{$order_id};

      if (!$abort_job && $r->{found_match}) {
         $found_match = 1;

         if ($q_flag) {
            MCE->abort(); $abort_all = $abort_job = 1;
            last;
         }
         for my $i (0 .. @{ $r->{matches} } - 1) {
            $total_found++;

            unless ($c_flag) {
               printf "%s:", $file if ($w_filename);
               printf "%d:", $r->{lines}[$i] + $total_lines if ($n_flag);
               print $r->{matches}[$i];
            }

            if ($max_count && $max_count == $total_found) {
               MCE->abort(); $abort_job = 1;
               last;
            }
         }
      }

      $total_lines += $r->{line_count} if ($n_flag);

      delete $result{$order_id};
      $order_id++;
   }
}

sub report_match {

   if (!$abort_job) {
      MCE->abort();
      $abort_all = 1 if $q_flag;
      $abort_job = $total_found = 1;
   }

   return;
}

###############################################################################
## ----------------------------------------------------------------------------
## Launch Many-core Engine.
##
###############################################################################

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

sub user_end {

   my ($self) = @_;

   if ($c_flag) {
      MCE->do('aggregate_count', $count) if ($count);
   }

   return;
}

sub user_func {

   my ($self, $chunk_ref, $chunk_id) = @_;
   my ($found_match, @matches, $line_count, @lines);

   ## Count and return immediately if -c was specified.

   if ($c_flag && !$f_list) {
      my $match_count = 0;

      if ($i_flag) {
         $match_count++ while ( $$chunk_ref =~ /$match_re/img );
      } else {
         $match_count++ while ( $$chunk_ref =~ /$match_re/mg );
      }

      if ($v_flag) {
         unless ($eol_re eq "\n") {
            $line_count = 0; $line_count++ while ( $$chunk_ref =~ /$eol_re/g );
         } else {
            $line_count = ( $$chunk_ref =~ tr/\n// );
         }
         $count += $line_count - $match_count;
      }
      else {
         $count += $match_count;
      }

      return;
   }

   ## Quickly determine if a match is found.

   if (!$v_flag || $f_list) {
      for (0 .. @patterns - 1) {
         if ($i_flag) {
            if ($$chunk_ref =~ /$patterns[$_]/im) {
               $found_match = 1;
               last;
            }
         }
         else {
            if ($$chunk_ref =~ /$patterns[$_]/m) {
               $found_match = 1;
               last;
            }
         }
      }
   }

   if ($f_list) {
      MCE->do('report_match')
         if (($l_flag && $found_match) || ($L_flag && !$found_match));

      return;
   }

   ## Obtain file handle to slurped data.
   ## Collect matched data if slurped chunk data contains a match.

   open my $_MEM_FH, '<', $chunk_ref;
   binmode $_MEM_FH, ':raw';

   if (!$v_flag && !$found_match) {
      if ($n_flag) {
         1 while (<$_MEM_FH>);
      }
   }
   else {
      if ($v_flag) {
         if ($i_flag) {
            while (<$_MEM_FH>) {
               if ($_ !~ /$re/i) {
                  push @matches, $_; push @lines, $. if ($n_flag);
               }
            }
         }
         else {
            while (<$_MEM_FH>) {
               if ($_ !~ /$re/) {
                  push @matches, $_; push @lines, $. if ($n_flag);
               }
            }
         }
      }
      else {
         if ($i_flag) {
            while (<$_MEM_FH>) {
               if ($_ =~ /$re/i) {
                  push @matches, $_; push @lines, $. if ($n_flag);
               }
            }
         }
         else {
            while (<$_MEM_FH>) {
               if ($_ =~ /$re/) {
                  push @matches, $_; push @lines, $. if ($n_flag);
               }
            }
         }
      }
   }

   $line_count = $.;
   close $_MEM_FH;

   ## Send results to the manager process.

   my %result = (
      'found_match' => scalar @matches,
      'line_count' => $line_count,
      'matches' => \@matches,
      'lines' => \@lines
   );

   MCE->do('display_result', \%result, $chunk_id);

   return;
}

my $mce = MCE->new(
   max_workers => $max_workers, chunk_size => $chunk_size, use_slurpio => 1,
   user_begin => \&user_begin, user_func => \&user_func,
   user_end => \&user_end

)->spawn;

###############################################################################
## ----------------------------------------------------------------------------
## Process files, otherwise read from standard input.
##
###############################################################################

sub display_matched {

   if (!$q_flag && $f_list) {
      print "$file\n" if $total_found;
   }
   elsif (!$q_flag && $c_flag) {
      printf "%s:", $file if $w_filename;
      print "$total_found\n";
   }

   $total_found = $total_lines = 0;
   $abort_job = undef;
   $order_id = 1;
}

if ($r_flag && @files > 0) {
   my @t_files = `egrep -lsr @r_patn ^ @files`;

   if (@t_files) {
      @files = @t_files; undef @t_files;
      chomp @files;
   }
}

$w_filename = 1
   if ((!$h_flag && @files > 1) || $H_flag);

if (@files > 0) {
   foreach (@files) {
      last if ($abort_all);

      $file = $_;

      if ($file eq '-') {
         $file = "(standard input)";
         open(STDIN, ($^O eq 'MSWin32') ? 'CON' : '/dev/tty') or die $!;
         $mce->process(\*STDIN);
         display_matched();
      }
      elsif (! -e $file) {
         $exit_status = 2;

         print STDERR "$prog_name: $file: No such file or directory\n"
            unless $no_msg;
      }
      elsif (-d $file) {
         $exit_status = 1;

       # print STDERR "$prog_name: $file: Is a directory\n"
       #    unless $no_msg;
      }
      else {
         $mce->process($file);
         display_matched();
      }
   }
}
else {
   $file = "(standard input)";
   $mce->process(\*STDIN);
   display_matched();
}

## Shutdown Many-core Engine and exit.

$mce->shutdown();

if (!$q_flag && $exit_status) {
   exit($exit_status);
}
else {
   exit($found_match ? 0 : ($exit_status ? $exit_status : 1));
}

