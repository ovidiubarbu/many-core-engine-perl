#!/usr/bin/env perl

use strict;
use warnings;

##
## FASTA index (.fai) generation for FASTA files.
##   https://gist.github.com/marioroy/85d08fc82845f11d12b5
##
## The original plan was to run CPAN BioUtil::Seq::FastaReader in parallel.
## I decided to process by records versus lines ($/ = "\n>") for faster
## performance. Created for the investigative Bioinformatics field.
##
## Synopsis
##   fasta_faidx.pl [ /path/to/fastafile.fa ]
##
##   FAIDXC=1 fasta_faidx.pl ...  use Incline C if available
##   NPROCS=2 fasta_faidx.pl ...  run with 2 MCE workers
##

use Cwd 'abs_path';
use lib abs_path($0 =~ m{^(.*)[\\/]} && $1 || abs_path) . '/include';
use lib abs_path($0 =~ m{^(.*)[\\/]} && $1 || abs_path) . '/../lib';

use MCE::Flow;
use Time::HiRes 'time';
use FastaReaderFaidx;

## Define FAIDXC=1 to use FastaReaderFaidxC for faster performance.
## no critic (BuiltinFunctions::ProhibitStringyEval)

my $fasta_reader = \&FastaReaderFaidx::Reader;

if (exists $ENV{FAIDXC} && $ENV{FAIDXC} eq '1') {
   local $@; eval 'require FastaReaderFaidxC';
   if ($@) {
      warn "Uh-oh: Inline C is missing or fasta_faidx.c failed to compile.\n";
   } else {
      $fasta_reader = \&FastaReaderFaidxC::Reader
   }
}

## Display error message.

my $exit_status = 0;

sub print_error {
   my ($error_msg) = @_;
   print {*STDERR} $error_msg."\n";
   $exit_status = 1;
}

## Open handle to index file *.fai.

my $output_fh;
my $file  = shift || \*DATA;
my $start = time;

if (ref $file) {
   $output_fh = \*STDOUT;
}
else {
   die "$file: $!\n" unless -f $file;
   open($output_fh, '>', "$file.fai") or die "$file.fai: open: $!\n";
}

## Iterator for preserving output order.

sub output_iterator {
   my ($output_fh, $offset) = @_;
   my (%tmp, $size); my $order_id = 1;

   return sub {
      $tmp{ (shift) } = \@_;

      while (1) {
         last unless exists $tmp{$order_id};
         $size = pop @{ $tmp{$order_id} };
         my $buffer = '';

         foreach my $row ( @{ delete $tmp{$order_id++} } ) {
            if ($row->[1] < 0) {
               $buffer .= $row->[0].$row->[1].$row->[2];
            } else {
               $buffer .= $row->[0].($row->[1] + $offset).$row->[2];
            }
         }
         print {$output_fh} $buffer;
         $offset += $size;
      }

      return;
   };
}

## Process file.

print {*STDERR} "Building $file.fai\n" unless ref $file;

my $offset_adj = FastaReaderFaidx::GetFirstOffset($file);
my $nlimit = MCE::Util::get_ncpu; $nlimit = 4 if $nlimit > 4;
my $nprocs = $ENV{NPROCS} || $nlimit;

mce_flow_f {
   chunk_size => "2m", max_workers => $nprocs,
   RS => "\n>", RS_prepend => ">", use_slurpio => 1,
   gather => output_iterator($output_fh, $offset_adj),
},
sub {
   my ($mce, $slurp_ref, $chunk_id) = @_;
   my ($name, $len, $off, $bases, $bytes, $acc, @output);

   ## read from scalar reference
   my $next_seq = $fasta_reader->($slurp_ref);

   ## loop through sequences in $slurp_ref
   while (my $fa = &$next_seq()) {
      ($name, $len, $off, $bases, $bytes, $acc) = @{ $fa };

      if ($off == -2) {
         my $err = 'SKIPPED: mismatched line lengths within sequence '.$name;
         MCE->do('print_error', $err);
      }
      else {
         ## concatenate left,right sides to save time for the manager process
         push @output, [ "$name\t$len\t", $off, "\t$bases\t$bytes\n" ];
      }
   }

   ## gather output for this chunk
   MCE->gather($chunk_id, @output, $acc);

}, $file;

## Finish.

close $output_fh unless ref $file;

printf {*STDERR} "\n## Compute time: %0.03f\n\n", time - $start;

exit $exit_status;

__END__
>seq1 description1
TAACCCTAACCCTAACCCTAACCCTAACCCTAACCCTAACCCTAACCCTAACCCTAACCC
TAACCCTAACCCTAACCCTAACCCTAACCCTAACCCTAACCCTAACCCAACCCTAACCCT
AACCCTAACCCTAACCCTAACCCTAACCCCTAACCCTAACCCTAACCCTAACCCTAACCT
AACCCTAACCCTAACCCTAACCCTAACCCTAACCCTAACCCTAACCCTAACCCCTAACCC
TAACCCTAAACCCTAAACCCTAACCCTAACCCTAACCCTAACCCTAACCCCAACCCCAAC
CCCAACCCCAACCCCAACCCCAACCCTAACCCCTAACCCTAACCCTAACCCTACCCTAAC
CCTAACCCTAACCCTAACCCTAACCCTAACCCCTAACCCCTAACCCTAACCCTAACCCTA
ACCCTAACCCTAACCCTAACCCCTAACCCTAACCCTAACCCTAACCCTCGCGGTACCCTC
AGCCGGCCCGCCCGCCCGGGTCTGACCTGAGGAGAACTGTGCTCCGCCTTCAGAGTACCA
CCGAAATCTGTGCAGAGGACAACGCAGCTCCGCCCTCGCGGTGCTCTCCGGGTCTGTGCT
GAGGAGAACGCAACTCCGCCGGCGCAGGCG

>seq2 description2
TAACCCTAACCCTAACCCTAACCCTAACCCTAACCCTAACCCTAACCCTAACCCTAACCC
TAACCCTAACCCTAACCCTAACCCTAACCCTAACCCTAACCCTAACCCAACCCTAACCCT
AACCCTAACCCTAACCCTAACCCTAACCCCTAACCCTAACCCTAACCCTAACCCTAACCT
AACCCTAACCCTAACCCTAACCCTAACCCTAACCCTAACCCTAACCCTAACCCCTAACCC
TAACCCTAAACCCTAAACCCTAACCCTAACCCTAACCCTAACCCTAACCCCAACCCCAAC
CCCAACCCCAACCCCAACCCCAACCCTAACCCCTAACCCTAACCCTAACCCTACCCTAAC
CCTAACCCTAACCCTAACCCTAACCCTAACCCCTAACCCCTAACCCTAACCCTAACCCTA
ACCCTAACCCTAACCCTAACCCCTAACCCTAACCCTAACCCTAACCCTCGCGGTACCCTC
AGCCGGCCCGCCCGCCCGGGTCTGACCTGAGGAGAACTGTGCTCCGCCTTCAGAGTACCA
CCGAAATCTGTGCAGAGGACAACGCAGCTCCGCCCTCGCGGTGCTCTCCGGGTCTGTGCT
GAGGAGAACGCAAC

>seq3 description3
TAACCCTAACCCTAACCCTAACCCTAACCCTAACCCTAACCCTAACCCTAACCCTAACCC
TAACCCTAACCCTAACCCTAACCCTAACCCTAACCCTAACCCTAACCCAACCCTAACCCT
AACCCTAACCCTAACCCTAACCCTAACCCCTAACCCTAACCCTAACCCTAACCCTAACCT
AACCCTAACCCTAACCCTAACCCTAACCCTAACCCTAACCCTAACCCTAACCCCTAACCC
TAACCCTAAACCCTAAACCCTAACCCTAACCCTAACCCTACTACCCTAACCCTAACCCTA
ACCCTAACCCTAACCCTAACCCCTAACCCCTAACCCTAACCCTAACCCTAACCCTAACCC
TAACCCTAACCCCTAACCCTAACCCTAACCCTAACCCTCGCGGTACCCTCAGCCGGCCCG
CCCGCCCGGGTCTGACCTGAGGAGAACTGTGCTCCGCCTTCAGAGTACCACCGAAATCTG
TGCAGAGGACAACGCAGCTCCGCCCTCGCGGTGCTCTCCGGGTCTGTGCTGAGGAGAACG
CAACTCCGCCGGCGCAGGCGACCCTAACCCCAACCCCAACCCCAACCCCAACCCCAACCC
CAACCCTAACCCCTAACCCTAACCCT

