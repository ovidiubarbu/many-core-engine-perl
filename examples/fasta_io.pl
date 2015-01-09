#!/usr/bin/env perl

use strict;
use warnings;

## FASTA reader for FASTA files.
##
## The original plan was to run CPAN BioUtil::Seq::FastaReader in parallel.
## I wanted to process by records versus lines ($/ = "\n>") for faster
## performance. Created for the investigative Bioinformatics field.
##
## Synopsis
##   fasta_io.pl [ /path/to/fastafile.fa ]
##
##   NPROCS=2 fasta_faidx.pl ...  run with 2 MCE workers

use Cwd 'abs_path';
use lib abs_path($0 =~ m{^(.*)[\\/]} && $1 || abs_path) . '/include';
use lib abs_path($0 =~ m{^(.*)[\\/]} && $1 || abs_path) . '/../lib';

use constant { HDR => 0, SEQ => 1, SID => 2, DESC => 3 };

use MCE::Flow;
use Time::HiRes 'time';
use FastaReaderIO;

my $file  = shift || \*DATA;
my $start = time;

## Iterator for preserving output order.
 
sub output_iterator {
   my %tmp; my $order_id = 1;
 
   return sub {
      $tmp{ (shift) } = \@_;
 
      while (1) {
         last unless exists $tmp{$order_id};
         print @{ delete $tmp{$order_id++} };
      }
 
      return;
   };
}

## Process file.

my $nlimit = MCE::Util::get_ncpu; $nlimit = 4 if $nlimit > 4;
my $nprocs = $ENV{NPROCS} || $nlimit;

mce_flow_f {
   chunk_size => "2m", max_workers => $nprocs,
   RS => "\n>", RS_prepend => ">", use_slurpio => 1,
   gather => output_iterator,
},
sub {
   my ($mce, $slurp_ref, $chunk_id) = @_;
   my ($hdr, $seq, $sid, $desc, $output);

   ## read from scalar reference
   my $next_seq = FastaReaderIO::Reader($slurp_ref);

   ## loop through sequences in $slurp_ref
   while (my $fa = &$next_seq()) {
    # my ($hdr, $seq, $sid, $desc) = @{ $fa };       ## <- extra copy in memory
    # $output .= ">$fa->[HDR]\n". $fa->[SEQ] ."\n";  ## <- do this instead
      $output .=  "$fa->[SID]\t$fa->[DESC]\n";       ##    hash-like retrieval
   }

   ## Send output to STDOUT for this chunk
   ## MCE->gather($chunk_id, $output);               ## <- not for big seq data
   MCE->print($output);                              ##    out of order ok

}, $file;

printf {*STDERR} "\n## Compute time: %0.03f\n\n", time - $start;

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

