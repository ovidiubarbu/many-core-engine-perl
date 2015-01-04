
use strict;
use warnings;

package BioUtil::Test;

no strict 'refs';
no warnings 'redefine';

## Using "\n>" for the input record separator, thus record driven.
## Generates output suitable for (.fai) index files.

sub FastaReader_fai {
   my ($file, $offset_adj) = @_;

   my ($close_flg, $finished, $rec) = (0, 0, 0);
   my ($fh, $pos, $hdr, $seq);

   if (ref $file eq '' || ref $file eq 'SCALAR') {
      open $fh, '<', $file or die "$file: open: !\n";
      $close_flg = 1;
   } else {
      $fh = $file;
   }

   my ($c1, $c2, $c3, $c4, $c5, $p1, $p2, $acc);

   $c3 = $offset_adj; $acc = 0;

   ## $c1 = the name of the sequence
   ## $c2 = the length of the sequence
   ## $c3 = the offset of the first base in the file
   ## $c4 = the number of bases in each fasta line
   ## $c5 = the number of bytes in each fasta line

   return sub {
      return if $finished;
      local $/ = "\n>";                     ## set input record separator

      while (<$fh>) {
         unless ($rec++) {                  ## 1st record must have leading ">"
            s/^>// || next;                 ## trim ">", otherwise skip record
         }
         chop if substr($_, -1, 1) eq '>';  ## trim trailing ">"

         $pos = index($_, "\n");            ## extract header and bases
         $hdr = substr($_, 0, $pos + 1);
         $seq = substr($_, $pos + 1);

        ($c1) = ($hdr) =~ /^(\S+)/;         ## compute initial values
         $c2  = length $seq;
         $c3  = $acc + 1 + length $hdr;
         $c5  = index $seq, "\n";
         $acc = $c3 + $c2;

         if ($c5 < 0) {
            return [ $c1, 0, -1, 0, 0, $acc ];     ## sequence has no bases
         }
         else {
            my @a;  $p1 = $c5 + 1;

            while ($p1 < $c2) {                    ## collect line lengths
               $p2 = index $seq, "\n", $p1;
               push @a, $p2 - $p1;
               $p1 = $p2 + 1;
            }
            if (scalar @a) {
               pop @a while ($a[-1] == 0);         ## pop blank lines
               pop @a;

               foreach (0 .. scalar(@a) - 1) {     ## any length mismatch?
                  if ($a[$_] != $c5) {
                     return [ $c1, 0, -2, 0, 0, $acc ];
                  }
               }
            }
            $c4  =  (substr($seq, ++$c5 - 2, 1) eq "\r") ? $c5 - 2 : $c5 - 1;
            $seq =~ tr/ \t\r\n//d;
            $c2  =  length $seq;
         }

         return [ $c1, $c2, $c3, $c4, $c5, $acc ];
      }

      close $fh if $close_flg;
      $finished = 1;

      return;
   };
}

## Get 1st offset position, typically 0, but just in case.

sub GetFirstOffset {
   my ($offset, $file) = (0, @_);

   if (ref $file eq '' || ref $file eq 'SCALAR') {
      open my $fh, '<', $file or die "$file: open: $!\n";
      while (<$fh>) { last if (/^>/); $offset += length; }
      close $fh;
   }
   else {
      while (<$file>) { last if (/^>/); $offset += length; }
      seek $file, 0, 0;
   }

   return $offset;
}

1;

