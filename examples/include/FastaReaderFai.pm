
use strict;
use warnings;

package FastaReaderFai;

## Using "\n>" for the input record separator, thus record driven.
## Generates output suitable for (.fai) index files.
## Also see BioUtil::Seq::FastaReader.

sub Reader {
   my ($file, $offset_adj) = @_;

   my ($open_flg, $finished, $first_flg) = (0, 0, 1);
   my ($fh, $pos, $hdr, $seq);

   if (ref $file eq '' || ref $file eq 'SCALAR') {
      open($fh, '<', $file) or die "$file: open: !\n";
      $open_flg = 1;
   } else {
      $fh = $file;
   }

   my ($c1, $c2, $c3, $c4, $c5, $p1, $p2, $acc);

   ## $c1 = the name of the sequence
   ## $c2 = the length of the sequence
   ## $c3 = the offset of the first base in the file
   ## $c4 = the number of bases in each fasta line
   ## $c5 = the number of bytes in each fasta line

   $c3 = $offset_adj; $acc = 0;

   return sub {
      return if $finished;
      local $/ = "\n>";                     ## set input record separator

      while (<$fh>) {
         if ($first_flg) {                  ## 1st record must have leading ">"
            $first_flg--;                   ## trim ">", otherwise skip record
            s/^>// || next;
         }
         chop if substr($_, -1, 1) eq '>';  ## trim trailing ">", part of $/

         $pos = index($_, "\n") + 1;        ## extract header and bases
         $hdr = substr($_, 0, $pos);
         $seq = substr($_, $pos);

        ($c1) = ($hdr) =~ /^(\S+)/;         ## compute initial values
         $c2  = length($seq);
         $c3  = $acc + 1 + length($hdr);
         $c5  = index($seq, "\n");
         $acc = $c3 + $c2;

         if ($c5 < 0) {
            return [ $c1, 0, -1, 0, 0, $acc ];     ## sequence has no bases
         }
         else {
            my @a;  $p1 = $c5 + 1;                 ## start on 2nd bases line

            while ($p1 < $c2) {                    ## collect line lengths
               $p2 = index($seq, "\n", $p1);
               push @a, $p2 - $p1;
               $p1 = $p2 + 1;
            }
            if (scalar @a) {
               pop @a while ($a[-1] == 0);         ## pop trailing blank lines
               pop @a;                             ## pop last line w/ bases

               foreach (@a) {                      ## any length mismatch?
                  return [ $c1, 0, -2, 0, 0, $acc ] if $_ != $c5;
               }
            }
            $c4  =  (substr($seq, ++$c5 - 2, 1) eq "\r") ? $c5 - 2 : $c5 - 1;
            $seq =~ s/\s//g;
            $c2  =  length($seq);

            undef $seq;
         }

         return [ $c1, $c2, $c3, $c4, $c5, $acc ];
      }

      close $fh if $open_flg;
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

