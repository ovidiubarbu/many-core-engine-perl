
use strict;
use warnings;

package FastaReaderTxt;

## Using "\n>" for the input record separator, thus record driven.
## Generates text output containing the raw data, excluding '>'.
## Also see BioUtil::Seq::FastaReader.

sub Reader {
   my ($file, $not_trim) = @_;

   my ($open_flg, $finished) = (0, 0);
   my ($fh, $pos, $hdr, $seq);

   if (ref $file eq '' || ref $file eq 'SCALAR') {
      open($fh, '<', $file) or die "$file: open: !\n";
      $open_flg = 1;
   } else {
      $fh = $file;
   }

   local $/ = \1;                                   ## read one byte
   while (<$fh>) {                                  ## until reaching ">"
      last if $_ eq '>';
   }

   return sub {
      return if $finished;

      local $/ = "\n>";                             ## input record separator
      while (<$fh>) {
         chop if substr($_, -1, 1) eq '>';          ## trim trailing ">"

         $pos = index($_, "\n") + 1;                ## header and sequence
         $hdr = substr($_, 0, $pos - 1);
         $seq = substr($_, $pos);

         chop $hdr if substr($hdr, -1, 1) eq "\r";  ## trim trailing "\r"
         $seq =~ tr/\t\r\n //d unless $not_trim;    ## trim white space

         return [ $hdr, $seq ];
      }

      close $fh if $open_flg;
      $finished = 1;

      return;
   };
}

1;

